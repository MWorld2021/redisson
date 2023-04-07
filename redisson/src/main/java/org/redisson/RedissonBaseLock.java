/**
 * Copyright (c) 2013-2022 Nikita Koksharov
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchResult;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.RedisException;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.client.protocol.decoder.MapValueDecoder;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.misc.CompletableFutureWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.function.Supplier;

/**
 * Base class for implementing distributed locks
 *
 * @author Danila Varatyntsev
 * @author Nikita Koksharov
 */
public abstract class RedissonBaseLock extends RedissonExpirable implements RLock {

    public static class ExpirationEntry {

        private final Map<Long, Integer> threadIds = new LinkedHashMap<>();
        private volatile Timeout timeout;

        public ExpirationEntry() {
            super();
        }

        public synchronized void addThreadId(long threadId) {
            threadIds.compute(threadId, (t, counter) -> {
                counter = Optional.ofNullable(counter).orElse(0);
                counter++;
                return counter;
            });
        }

        public synchronized boolean hasNoThreads() {
            return threadIds.isEmpty();
        }

        public synchronized Long getFirstThreadId() {
            if (threadIds.isEmpty()) {
                return null;
            }
            return threadIds.keySet().iterator().next();
        }

        public synchronized void removeThreadId(long threadId) {
            threadIds.compute(threadId, (t, counter) -> {
                if (counter == null) {
                    return null;
                }
                counter--;
                if (counter == 0) {
                    return null;
                }
                return counter;
            });
        }

        public void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }

        public Timeout getTimeout() {
            return timeout;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(RedissonBaseLock.class);

    private static final ConcurrentMap<String, ExpirationEntry> EXPIRATION_RENEWAL_MAP = new ConcurrentHashMap<>();
    protected long internalLockLeaseTime;

    final String id;
    final String entryName;

    final CommandAsyncExecutor commandExecutor;

    public RedissonBaseLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.id = commandExecutor.getServiceManager().getId();
        this.internalLockLeaseTime = commandExecutor.getServiceManager().getCfg().getLockWatchdogTimeout();
        this.entryName = id + ":" + name;
    }

    /**
     * Returns lock name
     *
     * @return lock name
     */
    protected String getEntryName() {
        return entryName;
    }

    /**
     * Returns lock name
     *
     * @return lockName
     */
    protected String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    private void renewExpiration() {
        ExpirationEntry ee = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (ee == null) {
            return;
        }

        Timeout task = commandExecutor.getServiceManager().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                ExpirationEntry ent = EXPIRATION_RENEWAL_MAP.get(getEntryName());
                if (ent == null) {
                    return;
                }
                Long threadId = ent.getFirstThreadId();
                if (threadId == null) {
                    return;
                }

                CompletionStage<Boolean> future = renewExpirationAsync(threadId);
                future.whenComplete((res, e) -> {
                    if (e != null) {
                        log.error("Can't update lock {} expiration", getRawName(), e);
                        EXPIRATION_RENEWAL_MAP.remove(getEntryName());
                        return;
                    }

                    if (res) {
                        // reschedule itself
                        renewExpiration();
                    } else {
                        cancelExpirationRenewal(null);
                    }
                });
            }
        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);

        ee.setTimeout(task);
    }

    protected void scheduleExpirationRenewal(long threadId) {
        ExpirationEntry entry = new ExpirationEntry();
        ExpirationEntry oldEntry = EXPIRATION_RENEWAL_MAP.putIfAbsent(getEntryName(), entry);
        if (oldEntry != null) {
            oldEntry.addThreadId(threadId);
        } else {
            entry.addThreadId(threadId);
            try {
                renewExpiration();
            } finally {
                if (Thread.currentThread().isInterrupted()) {
                    cancelExpirationRenewal(threadId);
                }
            }
        }
    }

    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return 0;",
                Collections.singletonList(getRawName()),
                internalLockLeaseTime, getLockName(threadId));
    }

    protected void cancelExpirationRenewal(Long threadId) {
        ExpirationEntry task = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (task == null) {
            return;
        }

        if (threadId != null) {
            task.removeThreadId(threadId);
        }

        if (threadId == null || task.hasNoThreads()) {
            Timeout timeout = task.getTimeout();
            if (timeout != null) {
                timeout.cancel();
            }
            EXPIRATION_RENEWAL_MAP.remove(getEntryName());
        }
    }

    protected <T> RFuture<T> evalWriteAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object... params) {
        CompletionStage<Map<String, String>> replicationFuture = CompletableFuture.completedFuture(Collections.emptyMap());
        if (!(commandExecutor instanceof CommandBatchService)
                && !commandExecutor.getServiceManager().getConfig().checkSkipSlavesInit()) {
            replicationFuture = commandExecutor.writeAsync(getRawName(), RedisCommands.INFO_REPLICATION);
        }
        CompletionStage<T> resFuture = replicationFuture.thenCompose(r -> {
            int availableSlaves = Integer.parseInt(r.getOrDefault("connected_slaves", "0"));

            CommandBatchService executorService = createCommandBatchService(availableSlaves);
            RFuture<T> result = executorService.evalWriteAsync(key, codec, evalCommandType, script, keys, params);
            if (commandExecutor instanceof CommandBatchService) {
                return result;
            }

            RFuture<BatchResult<?>> future = executorService.executeAsync();
            CompletionStage<T> f = future.handle((res, ex) -> {
                if (ex != null) {
                    throw new CompletionException(ex);
                }
                if (commandExecutor.getServiceManager().getCfg().isCheckLockSyncedSlaves()
                        && res.getSyncedSlaves() == 0 && availableSlaves > 0) {
                    throw new CompletionException(
                            new IllegalStateException("None of slaves were synced"));
                }

                return commandExecutor.getNow(result.toCompletableFuture());
            });
            return f;
        });
        return new CompletableFutureWrapper<>(resFuture);
    }

    private CommandBatchService createCommandBatchService(int availableSlaves) {
        if (commandExecutor instanceof CommandBatchService) {
            return (CommandBatchService) commandExecutor;
        }

        BatchOptions options = BatchOptions.defaults()
                .syncSlaves(availableSlaves, 1, TimeUnit.SECONDS);

        return new CommandBatchService(commandExecutor, options);
    }

    protected void acquireFailed(long waitTime, TimeUnit unit, long threadId) {
        commandExecutor.get(acquireFailedAsync(waitTime, unit, threadId));
    }

    protected void trySuccessFalse(long currentThreadId, CompletableFuture<Boolean> result) {
        acquireFailedAsync(-1, null, currentThreadId).whenComplete((res, e) -> {
            if (e == null) {
                result.complete(false);
            } else {
                result.completeExceptionally(e);
            }
        });
    }

    protected CompletableFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }

    @Override
    public RFuture<Boolean> isLockedAsync() {
        return isExistsAsync();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return isHeldByThread(Thread.currentThread().getId());
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        RFuture<Boolean> future = commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getRawName(), getLockName(threadId));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", new MapValueDecoder(), new IntegerReplayConvertor(0));

    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, HGET, getRawName(), getLockName(Thread.currentThread().getId()));
    }

    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    protected final <T> RFuture<T> execute(Supplier<RFuture<T>> supplier) {
        CompletableFuture<T> result = new CompletableFuture<>();
        int retryAttempts = commandExecutor.getServiceManager().getConfig().getRetryAttempts();
        AtomicInteger attempts = new AtomicInteger(retryAttempts);
        execute(attempts, result, supplier);
        return new CompletableFutureWrapper<>(result);
    }

    private <T> void execute(AtomicInteger attempts, CompletableFuture<T> result, Supplier<RFuture<T>> supplier) {
        RFuture<T> future = supplier.get();
        future.whenComplete((r, e) -> {
            if (e != null) {
                if (e.getCause().getMessage().equals("None of slaves were synced")) {
                    if (attempts.decrementAndGet() < 0) {
                        result.completeExceptionally(e);
                        return;
                    }

                    commandExecutor.getServiceManager().newTimeout(t -> execute(attempts, result, supplier),
                            commandExecutor.getServiceManager().getConfig().getRetryInterval(), TimeUnit.MILLISECONDS);
                    return;
                }

                result.completeExceptionally(e);
                return;
            }

            result.complete(r);
        });
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        return execute(() -> unlockAsync0(threadId));
    }

    private RFuture<Void> unlockAsync0(long threadId) {
        CompletionStage<Boolean> future = unlockInnerAsync(threadId);
        CompletionStage<Void> f = future.handle((opStatus, e) -> {
            cancelExpirationRenewal(threadId);

            if (e != null) {
                if (e instanceof CompletionException) {
                    throw (CompletionException) e;
                }
                throw new CompletionException(e);
            }
            if (opStatus == null) {
                IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                        + id + " thread-id: " + threadId);
                throw new CompletionException(cause);
            }

            return null;
        });

        return new CompletableFutureWrapper<>(f);
    }

    @Override
    public void unlock() {
        try {
            get(unlockAsync(Thread.currentThread().getId()));
        } catch (RedisException e) {
            if (e.getCause() instanceof IllegalMonitorStateException) {
                throw (IllegalMonitorStateException) e.getCause();
            } else {
                throw e;
            }
        }

//        Future<Void> future = unlockAsync();
//        future.awaitUninterruptibly();
//        if (future.isSuccess()) {
//            return;
//        }
//        if (future.cause() instanceof IllegalMonitorStateException) {
//            throw (IllegalMonitorStateException)future.cause();
//        }
//        throw commandExecutor.convertException(future);
    }

    @Override
    public boolean forceUnlock() {
        return get(forceUnlockAsync());
    }

    protected abstract RFuture<Boolean> unlockInnerAsync(long threadId);

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(-1, null);
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return lockAsync(leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(long currentThreadId) {
        return lockAsync(-1, null, currentThreadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return tryLockAsync(waitTime, leaseTime, unit, currentThreadId);
    }

    protected <T> CompletionStage<T> handleNoSync(long threadId, CompletionStage<T> ttlRemainingFuture) {
        CompletionStage<T> s = ttlRemainingFuture.handle((r, ex) -> {
            if (ex != null) {
                if (ex.getCause().getMessage().equals("None of slaves were synced")) {
                    return unlockInnerAsync(threadId).handle((r1, e) -> {
                        if (e != null) {
                            if (e.getCause().getMessage().equals("None of slaves were synced")) {
                                throw new CompletionException(ex.getCause());
                            }
                            e.getCause().addSuppressed(ex.getCause());
                        }
                        throw new CompletionException(ex.getCause());
                    });
                } else {
                    throw new CompletionException(ex.getCause());
                }
            }
            return CompletableFuture.completedFuture(r);
        }).thenCompose(f -> (CompletionStage<T>) f);
        return s;
    }

}
