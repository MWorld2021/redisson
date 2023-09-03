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
 * 用于实现分布式锁的基类
 *
 * @author Danila Varatyntsev
 * @author Nikita Koksharov
 */
public abstract class RedissonBaseLock extends RedissonExpirable implements RLock {

    /*用于存储锁的过期时间和线程 ID。具体来说，它维护了一个 Map，将线程 ID 映射到一个计数器，以便在锁被释放时正确地处理多个线程持有锁的情况。*/
    public static class ExpirationEntry {

        private final Map<Long, Integer> threadIds = new LinkedHashMap<>();
        private volatile Timeout timeout;

        public ExpirationEntry() {
            super();
        }

        /*提供了一些同步方法来添加、删除和获取线程 ID，以及设置和获取超时时间。*/
        public synchronized void addThreadId(long threadId) {
            // 使用 compute 方法将线程 ID 添加到 threadIds Map 中，并将计数器加 1。如果计数器为 null，则将其设置为 0。最后，返回计数器的值。
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
            /*threadIds.merge(threadId, -1, (oldValue, value) -> {
                // oldValue 是线程 ID 对应的计数器的原值，value 是 -1。
                int counter = oldValue + value;
                return counter <= 0 ? null : counter;
            });*/

            threadIds.compute(threadId, (t, counter) -> {
                // 如果计数器为 null，则返回 null。再删
                if (counter == null) {
                    return null;
                }
                // 如果计数器大于 1，则将其减 1。否则，从 threadIds Map 中删除该线程 ID。
                counter--;
                if (counter == 0) {
                    return null;
                }
                // 返回计数器的值。
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
     * Returns entry name
     */
    protected String getEntryName() {
        return entryName;
    }

    /**
     * Returns lock name
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

    /**
     * 异步执行写操作，支持批量操作
     *
     * @param key             键
     * @param codec           编解码器
     * @param evalCommandType Redis命令类型
     * @param script          脚本
     * @param keys            键列表
     * @param params          参数列表
     * @param <T>             返回值类型
     * @return 返回一个RFuture对象，表示异步执行结果
     */
    protected <T> RFuture<T> evalWriteAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object... params) {
        // 获取复制信息
        CompletionStage<Map<String, String>> replicationFuture = CompletableFuture.completedFuture(Collections.emptyMap());
        if (!(commandExecutor instanceof CommandBatchService)
                && !commandExecutor.getServiceManager().getConfig().checkSkipSlavesInit()) {
            replicationFuture = commandExecutor.writeAsync(getRawName(), RedisCommands.INFO_REPLICATION);
        }
        // 处理复制信息
        CompletionStage<T> resFuture = replicationFuture.thenCompose(r -> {
            int availableSlaves = Integer.parseInt(r.getOrDefault("connected_slaves", "0"));
            // 创建批量操作服务
            CommandBatchService executorService = createCommandBatchService(availableSlaves);
            // 执行写操作
            RFuture<T> result = executorService.evalWriteAsync(key, codec, evalCommandType, script, keys, params);
            if (commandExecutor instanceof CommandBatchService) {
                return result;
            }
            // 执行批量操作
            RFuture<BatchResult<?>> future = executorService.executeAsync();
            // 处理批量操作结果
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

    /**
     * 执行 RFuture 对象的方法，支持重试
     *
     * @param supplier 获取 RFuture 对象的方法
     * @param <T>      RFuture 对象的类型
     * @return 包装了 CompletableFuture 对象的 CompletableFutureWrapper 对象
     */
    protected final <T> RFuture<T> execute(Supplier<RFuture<T>> supplier) {
        // 创建了一个CompletableFuture对象result，并获取了重试次数retryAttempts。
        CompletableFuture<T> result = new CompletableFuture<>();
        int retryAttempts = commandExecutor.getServiceManager().getConfig().getRetryAttempts();
        // 使用AtomicInteger类型的attempts变量来记录当前重试次数。
        AtomicInteger attempts = new AtomicInteger(retryAttempts);
        // 调用execute方法，重试执行RFuture对象的方法。
        execute(attempts, result, supplier);
        // 最后，返回一个CompletableFutureWrapper对象，该对象包装了result对象。
        return new CompletableFutureWrapper<>(result);
    }

    /**
     * 重试执行 RFuture 对象的方法
     *
     * @param attempts 重试次数
     * @param result   CompletableFuture对象
     * @param supplier 获取RFuture对象的方法
     */
    private <T> void execute(AtomicInteger attempts, CompletableFuture<T> result, Supplier<RFuture<T>> supplier) {
        // 调用supplier.get()方法获取一个RFuture对象future。
        RFuture<T> future = supplier.get();
        // 为future对象添加一个回调函数，当future对象执行完成时，会调用该回调函数。使用whenComplete方法对future对象进行处理。
        future.whenComplete((r, e) -> {
            if (e != null) {
                // 如果错误信息中包含None of slaves were synced，则判断当前重试次数是否已经达到上限，
                if (e.getCause().getMessage().equals("None of slaves were synced")) {
                    // 如果达到上限则抛出异常，
                    if (attempts.decrementAndGet() < 0) {
                        result.completeExceptionally(e);
                        return;
                    }
                    // 否则等待一段时间后再次调用execute方法进行重试。
                    commandExecutor.getServiceManager().newTimeout(t -> execute(attempts, result, supplier),
                            commandExecutor.getServiceManager().getConfig().getRetryInterval(), TimeUnit.MILLISECONDS);
                    return;
                }
                // 如果错误信息中不包含None of slaves were synced，则直接抛出异常。
                result.completeExceptionally(e);
                return;
            }
            // 如果future对象执行成功，则调用result.complete(r)方法将结果传递给result对象；
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

    /**
     * 处理未同步的情况
     *
     * @param threadId           线程ID
     * @param ttlRemainingFuture 剩余时间的CompletionStage
     * @return 处理后的CompletionStage
     */
    protected <T> CompletionStage<T> handleNoSync(long threadId, CompletionStage<T> ttlRemainingFuture) {
        CompletionStage<T> s = ttlRemainingFuture.handle((r, ex) -> {
            // 如果出现异常，
            if (ex != null) {
                // 判断异常的原因是否是"None of slaves were synced"，如果是，则调用unlockInnerAsync方法进行解锁，并将返回的CompletionStage再次进行处理。
                if (ex.getCause().getMessage().equals("None of slaves were synced")) {
                    return unlockInnerAsync(threadId).handle((r1, e) -> {
                        if (e != null) {
                            if (e.getCause().getMessage().equals("None of slaves were synced")) {
                                throw new CompletionException(ex.getCause());
                            }
                            // 在处理过程中，如果出现异常，会将异常添加到原始异常的suppressed异常列表中，并最终抛出原始异常。
                            e.getCause().addSuppressed(ex.getCause());
                        }
                        // 如果不是，则直接抛出异常。
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
