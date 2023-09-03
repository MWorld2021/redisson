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
import org.redisson.api.RFuture;
import org.redisson.client.RedisTimeoutException;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.CompletableFutureWrapper;
import org.redisson.pubsub.LockPubSub;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * {@link java.util.concurrent.locks.Lock}的分布式实现
 * Implements reentrant lock.<br>
 * 实现可重入锁<br>
 * Lock will be removed automatically if client disconnects.
 * 如果客户端断开连接，锁定将自动删除。
 * <p>
 * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order.
 * 实现<b>非公平<b>锁定，因此不能保证获得订单。
 *
 * @author Nikita Koksharov
 */
public class RedissonLock extends RedissonBaseLock {

    protected long internalLockLeaseTime;

    protected final LockPubSub pubSub;

    final CommandAsyncExecutor commandExecutor;

    /**
     * Create a new instance of RedissonLock object for the given name.
     *
     * @param commandExecutor
     * @param name            名称
     */
    public RedissonLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.internalLockLeaseTime = commandExecutor.getServiceManager().getCfg().getLockWatchdogTimeout();
        this.pubSub = commandExecutor.getConnectionManager().getSubscribeService().getLockPubSub();
    }

    /**
     * ChannelName
     */
    String getChannelName() {
        return prefixName("redisson_lock__channel", getRawName());
    }

    @Override
    public void lock() {
        try {
            // 尝试获取锁，等待时间为 -1，租约时间为 null，不可中断
            lock(-1, null, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            // 尝试获取锁，等待时间为 leaseTime，租约时间为 unit，不可中断
            lock(leaseTime, unit, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        // 尝试获取锁，等待时间为 -1，租约时间为 null，可中断
        lock(-1, null, true);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        // 尝试获取锁，等待时间为 leaseTime，租约时间为 unit，可中断
        lock(leaseTime, unit, true);
    }

    private void lock(long leaseTime, TimeUnit unit, boolean interruptibly) throws InterruptedException {
        // 获取当前线程的 id = 标识符 / Get the ID of the current thread
        long threadId = Thread.currentThread().getId();
        // 尝试获取锁 / Attempt to acquire the lock
        Long ttl = tryAcquire(-1, leaseTime, unit, threadId);
        // lock acquired 锁获取成功 / If the lock is acquired successfully, return
        if (ttl == null) {
            return;
        }
        // waiting for message 订阅锁的通道并等待指示锁已释放的消息 / Subscribe to the lock's channel and wait for a message indicating that the lock has been released
        CompletableFuture<RedissonLockEntry> future = subscribe(threadId);
        // 等待订阅成功，设置超时时间，以确保在等待锁时不会无限期地阻塞。具体来说，它会将 future 作为参数传递给 timeout 方法，该方法将在一定时间后取消订阅 future。这样，如果在等待锁时发生超时，future 将被取消订阅，从而使线程能够继续执行。
        pubSub.timeout(future);
        // 获取订阅成功的结果 / Get the result of the subscription
        RedissonLockEntry entry;
        if (interruptibly) {
            // 如果是可中断的，获取中断的 future
            entry = commandExecutor.getInterrupted(future);
        } else {
            entry = commandExecutor.get(future);
        }

        try {
            while (true) {
                // 尝试获取锁 / Attempt to acquire the lock
                ttl = tryAcquire(-1, leaseTime, unit, threadId);
                // lock acquired 如果锁获取成功，直接跳出循环 / If the lock is acquired successfully, break the loop
                if (ttl == null) {
                    break;
                }

                // waiting for message 等待锁释放的消息
                if (ttl >= 0) {
                    try {
                        // 尝试获取 permit，Semaphore 的 tryAcquire 方法是尝试获取一个许可证，如果当前有可用的许可证，则获取许可证并返回 true，否则返回 false。
                        // 如果 tryAcquire 方法返回 false，则可以使用 acquire 方法等待许可证变为可用。
                        entry.getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        if (interruptibly) {
                            throw e;
                        }
                        entry.getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    }
                } else {
                    if (interruptibly) {
                        // 获取 permit
                        entry.getLatch().acquire();
                    } else {
                        // blocking until one is available
                        entry.getLatch().acquireUninterruptibly();
                    }
                }
            }
        } finally {
            // 等待都超时了，直接取消订阅锁的 channel / If all the waiting times are over, cancel the subscription to the channel
            unsubscribe(entry, threadId);
        }
//        get(lockAsync(leaseTime, unit));
    }

    /***
     * 尝试获取锁
     * @param waitTime 等待锁变为可用的最长时间。如果在此时间内锁不可用，则该方法将返回 null。
     * @param leaseTime 锁将被持有的时间。如果 leaseTime 大于 0，则在 leaseTime 经过后，锁将自动释放。如果 leaseTime 小于或等于 0，则锁将一直保持，直到显式释放。
     * @param unit waitTime 和 leaseTime 的时间单位。
     * @param threadId 尝试获取锁的线程的 ID。
     * @return 返回一个 Long 值，表示锁释放剩余的时间，如果成功获取锁，则返回 null。
     */
    private Long tryAcquire(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        return get(tryAcquireAsync0(waitTime, leaseTime, unit, threadId));
    }

    /***
     * 异步尝试获取锁
     * @param waitTime 等待锁变为可用的最长时间。如果在此时间内锁不可用，则该方法将返回 null。
     * @param leaseTime 锁将被持有的时间。如果 leaseTime 大于 0，则在 leaseTime 经过后，锁将自动释放。如果 leaseTime 小于或等于 0，则锁将一直保持，直到显式释放。
     * @param unit waitTime 和 leaseTime 的时间单位。
     * @param threadId 尝试获取锁的线程的 ID。
     * @return 返回一个 RFuture<Long> 对象
     */
    private RFuture<Long> tryAcquireAsync0(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        /*tryAcquireAsync0 is a helper method that executes tryAcquireAsync asynchronously.*/
        return execute(() -> tryAcquireAsync(waitTime, leaseTime, unit, threadId));
    }

    private RFuture<Boolean> tryAcquireOnceAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        CompletionStage<Boolean> acquiredFuture;
        if (leaseTime > 0) {
            acquiredFuture = tryLockInnerAsync(waitTime, leaseTime, unit, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        } else {
            acquiredFuture = tryLockInnerAsync(waitTime, internalLockLeaseTime,
                    TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        }

        acquiredFuture = handleNoSync(threadId, acquiredFuture);

        CompletionStage<Boolean> f = acquiredFuture.thenApply(acquired -> {
            // lock acquired
            if (acquired) {
                if (leaseTime > 0) {
                    internalLockLeaseTime = unit.toMillis(leaseTime);
                } else {
                    scheduleExpirationRenewal(threadId);
                }
            }
            return acquired;
        });
        return new CompletableFutureWrapper<>(f);
    }

    /**
     * Tries to acquire lock and returns remaining time to live of this lock instance.
     * 尝试获取锁并返回此锁实例的剩余生存时间。
     *
     * @param waitTime  - wait time
     *                  等待时间
     * @param leaseTime - lease time
     *                  租约时间
     * @param unit      - time unit
     *                  时间单位
     * @param threadId  - id of thread
     *                  线程ID
     * @return remaining time to live of this lock instance or null if lock not acquired
     * 此锁实例的剩余生存时间，如果未获取锁，则为null
     * 首先根据leaseTime的值选择合适的锁超时时间，然后调用tryLockInnerAsync方法尝试获取锁。
     * 如果获取锁成功，它会根据leaseTime的值更新锁的租约时间或者启动一个定时任务来定期续租锁。
     * 如果获取锁失败，它会返回null。
     */
    private <T> RFuture<Long> tryAcquireAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        RFuture<Long> ttlRemainingFuture;
        if (leaseTime > 0) {
            // 如果leaseTime大于0，则使用传入的leaseTime，使用tryLockInnerAsync方法用于尝试获取锁
            ttlRemainingFuture = tryLockInnerAsync(waitTime, leaseTime, unit, threadId, RedisCommands.EVAL_LONG);
        } else {
            // 如果leaseTime小于等于0，则使用默认的锁租约时间internalLockLeaseTime
            ttlRemainingFuture = tryLockInnerAsync(waitTime, internalLockLeaseTime,
                    TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_LONG);
        }
        // handleNoSync方法用于处理异步获取锁的结果
        CompletionStage<Long> s = handleNoSync(threadId, ttlRemainingFuture);
        ttlRemainingFuture = new CompletableFutureWrapper<>(s);
        // 如果获取锁成功，则根据leaseTime的值更新锁的租约时间或者启动一个定时任务来定期续租锁
        CompletionStage<Long> f = ttlRemainingFuture.thenApply(ttlRemaining -> {
            // lock acquired
            if (ttlRemaining == null) {
                if (leaseTime > 0) {
                    internalLockLeaseTime = unit.toMillis(leaseTime);
                } else {
                    scheduleExpirationRenewal(threadId);
                }
            }
            return ttlRemaining;
        });
        return new CompletableFutureWrapper<>(f);
    }

    @Override
    public boolean tryLock() {
        return get(tryLockAsync());
    }

    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        /*
            使用 evalWriteAsync 方法执行 Redis 命令，尝试获取锁。
            如果锁不存在或者当前线程已经持有锁，则增加锁的计数器并设置锁的过期时间，然后返回 null。否则返回锁的剩余过期时间。
            if ((redis.call('exists', KEYS[1]) == 0) or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then
                redis.call('hincrby', KEYS[1], ARGV[2], 1);
                redis.call('pexpire', KEYS[1], ARGV[1]);
                return nil;
            end;
            return redis.call('pttl', KEYS[1]);
            其中 KEYS[1] 是锁的名称，ARGV[1] 是锁的过期时间，ARGV[2] 是当前线程的 ID。
         */
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                "if ((redis.call('exists', KEYS[1]) == 0) " +
                        "or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return nil; " +
                        "end; " +
                        "return redis.call('pttl', KEYS[1]);",
                Collections.singletonList(getRawName()), unit.toMillis(leaseTime), getLockName(threadId));
    }

    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
        long time = unit.toMillis(waitTime);
        long current = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        Long ttl = tryAcquire(waitTime, leaseTime, unit, threadId);
        // lock acquired
        if (ttl == null) {
            return true;
        }

        time -= System.currentTimeMillis() - current;
        if (time <= 0) {
            acquireFailed(waitTime, unit, threadId);
            return false;
        }

        current = System.currentTimeMillis();
        // 订阅分布式锁, 解锁时进行通知
        CompletableFuture<RedissonLockEntry> subscribeFuture = subscribe(threadId);
        try {
            subscribeFuture.get(time, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            if (!subscribeFuture.completeExceptionally(new RedisTimeoutException(
                    "Unable to acquire subscription lock after " + time + "ms. " +
                            "Try to increase 'subscriptionsPerConnection' and/or 'subscriptionConnectionPoolSize' parameters."))) {
                subscribeFuture.whenComplete((res, ex) -> {
                    if (ex == null) {
                        unsubscribe(res, threadId);
                    }
                });
            }
            acquireFailed(waitTime, unit, threadId);
            return false;
        } catch (ExecutionException e) {
            acquireFailed(waitTime, unit, threadId);
            return false;
        }

        try {
            time -= System.currentTimeMillis() - current;
            if (time <= 0) {
                acquireFailed(waitTime, unit, threadId);
                return false;
            }

            while (true) {
                long currentTime = System.currentTimeMillis();
                ttl = tryAcquire(waitTime, leaseTime, unit, threadId);
                // lock acquired
                if (ttl == null) {
                    return true;
                }

                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    acquireFailed(waitTime, unit, threadId);
                    return false;
                }

                // waiting for message
                currentTime = System.currentTimeMillis();
                if (ttl >= 0 && ttl < time) {
                    commandExecutor.getNow(subscribeFuture).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    commandExecutor.getNow(subscribeFuture).getLatch().tryAcquire(time, TimeUnit.MILLISECONDS);
                }

                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    acquireFailed(waitTime, unit, threadId);
                    return false;
                }
            }
        } finally {
            unsubscribe(commandExecutor.getNow(subscribeFuture), threadId);
        }
//        return get(tryLockAsync(waitTime, leaseTime, unit));
    }

    /**
     * 通过线程标识符去订阅锁的消息
     */
    protected CompletableFuture<RedissonLockEntry> subscribe(long threadId) {
        // subscribe to channel
        return pubSub.subscribe(getEntryName(), getChannelName());
    }

    protected void unsubscribe(RedissonLockEntry entry, long threadId) {
        pubSub.unsubscribe(entry, getEntryName(), getChannelName());
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }

    @Override
    protected void cancelExpirationRenewal(Long threadId) {
        super.cancelExpirationRenewal(threadId);
        this.internalLockLeaseTime = commandExecutor.getServiceManager().getCfg().getLockWatchdogTimeout();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('del', KEYS[1]) == 1) then "
                        + "redis.call('publish', KEYS[2], ARGV[1]); "
                        + "return 1 "
                        + "else "
                        + "return 0 "
                        + "end",
                Arrays.asList(getRawName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }


    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                        "return nil;" +
                        "end; " +
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        "if (counter > 0) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                        "return 0; " +
                        "else " +
                        "redis.call('del', KEYS[1]); " +
                        "redis.call('publish', KEYS[2], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return nil;",
                Arrays.asList(getRawName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId));
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long currentThreadId) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        RFuture<Long> ttlFuture = tryAcquireAsync0(-1, leaseTime, unit, currentThreadId);
        ttlFuture.whenComplete((ttl, e) -> {
            if (e != null) {
                result.completeExceptionally(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.complete(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            CompletableFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            pubSub.timeout(subscribeFuture);
            subscribeFuture.whenComplete((res, ex) -> {
                if (ex != null) {
                    result.completeExceptionally(ex);
                    return;
                }

                lockAsync(leaseTime, unit, res, result, currentThreadId);
            });
        });

        return new CompletableFutureWrapper<>(result);
    }

    private void lockAsync(long leaseTime, TimeUnit unit,
                           RedissonLockEntry entry, CompletableFuture<Void> result, long currentThreadId) {
        RFuture<Long> ttlFuture = tryAcquireAsync0(-1, leaseTime, unit, currentThreadId);
        ttlFuture.whenComplete((ttl, e) -> {
            if (e != null) {
                unsubscribe(entry, currentThreadId);
                result.completeExceptionally(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                unsubscribe(entry, currentThreadId);
                if (!result.complete(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            if (entry.getLatch().tryAcquire()) {
                lockAsync(leaseTime, unit, entry, result, currentThreadId);
            } else {
                // waiting for message
                AtomicReference<Timeout> futureRef = new AtomicReference<>();
                Runnable listener = () -> {
                    if (futureRef.get() != null) {
                        futureRef.get().cancel();
                    }
                    lockAsync(leaseTime, unit, entry, result, currentThreadId);
                };

                entry.addListener(listener);

                if (ttl >= 0) {
                    Timeout scheduledFuture = commandExecutor.getServiceManager().newTimeout(timeout -> {
                        if (entry.removeListener(listener)) {
                            lockAsync(leaseTime, unit, entry, result, currentThreadId);
                        }
                    }, ttl, TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }
        });
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        return execute(() -> tryAcquireOnceAsync(-1, -1, null, threadId));
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit,
                                         long currentThreadId) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
        long currentTime = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync0(waitTime, leaseTime, unit, currentThreadId);
        ttlFuture.whenComplete((ttl, e) -> {
            if (e != null) {
                result.completeExceptionally(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.complete(true)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            long el = System.currentTimeMillis() - currentTime;
            time.addAndGet(-el);

            if (time.get() <= 0) {
                trySuccessFalse(currentThreadId, result);
                return;
            }

            long current = System.currentTimeMillis();
            AtomicReference<Timeout> futureRef = new AtomicReference<>();
            CompletableFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            pubSub.timeout(subscribeFuture, time.get());
            subscribeFuture.whenComplete((r, ex) -> {
                if (ex != null) {
                    result.completeExceptionally(ex);
                    return;
                }

                if (futureRef.get() != null) {
                    futureRef.get().cancel();
                }

                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);

                tryLockAsync(time, waitTime, leaseTime, unit, r, result, currentThreadId);
            });
            if (!subscribeFuture.isDone()) {
                Timeout scheduledFuture = commandExecutor.getServiceManager().newTimeout(timeout -> {
                    if (!subscribeFuture.isDone()) {
                        subscribeFuture.cancel(false);
                        trySuccessFalse(currentThreadId, result);
                    }
                }, time.get(), TimeUnit.MILLISECONDS);
                futureRef.set(scheduledFuture);
            }
        });


        return new CompletableFutureWrapper<>(result);
    }

    private void tryLockAsync(AtomicLong time, long waitTime, long leaseTime, TimeUnit unit,
                              RedissonLockEntry entry, CompletableFuture<Boolean> result, long currentThreadId) {
        if (result.isDone()) {
            unsubscribe(entry, currentThreadId);
            return;
        }

        if (time.get() <= 0) {
            unsubscribe(entry, currentThreadId);
            trySuccessFalse(currentThreadId, result);
            return;
        }

        long curr = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync0(waitTime, leaseTime, unit, currentThreadId);
        ttlFuture.whenComplete((ttl, e) -> {
            if (e != null) {
                unsubscribe(entry, currentThreadId);
                result.completeExceptionally(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                unsubscribe(entry, currentThreadId);
                if (!result.complete(true)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            long el = System.currentTimeMillis() - curr;
            time.addAndGet(-el);

            if (time.get() <= 0) {
                unsubscribe(entry, currentThreadId);
                trySuccessFalse(currentThreadId, result);
                return;
            }

            // waiting for message
            long current = System.currentTimeMillis();
            if (entry.getLatch().tryAcquire()) {
                tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
            } else {
                AtomicBoolean executed = new AtomicBoolean();
                AtomicReference<Timeout> futureRef = new AtomicReference<>();
                Runnable listener = () -> {
                    executed.set(true);
                    if (futureRef.get() != null) {
                        futureRef.get().cancel();
                    }

                    long elapsed = System.currentTimeMillis() - current;
                    time.addAndGet(-elapsed);

                    tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
                };
                entry.addListener(listener);

                long t = time.get();
                if (ttl >= 0 && ttl < time.get()) {
                    t = ttl;
                }
                if (!executed.get()) {
                    Timeout scheduledFuture = commandExecutor.getServiceManager().newTimeout(timeout -> {
                        if (entry.removeListener(listener)) {
                            long elapsed = System.currentTimeMillis() - current;
                            time.addAndGet(-elapsed);

                            tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
                        }
                    }, t, TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }
        });
    }


}
