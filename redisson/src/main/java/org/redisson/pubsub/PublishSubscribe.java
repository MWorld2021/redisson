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
package org.redisson.pubsub;

import org.redisson.PubSubEntry;
import org.redisson.client.BaseRedisPubSubListener;
import org.redisson.client.ChannelName;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.codec.LongCodec;
import org.redisson.misc.AsyncSemaphore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Nikita Koksharov
 */
abstract class PublishSubscribe<E extends PubSubEntry<E>> {

    private final ConcurrentMap<String, E> entries = new ConcurrentHashMap<>();
    /**
     * Redis pub/sub listener
     */
    private final PublishSubscribeService service;

    PublishSubscribe(PublishSubscribeService service) {
        super();
        this.service = service;
    }

    public void unsubscribe(E entry, String entryName, String channelName) {
        ChannelName cn = new ChannelName(channelName);
        AsyncSemaphore semaphore = service.getSemaphore(cn);
        semaphore.acquire().thenAccept(c -> {
            if (entry.release() == 0) {
                entries.remove(entryName);
                service.unsubscribeLocked(cn)
                        .whenComplete((r, e) -> {
                            semaphore.release();
                        });
            } else {
                semaphore.release();
            }
        });
    }

    public void timeout(CompletableFuture<?> promise) {
        service.timeout(promise);
    }

    public void timeout(CompletableFuture<?> promise, long timeout) {
        service.timeout(promise, timeout);
    }

    public CompletableFuture<E> subscribe(String entryName, String channelName) {
        // 通过 channelName 获取 AsyncSemaphore 对象, 该信号量是一个计数信号量, 用于控制并发数, 该信号量的计数器初始值为 1, 每次调用 acquire 方法, 计数器减 1, 每次调用 release 方法, 计数器加 1, 当计数器为 0 时, 表示信号量被占用, 其他线程需要等待, 直到信号量释放, 线程才能继续执行。
        AsyncSemaphore semaphore = service.getSemaphore(new ChannelName(channelName));
        // 创建一个新的 CompletableFuture 对象, 用于返回
        CompletableFuture<E> newPromise = new CompletableFuture<>();
        // 调用信号量的 acquire 方法, 该方法返回一个 CompletableFuture 对象, 该对象的结果为信号量的计数器值, 该方法会阻塞当前线程, 直到信号量被释放, 线程才能继续执行。
        semaphore.acquire().thenAccept(c -> {
            // 如果 newPromise 对象已经完成, 则释放信号量, 并返回
            if (newPromise.isDone()) {
                // 释放信号量，计数器加 1
                semaphore.release();
                return;
            }
            // 从 entries 集合中获取 entryName 对应的 PubSubEntry 对象
            E entry = entries.get(entryName);
            if (entry != null) {
                // 如果 entry 对象不为空, 则调用 entry 对象的 acquire 方法, 该方法会将 entry 对象的引用计数加 1
                entry.acquire();
                // 释放信号量，计数器加 1
                semaphore.release();
                // 将 entry 对象的 promise 对象的结果赋值给 newPromise 对象
                entry.getPromise().whenComplete((r, e) -> {
                    if (e != null) {
                        // 如果 entry 对象的 promise 对象的结果为异常, 则将异常赋值给 newPromise 对象
                        newPromise.completeExceptionally(e);
                        return;
                    }
                    // 如果 entry 对象的 promise 对象的结果为正常, 则将结果赋值给 newPromise 对象
                    newPromise.complete(r);
                });
                return;
            }

            E value = createEntry(newPromise);
            value.acquire();
            // 将 entryName 和 value 对象放入 entries 集合中
            E oldValue = entries.putIfAbsent(entryName, value);
            if (oldValue != null) {
                oldValue.acquire();
                semaphore.release();
                oldValue.getPromise().whenComplete((r, e) -> {
                    if (e != null) {
                        newPromise.completeExceptionally(e);
                        return;
                    }
                    newPromise.complete(r);
                });
                return;
            }
            // 创建一个 RedisPubSubListener 对象, 当 Redis 服务器向客户端发送消息时, 该该对象的 onMessage 方法会被调用
            RedisPubSubListener<Object> listener = createListener(channelName, value);
            // 调用 service 对象的 subscribeNoTimeout 方法, 该方法会向 Redis 服务器发送 SUBSCRIBE 命令, 并返回一个 CompletableFuture 对象
            CompletableFuture<PubSubConnectionEntry> s = service.subscribeNoTimeout(LongCodec.INSTANCE, channelName, semaphore, listener);
            newPromise.whenComplete((r, e) -> {
                if (e != null) {
                    s.completeExceptionally(e);
                }
            });
            s.whenComplete((r, e) -> {
                if (e != null) {
                    entries.remove(entryName);
                    value.getPromise().completeExceptionally(e);
                    return;
                }
                value.getPromise().complete(value);
            });

        });

        return newPromise;
    }

    protected abstract E createEntry(CompletableFuture<E> newPromise);

    protected abstract void onMessage(E value, Long message);

    private RedisPubSubListener<Object> createListener(String channelName, E value) {
        RedisPubSubListener<Object> listener = new BaseRedisPubSubListener() {

            @Override
            public void onMessage(CharSequence channel, Object message) {
                if (!channelName.equals(channel.toString())) {
                    return;
                }

                PublishSubscribe.this.onMessage(value, (Long) message);
            }
        };
        return listener;
    }

}
