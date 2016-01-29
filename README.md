Redisson - distributed and scalable Java objects powered by Redis. Advanced Java Redis client
====

[![Maven Central](https://img.shields.io/maven-central/v/org.redisson/redisson.svg?style=flat-square)](https://maven-badges.herokuapp.com/maven-central/org.redisson/redisson/)

Use familiar Java data structures with power of [Redis](http://redis.io).

Based on high-performance async and lock-free Java Redis client and [Netty 4](http://netty.io) framework.  
Redis 2.8+ and JDK 1.6+ compatible.


Read [wiki](https://github.com/mrniko/redisson/wiki) for more Redisson usage details.  
[Redisson releases history](https://github.com/mrniko/redisson/blob/master/CHANGELOG.md).


Licensed under the Apache License 2.0.

Welcome to support chat - [![Join the chat at https://gitter.im/mrniko/redisson](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/mrniko/redisson?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Features
================================
* [AWS ElastiCache](https://aws.amazon.com/elasticache/) servers mode:
    1. automatic new master server discovery
    2. automatic new slave servers discovery
    3. read data using slave servers, write data using master server
* Cluster servers mode:
    1. automatic master and slave discovery
    2. automatic new master server discovery
    3. automatic new slave servers discovery
    4. automatic slots change discovery
    5. read data using slave servers, write data using master server
* Sentinel servers mode: 
    1. automatic master and slave servers discovery
    2. automatic new master server discovery
    3. automatic new slave servers discovery
    4. automatic slave servers offline/online discovery  
    5. automatic sentinel servers discovery  
    6. read data using slave servers, write data using master server
* Master with Slave servers mode: read data using slave servers, write data using master server
* Single server mode: read and write data using single server
* Lua scripting  
* Distributed implementation of `java.util.BitSet`  
* Distributed implementation of `java.util.List`  
* Distributed implementation of `java.util.Set` with TTL support for each entry
* Distributed implementation of `java.util.SortedSet`  
* Distributed implementation of `java.util.Queue`  
* Distributed implementation of `java.util.concurrent.BlockingQueue`  
* Distributed implementation of `java.util.Deque`  
* Distributed implementation of `java.util.concurrent.BlockingDeque`  
* Distributed implementation of `java.util.Map` with TTL support for each entry 
* Distributed implementation of `java.util.concurrent.ConcurrentMap` with TTL support for each entry 
* Distributed implementation of reentrant `java.util.concurrent.locks.Lock` with TTL support  
* Distributed implementation of reentrant `java.util.concurrent.locks.ReadWriteLock` with TTL support  
* Distributed alternative to the `java.util.concurrent.atomic.AtomicLong`  
* Distributed alternative to the `java.util.concurrent.CountDownLatch`  
* Distributed alternative to the `java.util.concurrent.Semaphore`  
* Distributed publish/subscribe messaging via `org.redisson.core.RTopic`  
* Distributed HyperLogLog via `org.redisson.core.RHyperLogLog`  
* Asynchronous interface for each object  
* Asynchronous connection pool  
* Thread-safe implementation  
* All commands executes in an atomic way  
* [Spring cache](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/cache.html) integration  
* Supports [Reactive Streams](http://www.reactive-streams.org)
* Supports [Redis pipelining](http://redis.io/topics/pipelining) (command batches)  
* Supports Android platform  
* Supports auto-reconnect  
* Supports failed to send command auto-retry  
* Supports OSGi  
* Supports many popular codecs ([Jackson JSON](https://github.com/FasterXML/jackson), [CBOR](http://cbor.io/), [MsgPack](http://msgpack.org/), [Kryo](https://github.com/EsotericSoftware/kryo), [FST](https://github.com/RuedigerMoeller/fast-serialization), [LZ4](https://github.com/jpountz/lz4-java), [Snappy](https://github.com/xerial/snappy-java) and JDK Serialization)
* With over 500 unit tests  

Projects using Redisson
================================
Setronica: [setronica.com](http://setronica.com/)  
Monits: [monits.com](http://monits.com/)  
Brookhaven National Laboratory: [bnl.gov](http://bnl.gov/)  
Netflix Dyno client: [dyno] (https://github.com/Netflix/dyno)  
武林Q传: [nbrpg.com](http://www.nbrpg.com/)  
Ocous: [ocous.com](http://www.ocous.com/)  
Invaluable: [invaluable.com](http://www.invaluable.com/)

Latest version changelog
=================================
####28-Jan-2016 - version 2.2.6 released  

Feature - __new object added__ `RedissonMultiLock`  
Feature - `move` method added to `RSet`, `RSetReactive` objects (thanks to thrau)  
Feature - `put` methods with `maxIdleTime` param added to `RMapCache` object  
Feature - `RList.subList` returns `live` view object  
Feature - `readAll` method added to `RList` and `RSet` objects  
Feature - `trim` method added to `RList` object  
Feature - ability to read/write Redisson config object from/to `JSON` or `YAML` format  
Feature - [Spring cache](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/cache.html) integration  
Feature - `readMode` setting added  
Improvement - `RSetCache` object entry eviction optimization  
Improvement - `RList` object optimization  
Improvement - `RedissonCountDownLatchAsync` interface added  
Improvement - cluster restrictions removed from `loadBucketValues` and `saveBuckets` methods  
Fixed - wrong ByteBuf read position in all codecs based on `StringCodec`  
Fixed - can't connect with password to Sentinel and Elasticache servers  
Fixed - Cluster slave discovery (regression since 2.1.5)  
Fixed - Sentinel slave discovery (regression since 2.1.5)  

### Maven 

Include the following to your dependency list:

    <dependency>
       <groupId>org.redisson</groupId>
       <artifactId>redisson</artifactId>
       <version>2.2.6</version>
    </dependency>

### Gradle

    compile 'org.redisson:redisson:2.2.6'

### Supported by

YourKit is kindly supporting this open source project with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
<a href="http://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a> and
<a href="http://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>.
