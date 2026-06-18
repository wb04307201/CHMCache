# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

CHMCache 是一个基于 `ConcurrentHashMap` 和 LRU 策略的 Java 缓存库。提供自动过期（TTL）、大小限制、LRU 淘汰、后台清理、监控指标等特性。

- **Maven 坐标**: `io.github.wb04307201:CHMCache`
- **Java 版本**: 17
- **测试框架**: JUnit 5 (Jupiter) 5.13.4
- **许可证**: Apache 2.0
- **发布仓库**: JitPack（`io.github.wb04307201:CHMCache:<version>`）

## 常用命令

构建与编译：
```bash
mvn compile          # 仅编译主源码
mvn package          # 编译并打包为 jar
mvn clean            # 清理 target 目录
```

测试：
```bash
mvn test                                        # 运行所有测试
mvn test -Dtest=CHMCacheTest                    # 运行指定测试类
mvn test -Dtest=CHMCacheTest#testPutAndGet      # 运行单个测试方法
```

## 架构概览

所有源码位于 `src/main/java/cn/wubo/cache/`，仅 4 个类，职责清晰：

### `CHMCache.java` — 主缓存类

这是唯一的对外 API。内部用 **三个数据结构协同工作**：

1. **`ConcurrentHashMap<K, CacheValue<V>> cacheMap`** — 主存储。读路径无锁，支持高并发。
2. **`LinkedHashMap<K, Long> accessOrderMap`** — 启用 `accessOrder=true` 维护访问顺序，由独立 `ReentrantLock lruLock` 保护。**注意：写 LRU 顺序必须在 `lruLock` 内执行**（见 `get`/`put`）。
3. **`DelayQueue<DelayedItem<K>> expirationQueue`** — 显式到期项队列，背景线程每秒 drain 一次。

**后台清理线程**（`startCleanupThread`，每 1 秒一次）：
- 先排空 `expirationQueue` 中已到期项。
- 然后对 `cacheMap` 随机采样 `min(100, size/10)` 个 key 做惰性过期清理。
- 若仍超过 `maxSize`，调用 `enforceLRU()` 触发 LRU 淘汰。

**TTL 随机化**：`getRandomizedTtl()` 在默认 TTL 基础上 ±20% 浮动，避免缓存雪崩。`put(K,V)`（无 TTL 参数）使用随机 TTL；显式 `put(K,V,ttl)` 使用指定 TTL。

**指标收集**：四个 `AtomicLong` 计数器（hit/miss/eviction/cleanupTimeNanos）通过 `getMetrics()` 快照为 `MonitorMetrics`。

### `CacheValue.java` — 值包装

不可变（除内部状态）。持有 `value`、`createTime`、`ttlMillis`，通过 `isExpired()` 检查 `currentTimeMillis - createTime > ttlMillis`。

### `DelayedItem.java` — 延迟队列元素

实现 `java.util.concurrent.Delayed`，按 `expireTime` 排序。仅在 `expirationQueue` 内部流转。

### `MonitorMetrics.java` — 指标快照

不可变 POJO，对外暴露 `hitCount`/`missCount`/`evictionCount`/`cleanupTimeNanos`/`currentSize`，并计算 `getHitRate()`（命中率）与 `getAverageCleanupTimeMillis()`。

## 关键设计要点

- **线程安全分工**：`cacheMap` 用 CAS 读、`put`/`get` 内对 `accessOrderMap` 单独加 `lruLock`、`enforceLRU` 也走 `lruLock`。三者通过 `lruLock` 串行化避免 ABA/LRU 与存储不一致。
- **过期处理三段式**：
  1. **惰性删除** — `get()` 命中时检查 `isExpired()` 并当场移除。
  2. **延迟队列** — `put` 时入队 `DelayQueue`，后台线程精确到期。
  3. **采样扫描** — 后台线程每 1s 随机抽样补偿前两者遗漏的过期项。
- **LRU 实现**：遍历 `accessOrderMap.entrySet().iterator()` 从最旧往最新淘汰，**直到 `cacheMap.size() <= maxSize`**。
- **资源释放**：必须调用 `shutdown()` 关闭 `cleanupExecutor`（应用关闭时）。
- **已知性能瓶颈**（README 已声明）：`accessOrderMap` 的全局 `lruLock` 在超高并发下可能成为瓶颈。

## 测试

`src/test/java/cn/wubo/cache/CHMCacheTest.java` 覆盖 9 个用例：`putAndGet`、`getNonExistentKey`、`remove`、`expiration`、`size`、`LRUEviction`、`accessOrder`、`metrics`、`cleanup`、`defaultConstructor`。每个 `@Test` 通过 `@BeforeEach` 初始化 `CHMCache(100, 1000)`，`@AfterEach` 调用 `shutdown()`。

`LRUEviction` 与 `accessOrder` 两个用例构造独立小缓存（`maxSize=3`），**注意需要在该用例内自己调用 `shutdown()`**，因为它们不通过 `tearDown` 共享的 `cache` 字段。
