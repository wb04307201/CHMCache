# CHMCache

> 一个基于 `ConcurrentHashMap` 的高性能缓存库，提供 **Caffeine 风格的 API**，支持自动过期、LRU 淘汰、权重淘汰、事件回调、加载器等特性。

[![](https://jitpack.io/v/io.github.wb04307201/CHMCache.svg)](https://jitpack.io/#io.github.wb04307201/CHMCache)

## 核心特性

- **Caffeine 风格 API**：`maximumSize` / `expireAfterWrite` / `expireAfterAccess` / `refreshAfterWrite` / `removalListener` / `recordStats`
- **三种过期策略**：写后过期（默认）、滑动 TTL、异步刷新
- **两种淘汰策略**：按条目数淘汰、按累计权重淘汰
- **事件回调**：`RemovalListener` 区分 7 种 `RemovalCause`
- **加载器模式**：`get(K, CacheLoader)`、`computeIfAbsent`、`getAll(批量加载)`、异步 `refresh`
- **观测能力**：`CacheMetrics`（始终可用）+ `CacheStats`（recordStats 后可用，含延迟分桶与热点 Key 采样）
- **分片 LRU**（默认 16 段）：每段独立 `ReentrantLock` + `LinkedHashMap(accessOrder=true)`，段内严格 LRU、段间近似
- **Java 17 干净实现**：基于 `ConcurrentHashMap` + 分片 `LinkedHashMap` + `DelayQueue`，零三方核心依赖

---

## 引入依赖

```xml
<dependency>
    <groupId>io.github.wb04307201</groupId>
    <artifactId>CHMCache</artifactId>
    <version>1.1.0</version>
</dependency>
```

---

## 快速上手

```java
import java.time.Duration;
import cn.wubo.cache.CHMCache;

CHMCache<String, User> cache = CHMCache.<String, User>newBuilder()
    .name("users")
    .maximumSize(10_000)
    .expireAfterWrite(Duration.ofMinutes(5))
    .removalListener((k, v, cause) -> log.info("{} removed ({})", k, cause))
    .recordStats()
    .build();

cache.put("u:1", user);
User u = cache.get("u:1");
cache.invalidate("u:1");
```

---

## API 一览

### 1. 创建缓存

```java
// 条目数上限
CHMCache<String, String> c1 = CHMCache.<String, String>newBuilder()
    .maximumSize(10_000)
    .build();

// 累计权重上限
CHMCache<String, byte[]> c2 = CHMCache.<String, byte[]>newBuilder()
    .maximumWeight(100 * 1024 * 1024)
    .weigher((k, v) -> v.length)
    .build();

// 写后过期（默认）
CHMCache<String, String> c3 = CHMCache.<String, String>newBuilder()
    .maximumSize(10_000)
    .expireAfterWrite(Duration.ofMinutes(5))
    .build();

// 滑动 TTL
CHMCache<String, String> c4 = CHMCache.<String, String>newBuilder()
    .maximumSize(10_000)
    .expireAfterAccess(Duration.ofMinutes(30))
    .build();

// 写后异步刷新
CHMCache<String, String> c5 = CHMCache.<String, String>newBuilder()
    .maximumSize(10_000)
    .refreshAfterWrite(Duration.ofMinutes(4))
    .build();

// 自定义过期
CHMCache<String, String> c6 = CHMCache.<String, String>newBuilder()
    .maximumSize(10_000)
    .expireAfter((k, v, currentTimeNanos) ->
        v.startsWith("hot:") ? Duration.ofMinutes(30) : Duration.ofMinutes(1))
    .build();
```

### 2. 数据操作

```java
// put
cache.put("k", "v");
cache.put("k", "v", Duration.ofSeconds(10));

// get
String v = cache.get("k");
String loaded = cache.get("k", key -> userRepo.load((String) key));
String computed = cache.computeIfAbsent("k", key -> "v-" + key);

// 异步刷新
cache.refresh("k", key -> userRepo.load((String) key));

// 失效
cache.invalidate("k");
cache.invalidateIf(k -> ((String) k).startsWith("user:"));
cache.invalidateAll();

// 手动触发一次清理(后台清理线程之外,等同 fullScan=true)
cache.cleanup();

// 权重模式专属:当前累计权重
long ws = cache.weightedSize();

// 批量加载(命中回填,未命中走 bulkLoader)
Map<String, User> loaded = cache.getAll(
    Set.of("u:1", "u:2", "u:3"),
    missing -> userRepo.loadAll(missing));
```

### 3. 观测

```java
// 轻量指标：始终可用
CacheMetrics m = cache.metrics();
long hits = m.hitCount();
long miss = m.missCount();
double rate = m.hitRate();

// 详细统计：recordStats() 后非空
CacheStats s = cache.stats();
s.hitRate();
s.averageGetPenalty(TimeUnit.MICROSECONDS);
s.evictionCount();
s.sizeWatermark();
```

---

## Builder 完整配置

| 方法 | 说明 | 默认值 |
| --- | --- | --- |
| `name(String)` | 缓存实例名，指标前缀 | `"default"` |
| `maximumSize(long)` | 最大条目数 | 必填（与 maximumWeight 互斥） |
| `maximumWeight(long)` | 最大累计权重 | 必填（与 maximumSize 互斥） |
| `weigher(Weigher)` | 权重计算器 | 永远返回 1 |
| `expireAfterWrite(Duration)` | 写后过期 | 永不过期 |
| `expireAfterAccess(Duration)` | 滑动 TTL（get 命中时刷新） | - |
| `refreshAfterWrite(Duration)` | 写后异步刷新 | - |
| `expireAfter(Expiry)` | 自定义过期（覆盖以上三个） | - |
| `cleanupInterval(Duration)` | 后台清理线程周期 | 1 秒 |
| `removalListener(RemovalListener)` | 移除事件回调 | 无 |
| `recordStats()` | 启用详细统计 | 关 |
| `shardedLocks(boolean)` | 是否启用分片锁 | true |
| `executor(Executor)` | 自定义 refresh 任务执行器 | `ForkJoinPool.commonPool()` |

---

## 三层功能矩阵

| 能力 | Tier 1 基础 | Tier 2 进阶 | Tier 3 高阶 |
| --- | --- | --- | --- |
| 容量 | `maximumSize` | `maximumWeight` + `weigher` | - |
| 过期 | `expireAfterWrite` | `expireAfterAccess`（滑动） | `expireAfter`（自定义） |
| 失效 | `invalidate(K)` | `invalidateIf(Predicate)` | `invalidateAll()` |
| 加载 | `computeIfAbsent` | `get(K, CacheLoader)` | `refresh(K, RefreshLoader)` 异步 |
| 观测 | `CacheMetrics`（轻量） | `CacheStats`（含延迟/热点） | - |
| 性能 | - | - | 分片锁（16 段） |
| 事件 | - | `RemovalListener` + 7 种 `RemovalCause` | - |

---

## 运行测试与 Benchmark

### 单元测试

```bash
mvn test
```

当前 **232 个测试**全部通过（5 次连跑稳定无 flaky），覆盖 9 轮深度探针：

- **基础功能**：put / get / remove / 过期 / LRU / metrics / cleanup / 默认构造
- **Token 安全性**：`DelayedItem` 与 cacheMap 条目通过 token 校验，避免误删新值
- **统计一致性**：`hit/miss/eviction/load/loadFailure` 计数在并发与异常路径下均准确
- **线程安全**：8 线程 × 5000 次 put 后 `size == maximumSize`；cleanup 与 put 并发无 CME
- **权重一致性**：weight-based 模式下 `weightedSize()` 不漂移
- **自定义组件鲁棒性**：`Expiry` / `Weigher` / `RemovalListener` 异常隔离
- **AsyncRefresher**：refreshAfterWrite 模式、null 返回、shutdown 后行为
- **资源/内存峰值**：高频短 TTL 突发 + 后台清理不 OOM

### JMH Benchmark

```bash
# 编译 benchmark
mvn -Pbenchmark test-compile

# 打包可执行 jar
mvn -Pbenchmark package -DskipTests

# 跑 benchmark（4 个对照组：CHMCache / Caffeine / Guava / 裸 CHM）
java -jar target/CHMCache-2.0.0-SNAPSHOT.jar -rf json -rff target/jmh-results/results.json
```

Benchmark 覆盖：`getOnly` / `putOnly` / `mixed (90% get + 10% put)`，容量 10K / 100K，8 线程。

---

## 设计要点

- **三个数据结构协同**：`ConcurrentHashMap`（无锁读写）+ 分片 `AccessOrderTracker`（按 key hash 分桶，每桶独立 lock）+ `DelayQueue`（显式到期项）
- **原子校验**：每条缓存项携带唯一 token，`DelayedItem` 到期回收时通过 `computeIfPresent` 做原子校验，避免误删新值
- **TTL 单调时钟**：基于 `System.nanoTime()`，避免系统时钟回拨（NTP 校时）导致过期判断异常
- **后台清理**：`daemon` 线程包 `try-catch`，异常不会静默取消后续调度；`shutdown()` 幂等
- **分片 LRU**：段内严格 LRU，段间近似。文档化此权衡，与 Caffeine 同样的取舍
- **零三方核心依赖**：Caffeine / Guava 全部 `<optional>true</optional>`（仅 benchmark 用）

---

## v2.0 探针修复记录

9 轮深度探针（232 个测试）发现的 6 个缺陷已全部修复：

| # | 缺陷 | 修复 |
| --- | --- | --- |
| 1 | `removalListener(null)` 静默接受 | `CHMCacheBuilder` 增 `Objects.requireNonNull` 校验 |
| 2 | `computeTtl` 中 `isRead` 优先级高于 `isCreate`，`expireAfterCreate` 永不触发 | 调整分支顺序，`isCreate` 优先 |
| 3 | `expireAfterWrite` 不重置 `slidingTtl`，与 `expireAfterAccess` 冲突 | 显式重置 `slidingTtl = false` |
| 4 | `loadCount` 在 loader 返回 null 时被累加，破坏 `loadFailureRate` 语义 | 移入 `if (loaded != null)` 分支 |
| 5 | `ReentrantLock` 与 `synchronized` 混用导致 `LinkedHashMap` CME | `AccessOrderTracker` 统一使用 `ReentrantLock` |
| 6 | DelayQueue 路径过期清理不减少 `currentWeight`，`weightedSize` 单调递增 | 改用 `int[1]` 捕获 weight 后再 `addAndGet(-weight)` |

---

## 注意事项

1. **线程安全**：所有公共方法都是线程安全的
2. **内存管理**：缓存会自动清理过期项，但建议在应用结束时调用 `shutdown()` 释放后台线程
3. **LRU 实现**：分片 LRU 是段内严格、段间近似（性能换精度的取舍）。如需严格全局 LRU 顺序，请使用 `shardedLocks(false)` 关闭分片
4. **滑动 TTL**：启用 `expireAfterAccess` 后，每次 get 命中会有一次原子 `replace` 开销
5. **过期时间单位**：所有时间相关配置使用 `Duration`，内部统一以纳秒存储
6. **RemovalListener 执行点**：`RemovalListener` 在段锁内被同步调用，慢 listener 会阻塞同段其他 put / evict 路径。建议将重活（IO、日志）异步化或使用专门的清理线程
7. **自定义 Expiry / Weigher 异常**：若用户回调抛异常，会向上传播至调用方且当前条目不会写入。生产环境请在回调内自行 try-catch

---

## 升级说明（v1.x → v2.0）

v2.0 **破坏向后兼容**。迁移清单：

| v1.x | v2.0 |
| --- | --- |
| `new CHMCache<>()` / `new CHMCache<>(maxSize, ttl)` | `CHMCache.newBuilder().maximumSize(...).build()` |
| `new CHMCache<>(maxSize, ttl, unit)` / `new CHMCache<>(maxSize, ttl, unit, cleanupInterval)` | Builder 链式调用 |
| `put(k, v, long)` / `put(k, v, long, unit)` | `put(k, v, Duration.ofSeconds(...))` |
| `cache.remove(k)` | `cache.invalidate(k)` |
| `cache.clear()` | `cache.invalidateAll()` |
| `cache.getMetrics().getHitCount()` | `cache.metrics().hitCount()` |
| `cache.putIfAbsent(k, v)` | `cache.computeIfAbsent(k, key -> v)` |
| `CacheLoader` 隐式存在 | `CacheLoader` / `RefreshLoader` 显式传入 |
