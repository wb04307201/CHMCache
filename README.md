# CHMCacheCHMCache - 基于 ConcurrentHashMap 的缓存实现

一个基于 `ConcurrentHashMap` 和 LRU 策略的高性能缓存实现，支持自动过期、大小限制、LRU 淘汰和后台清理等特性。

[![](https://jitpack.io/v/com.gitee.wb04307201/CHMCache.svg)](https://jitpack.io/#com.gitee.wb04307201/CHMCache)

## 特性

- **高性能**: 基于 `ConcurrentHashMap` 实现，支持高并发访问
- **自动过期**: 支持设置缓存项的生存时间(TTL)，自动清理过期项
- **LRU 淘汰**: 当缓存达到最大容量时，自动移除最近最少使用的项
- **后台清理**: 定时清理过期项，减少主线程负担
- **线程安全**: 完整的线程安全设计，适用于多线程环境
- **监控指标**: 提供缓存命中率、清理时间等监控数据

---

## 引入

### 增加 JitPack 仓库
```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```
### 添加依赖
```xml
<dependency>
    <groupId>com.gitee.wb04307201</groupId>
    <artifactId>CHMCache</artifactId>
    <version>1.0.1</version>
</dependency>
```

---

## 使用方法

### 1. 创建缓存实例

```java
import java.util.concurrent.TimeUnit;

// 使用默认配置（最大容量1000，TTL 60秒）
CHMCache<String, Object> cache = new CHMCache<>();

// 自定义配置
CHMCache<String, Object> cache = new CHMCache<>(5000, 30_000); // 最大容量5000，TTL 30秒

// 自定义配置
CHMCache<String, Object> cache = new CHMCache<>(5000, 30, TimeUnit.SECONDS); // 最大容量5000，TTL 30秒
```


### 2. 添加缓存项

```java
// 使用默认TTL
cache.put("key1", "value1");

// 指定TTL（毫秒）
cache.put("key2", "value2", 10_000); // 10秒后过期

// 指定TTL
cache.put("key2", "value2", 10, TimeUnit.SECONDS); // 10秒后过期
```


### 3. 获取缓存项

```java
String value = cache.get("key1");
if (value != null) {
    // 使用缓存值
} else {
    // 缓存未命中，需要重新加载
}
```


### 4. 删除缓存项

```java
String removedValue = cache.remove("key1");
```


### 5. 关闭缓存

```java
// 应用关闭前调用，停止后台清理线程
cache.shutdown();
```

### 核心方法

| 方法                                                        | 描述               |
|-----------------------------------------------------------|------------------|
| `void put(K key, V value)`                                | 添加缓存项，使用默认随机TTL  |
| `void put(K key, V value, long ttlMillis)`                | 添加缓存项，指定TTL(毫秒)  |
| `void put(K key, V value, long ttlMillis, TimeUnit unit)` | 添加缓存项，指定TTL和时间单位 |
| `V get(K key)`                                            | 获取缓存项            |
| `V remove(K key)`                                         | 删除缓存项            |
| `int size()`                                              | 获取当前缓存大小         |
| `void cleanup()`                                          | 手动触发清理过期项        |
| `void shutdown()`                                         | 关闭缓存，停止后台清理线程    |
| `MonitorMetrics getMetrics()`                             | 获取监控指标           |

## 监控指标

```java
MonitorMetrics metrics = cache.getMetrics();

long hitCount = metrics.getHitCount();         // 命中次数
long missCount = metrics.getMissCount();       // 未命中次数
long evictionCount = metrics.getEvictionCount(); // 淘汰次数
double hitRate = metrics.getHitRate();         // 命中率
int currentSize = metrics.getCurrentSize();    // 当前缓存大小
```

## 缓存策略

### 过期策略

- 支持为每个缓存项设置独立的TTL（生存时间）
- 默认TTL可在构造时指定
- 使用随机化TTL（±20%）避免缓存雪崩
- 采用惰性删除和主动清理相结合的方式

### 淘汰策略

- LRU（最近最少使用）策略
- 当缓存大小超过阈值时自动触发淘汰
- 后台定时线程定期执行清理任务

## 性能优化建议

1. **合理设置缓存大小**: 根据应用内存和访问模式设置合适的 [maxSize](src/main/java/cn/wubo/cache/CHMCache.java#L25-L25)
   
2. **调整TTL**: 根据数据变化频率设置合适的过期时间
3. **监控指标**: 定期检查命中率等指标，优化缓存配置
4. **及时关闭**: 应用结束时调用 [shutdown()](src\main\java\cn\wubo\cache\CHMCache.java#L288-L298) 方法释放资源

## 注意事项

1. **线程安全**: 所有公共方法都是线程安全的
2. **内存管理**: 缓存会自动清理过期项，但建议在应用结束时调用 [shutdown()](src\main\java\cn\wubo\cache\CHMCache.java#L288-L298)
3. **LRU实现**: 当前LRU实现使用全局锁，在高并发场景下可能成为性能瓶颈