# CHMCache

一个基于 `ConcurrentHashMap` 和 LRU 策略的高性能缓存实现，支持自动过期、大小限制、LRU 淘汰和后台清理等特性。

## 特性

- **高性能**: 基于 `ConcurrentHashMap` 实现，支持高并发访问
- **自动过期**: 支持设置缓存项的生存时间(TTL)，自动清理过期数据
- **LRU淘汰**: 当缓存达到最大容量时，自动移除最近最少使用的项
- **后台清理**: 定时清理线程自动处理过期和多余的缓存项
- **统计监控**: 提供命中率、清理次数等监控指标

## 使用方法

### 1. 创建缓存实例

```java
// 使用默认配置（最大容量1000，TTL 60秒）
CHMCache<String, Object> cache = new CHMCache<>();

// 自定义配置
CHMCache<String, Object> cache = new CHMCache<>(5000, 30_000); // 最大容量5000，TTL 30秒
```


### 2. 添加缓存项

```java
// 使用默认TTL
cache.put("key1", "value1");

// 指定TTL（毫秒）
cache.put("key2", "value2", 10_000); // 10秒后过期
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


## 核心组件

### CHMCache<K, V>

主要的缓存实现类，提供完整的缓存操作接口：
- [put(K key, V value)](src/main/java/cn/wubo/cache/CHMCache.java#L147-L149): 添加缓存项（使用随机TTL）
- `put(K key, V value, long ttlMillis)`: 添加缓存项（指定TTL）
- [get(K key)](src/main/java/cn/wubo/cache/CHMCache.java#L181-L206): 获取缓存项
- [remove(K key)](src/main/java/cn/wubo/cache/CHMCache.java#L213-L220): 删除缓存项
- [cleanup()](src/main/java/cn/wubo/cache/CHMCache.java#L225-L253): 手动清理过期项
- [size()](src/main/java/cn/wubo/cache/CHMCache.java#L259-L261): 获取缓存大小
- [shutdown()](src/main/java/cn/wubo/cache/CHMCache.java#L266-L276): 关闭缓存

### CacheValue<V>

缓存值的包装类，包含实际值和过期时间信息。

### DelayedItem<K>

用于延迟队列的缓存项包装，实现 `Delayed` 接口，支持按过期时间排序。

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

## 监控指标

- [getHitRate()](src/main/java/cn/wubo/cache/CHMCache.java#L282-L285): 缓存命中率
- [getHitCount()](src/main/java/cn/wubo/cache/CHMCache.java#L291-L293): 命中次数
- [getMissCount()](src/main/java/cn/wubo/cache/CHMCache.java#L299-L301): 未命中次数
- [getEvictionCount()](src/main/java/cn/wubo/cache/CHMCache.java#L307-L309): 淘汰次数
- [getAverageCleanupTimeMillis()](src/main/java/cn/wubo/cache/CHMCache.java#L315-L318): 平均清理耗时

