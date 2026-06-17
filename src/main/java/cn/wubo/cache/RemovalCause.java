package cn.wubo.cache;

/**
 * 缓存条目被移除的原因。由 {@link RemovalListener} 用于区分不同清除路径。
 */
public enum RemovalCause {

    /** 因超过 expireAfterWrite / expireAfterAccess 时间而被回收（惰性或主动清理）。 */
    EXPIRED,

    /** 因超过 maximumSize / maximumWeight 限制而被 LRU 淘汰。 */
    EVICTED,

    /** 同一个 key 被再次 put，旧值被新值替换。 */
    REPLACED,

    /** 调用方主动调用 {@link CHMCache#invalidate(Object)} 或 {@link CHMCache#invalidateAll()}。 */
    EXPLICIT,

    /** 收集器回收（预留，当前实现未启用软/弱引用）。 */
    COLLECTED,

    /** 容量上限触发的 SIZE 类型淘汰（与 EVICTED 同义，保留以兼容 Caffeine 习惯）。 */
    SIZE,

    /** 容量上限触发的 WEIGHT 类型淘汰。 */
    WEIGHT
}