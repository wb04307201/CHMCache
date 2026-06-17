package cn.wubo.cache;

/**
 * 预置的 {@link Eviction} 工厂。
 */
public final class Evictions {

    private Evictions() {}

    /**
     * 按条目数淘汰。需配置 maximumSize。
     */
    public static Eviction sizeBased() {
        return new Eviction() {
            @Override
            public boolean shouldEvict(long currentSize, long maxSize, long currentWeight, long maxWeight) {
                return maxSize > 0 && currentSize > maxSize;
            }

            @Override
            public long total(long currentSize, long currentWeight) {
                return currentSize;
            }

            @Override
            public long limit(long maxSize, long maxWeight) {
                return maxSize;
            }

            @Override
            public void onRemoval(long[] sizeAndWeight) {
                if (sizeAndWeight[0] > 0) sizeAndWeight[0]--;
            }
        };
    }

    /**
     * 按累计权重淘汰。需配置 maximumWeight + weigher。
     */
    public static Eviction weightBased() {
        return new Eviction() {
            @Override
            public boolean shouldEvict(long currentSize, long maxSize, long currentWeight, long maxWeight) {
                return maxWeight > 0 && currentWeight > maxWeight;
            }

            @Override
            public long total(long currentSize, long currentWeight) {
                return currentWeight;
            }

            @Override
            public long limit(long maxSize, long maxWeight) {
                return maxWeight;
            }

            @Override
            public void onRemoval(long[] sizeAndWeight) {
                if (sizeAndWeight[1] > 0) sizeAndWeight[1]--;
            }
        };
    }
}