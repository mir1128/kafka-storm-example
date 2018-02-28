package org.apache.storm.windowing;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.windowing.config.BaseWindowConfig;
import org.apache.storm.trident.windowing.strategy.WindowStrategy;

public class CustomFieldSlidingWindow extends BaseWindowConfig {

    private int fieldIndex;

    protected CustomFieldSlidingWindow(int windowLength, int slideLength, int fieldIndex) {
        super(windowLength, slideLength);
        this.fieldIndex = fieldIndex;
    }

    @Override
    public <T> WindowStrategy<T> getWindowStrategy() {
        return new CustomFieldWindowStrategy<>(this);
    }

    public static CustomFieldSlidingWindow of(BaseWindowedBolt.Duration windowDuration,
                                              BaseWindowedBolt.Duration slidingDuration,
                                              int fieldIndex) {
        return new CustomFieldSlidingWindow(windowDuration.value, slidingDuration.value, fieldIndex);
    }
}
