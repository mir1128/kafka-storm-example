package org.apache.storm.windowing;

import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.trident.windowing.strategy.BaseWindowStrategy;

public class CustomFieldWindowStrategy<T> extends BaseWindowStrategy<T> {

    public CustomFieldWindowStrategy(WindowConfig windowConfig) {
        super(windowConfig);
    }

    @Override
    public TriggerPolicy<T> getTriggerPolicy(TriggerHandler triggerHandler, EvictionPolicy<T> evictionPolicy) {
        return new TimeTriggerPolicy<>(windowConfig.getSlidingLength(), triggerHandler, evictionPolicy);
    }

    @Override
    public EvictionPolicy<T> getEvictionPolicy() {
        return new CustomFieldTimeEvictionPolicy<>(1, windowConfig.getWindowLength());
    }
}
