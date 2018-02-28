package org.apache.storm.windowing;


import lombok.extern.slf4j.Slf4j;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.TridentBatchTuple;

import java.lang.reflect.Field;

@Slf4j
public class CustomFieldTimeEvictionPolicy<T> implements EvictionPolicy<T> {

    private int fieldIndex;
    private int windowLength;

    protected Long prevReferenceTime;

    public CustomFieldTimeEvictionPolicy(int fieldIndex, int windowLength) {
        this.fieldIndex = fieldIndex;
        this.windowLength = windowLength;
    }

    @Override
    public Action evict(Event<T> event) {
        Long timestamp = extractTimestamp(event);

        if (prevReferenceTime == null) {
            prevReferenceTime = timestamp - (windowLength / 2);
        }

        if (timestamp == null) {
            return Action.KEEP;
        }

        long diff = timestamp - prevReferenceTime;
        if (diff >= windowLength) {
            return Action.EXPIRE;
        } else if (diff < 0) { // do not process events beyond current ts
            return Action.KEEP;
        }
        return Action.PROCESS;
    }

    @Override
    public void track(Event<T> event) {
    }

    @Override
    public void setContext(EvictionContext context) {
        if (prevReferenceTime != null) {
            prevReferenceTime += context.getSlidingInterval();
            log.info("[set context] prevReferenceTime is {}", prevReferenceTime);
        }
    }


    private Long extractTimestamp(Event<T> event) {
        Long timestamp = null;

        T t = event.get();
        if (t instanceof TridentBatchTuple == false) {
            throw new RuntimeException();
        }

        TridentBatchTuple batchTuple = (TridentBatchTuple) t;
        try {
            Field f = batchTuple.getClass().getDeclaredField("tridentTuple");
            f.setAccessible(true);
            TridentTuple tridentTuple = (TridentTuple) f.get(batchTuple);
            if (tridentTuple != null) {
                timestamp = Long.parseLong((String) tridentTuple.get(fieldIndex));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return timestamp;
    }
}
