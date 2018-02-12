package com.loobo;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

@Slf4j
public class MovingAverageFunction extends BaseFunction {

    private EWMA ewma;
    private TimeCustom emmitRatePer;

    public MovingAverageFunction(EWMA ewma, TimeCustom emmitRatePer) {
        this.ewma = ewma;
        this.emmitRatePer = emmitRatePer;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        ewma.mark(Long.parseLong(tuple.getStringByField("timestamp")));
        log.info("[MovingAverageFunction] rate : {}", ewma.getAverageRatePer(emmitRatePer));
        collector.emit(new Values(ewma.getAverageRatePer(emmitRatePer)));
    }
}
