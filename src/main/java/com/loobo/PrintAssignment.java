package com.loobo;

import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;

public class PrintAssignment implements Filter {
    public boolean isKeep(TridentTuple tuple) {

        System.out.println("[print assignment] tuple is" + tuple);
        return false;
    }

    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    public void cleanup() {

    }
}
