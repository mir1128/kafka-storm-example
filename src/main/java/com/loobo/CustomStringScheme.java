package com.loobo;

import org.apache.storm.kafka.StringScheme;
import org.apache.storm.tuple.Fields;

public class CustomStringScheme extends StringScheme {
    private String fieldName;

    public CustomStringScheme(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(fieldName);
    }
}
