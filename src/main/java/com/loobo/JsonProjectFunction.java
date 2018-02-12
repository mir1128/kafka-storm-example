package com.loobo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonProjectFunction extends BaseFunction {

    private Fields fields;

    public JsonProjectFunction(Fields fields) {
        this.fields = fields;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        ObjectMapper mapper = new ObjectMapper();
        String field = tuple.getStringByField("log-event");

        Map<String, String> logContent = new HashMap<>();
        try {
            logContent = mapper.readValue(field, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Values values = new Values();
        for (int i = 0; i < fields.size(); ++i) {
            values.add(logContent.get(fields.get(i)));
        }
        collector.emit(values);
    }
}

