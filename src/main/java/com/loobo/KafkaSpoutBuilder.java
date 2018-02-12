package com.loobo;

import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.tuple.Fields;

public class KafkaSpoutBuilder {
    public static OpaqueTridentKafkaSpout builder() {

        TridentKafkaConfig spoutConf = new TridentKafkaConfig(new ZkHosts("localhost:2181"), "test");

        StringScheme scheme = new CustomStringScheme("log-event");
        spoutConf.scheme = new SchemeAsMultiScheme(scheme);

        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
        return spout;
    }
}
