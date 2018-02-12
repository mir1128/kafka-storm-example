package com.loobo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class KafkaStormTopology {


    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        Stream kafkaStream = topology.newStream("str", KafkaSpoutBuilder.builder());
        kafkaStream.each(new Fields("str"), new PrintAssignment());
        return topology.build();
    }


    public static void main(String[] args) throws InterruptedException {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc", conf, buildTopology());
        Thread.sleep(20000);
        cluster.shutdown();
    }
}
