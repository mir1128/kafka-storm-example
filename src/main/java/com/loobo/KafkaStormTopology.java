package com.loobo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.windowing.CustomFieldSlidingWindow;

public class KafkaStormTopology {

    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        Stream kafkaStream = topology.newStream("kafka-spout", KafkaSpoutBuilder.builder());

        Fields jsonFields = new Fields("level", "timestamp", "data","message");

        WindowsStoreFactory windowsStore = new InMemoryWindowsStoreFactory();

        CustomFieldSlidingWindow windowConfig = CustomFieldSlidingWindow.of(BaseWindowedBolt.Duration.of(20000),
                        BaseWindowedBolt.Duration.of(500),
                        1);

        kafkaStream.each(new Fields("log-event"), new JsonProjectFunction(jsonFields), jsonFields)
                .window(windowConfig, windowsStore, jsonFields, new CountAsAggregator(), new Fields("count"))
                .each(new Fields("count"), new PrintAssignment());

        return topology.build();
    }

    public static void main(String[] args) throws InterruptedException {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        conf.put(Config.TOPOLOGY_TRIDENT_WINDOWING_INMEMORY_CACHE_LIMIT, 2000000);

        cluster.submitTopology("cdc", conf, buildTopology());
        Thread.sleep(200000);
        cluster.shutdown();
    }
}
