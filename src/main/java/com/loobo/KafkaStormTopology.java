package com.loobo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class KafkaStormTopology {

    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        Stream kafkaStream = topology.newStream("kafka-spout", KafkaSpoutBuilder.builder());

        EWMA ewma = EWMA.builder().alphaWindow(TimeCustom.MINUTES.getMillis() * 1).alpha(EWMA.ONE_MINUTE_ALPHA).build();

        Fields jsonFields = new Fields("level", "timestamp", "message");
        kafkaStream.each(new Fields("log-event"), new JsonProjectFunction(jsonFields), jsonFields)
                .each(new Fields("timestamp"), new MovingAverageFunction(ewma, TimeCustom.MINUTES), new Fields("average"))
                .each(new Fields("average"), new PrintAssignment());

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
