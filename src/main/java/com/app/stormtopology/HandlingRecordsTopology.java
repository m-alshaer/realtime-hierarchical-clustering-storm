/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.app.stormtopology;

import com.app.bolts.*;
import com.app.configuration.StormConfiguration;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

/**
 * @author Mohammad ALSHAER <malshaer at LYNGRO>
 */
public class HandlingRecordsTopology {
    private static final String WARNING_EVENT_THRESHOLD = "400";
    private static final String CRITICAL_EVENT_THRESHOLD = "100";
    private static final String CRITICAL_EVENT_MULTIPLIER = "1.5";

    private static final String KAFKA_TOPIC = StormConfiguration
            .getString("m.storm.kafka_topic");

    public static void main(String[] args) throws Exception {

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], createConfig(false),
                    createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("defense-analysis", createConfig(true),
                    createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }

    private static KafkaSpout buildKafkaSpout() {
        SpoutConfig cfg = new SpoutConfig(new ZkHosts(StormConfiguration.getString("m.storm.zkhosts")), KAFKA_TOPIC,
                "/kafka", "KafkaSpout");
        cfg.zkPort = 2181;
        cfg.zkServers = Arrays.asList("localhost");
        cfg.scheme = new SchemeAsMultiScheme(new StringScheme());
        cfg.startOffsetTime = -2;
        return new KafkaSpout(cfg);
    }

    private static StormTopology createTopology() {
        TopologyBuilder topology = new TopologyBuilder();

        topology.setSpout("kafka_spout", buildKafkaSpout(), 2);

        topology.setBolt("twitter_filter", new TwitterFilterBolt(), 2)
                .shuffleGrouping("kafka_spout");

        topology.setBolt("twitter_cleaner", new TwitterCleanerBolt(), 2)
                .shuffleGrouping("twitter_filter");

        topology.setBolt("twitter_transformer", new TwitterTransformerBolt(), 2)
                .shuffleGrouping("twitter_cleaner");

        topology.setBolt("stanford_nlp", new StanfordNERBolt(), 2)
                .shuffleGrouping("twitter_transformer");

        topology.setBolt("realtime-clustering", new HierarchicalClusteringBolt(), 2)
                .shuffleGrouping("stanford_nlp");

        topology.setBolt("twitter_analytics", new AnalyticalBolt(), 2)
                .shuffleGrouping("realtime-clustering");


        return topology.createTopology();
    }
    private static Config createConfig(boolean local) {
        int workers = 2;//Properties.getInt("rts.storm.workers");
        Config conf = new Config();
        conf.setDebug(true);
        if (local) {
            conf.setMaxTaskParallelism(workers);
        } else {
            conf.setNumWorkers(workers);
        }
        //    conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        //    conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        //    conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("127.0.0.1"));
        return conf;
    }
}
