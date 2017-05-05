package com.app.bolts;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Mohammad ALSHAER <malshaer at LYON && Beirut>
 */
public class AnalyticalBolt extends BaseRichBolt {
    private static final Logger LOGGER = Logger.getLogger(AnalyticalBolt.class);
    private static String DIR_PATH;
    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("finished"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        String env = "REALTIMECLUSTERING";
        DIR_PATH = System.getenv(env);
        if (DIR_PATH != null) {
            LOGGER.error(String.format("%s=%s%n", env, DIR_PATH));
        } else {
            LOGGER.error(String.format("%s is"
                    + " not assigned.%n", env));
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String clusters = tuple.getStringByField("clusters");
        String processing_filepath = DIR_PATH + "output/clusters-x";//+ RandomStringUtils.randomAlphanumeric(8)+".txt";

        try {
            try (FileWriter fw = new FileWriter(processing_filepath,true)) {
                fw.write(clusters);
                fw.write("\n\t\t---------\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        collector.emit(new Values(true));
    }
}
