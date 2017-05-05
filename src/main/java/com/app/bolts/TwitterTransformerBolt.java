package com.app.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.apache.commons.lang.RandomStringUtils;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Mohammad ALSHAER <malshaer at LYON && Beirut>
 */
public class TwitterTransformerBolt extends BaseRichBolt {
    private static String DIR_PATH;
    private static final Logger LOGGER = Logger.getLogger(TwitterTransformerBolt.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
     }

    @Override
    public void execute(Tuple tuple) {
        // do the windowing computation
        String name = tuple.getStringByField("name");
        String username = tuple.getStringByField("username");
        String description = tuple.getStringByField("description");
        String location = tuple.getStringByField("location");
        int followers = tuple.getIntegerByField("followers");
        int numberstatuses = tuple.getIntegerByField("numberstatuses");
        String time = tuple.getStringByField("time");
        String tweets = tuple.getStringByField("tweets");
       // String record = String.format("%s,%s,%s,%s,%d,%d,%s,%s",name,username,description,location,followers,numberstatuses,time,tweets);
        String record = String.format("%s,%s,%d,%d,%s,%s",name,location,followers,numberstatuses,time,tweets);
        // emit the results
        collector.emit(new Values(record));

    }

 //  @Override
    public void execute(TupleWindow inputWindow) {
        LOGGER.debug("transforming incoming tweets into csv records");
        try {

            for (Tuple tuple : inputWindow.get()) {
                // do the windowing computation
                String name = tuple.getStringByField("name");
                String username = tuple.getStringByField("username");
                String description = tuple.getStringByField("description");
                String location = tuple.getStringByField("location");
                int followers = tuple.getIntegerByField("followers");
                int numberstatuses = tuple.getIntegerByField("numberstatuses");
                String time = tuple.getStringByField("time");
                String tweets = tuple.getStringByField("tweets");
                String record = String.format("%s,%s,%s,%s,%d,%d,%s,%s",name,username,description,location,followers,numberstatuses,time,tweets);
                // emit the results
                collector.emit(new Values(record));
            }
        } catch (Exception ex) {
            LOGGER.error("Error while transforming the tweets", ex);
            LOGGER.trace(null, ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));
    }
}
