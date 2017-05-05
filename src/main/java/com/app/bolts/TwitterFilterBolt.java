package com.app.bolts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;

/**
 * @author Mohammad ALSHAER <malshaer at LYON && Beirut>
 */
public class TwitterFilterBolt extends BaseBasicBolt {
    private static final Logger LOGGER = Logger.getLogger(TwitterFilterBolt.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("name", "username", "description", "location", "followers", "numberstatuses", "time", "tweets"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        LOGGER.debug("filttering incoming tweets");
        String json = input.getString(0);
        try {

            JsonNode root = mapper.readValue(json, JsonNode.class);
            long id;
            String name;
            String username;
            String description;
            String location;
            int followers;
            int numberstatuses;
            String time;
            String tweets;

            if (root.get("id") != null && root.get("text") != null) {
                id = root.get("id").longValue();
                name = root.get("user").get("name").textValue();
                username = root.get("user").get("screen_name").textValue();
                description = root.get("user").get("description").textValue();
                location = root.get("user").get("location").textValue();
                followers = root.get("user").get("followers_count").intValue();
                numberstatuses = root.get("user").get("statuses_count").intValue();
                time = root.get("created_at").textValue();
                tweets = root.get("text").textValue();
                collector.emit(new Values(name, username, description, location, followers, numberstatuses, time, tweets));
            } else
                LOGGER.debug("tweet id and/ or text was null");
        } catch (IOException ex) {
            LOGGER.error("IO error while filtering tweets", ex);
            LOGGER.trace(null, ex);
        }
    }
}
