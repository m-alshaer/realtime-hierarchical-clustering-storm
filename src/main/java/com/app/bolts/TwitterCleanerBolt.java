package com.app.bolts;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Mohammad ALSHAER <malshaer at LYON && Beirut>
 */
public class TwitterCleanerBolt extends BaseBasicBolt {
    private static final Logger LOGGER = Logger.getLogger(TwitterFilterBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String name;
        String username;
        String description;
        String location;
        int followers;
        int numberstatuses;
        String time;
        String tweets;
        DateFormat dfin = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
        DateFormat dfout = new SimpleDateFormat("MM/dd/yyyy HH:mm");
        try {
            name = tuple.getStringByField("name").replaceAll("(&amp;)|[;|,|\"|\n|\r]", " ");
            username = tuple.getStringByField("username").replaceAll("(&amp;)|[;|,|\"|\n|\r]", " ");
            if (tuple.getStringByField("description") == null || tuple.getStringByField("description").equals(""))
                description = "";
            else
                description = tuple.getStringByField("description").replaceAll("(&amp;)|[;|,|\"|\n|\r]", " ");
            if (tuple.getStringByField("location") == null || tuple.getStringByField("location").equals(""))
                location = "";
            else
                location = tuple.getStringByField("location").replaceAll("(&amp;)|[;|,|\"|\n|\r]", " ");
            followers = tuple.getIntegerByField("followers");
            numberstatuses = tuple.getIntegerByField("numberstatuses");
            time = tuple.getStringByField("time").replaceAll("[,|\"|\n|\r]", " ").replaceAll("(\\+0+ )", "");
            Date din = dfin.parse(time);
            time = dfout.format(din);
            tweets = tuple.getStringByField("tweets").replaceAll("(&amp;)|[;|,|\"|\n|\r]", " ");
            //if(tweets.contains("RT @") || tweets.contains("via @")) {
            collector.emit(new Values(name, username, description, location, followers, numberstatuses, time, tweets));
           // }
        } catch (Exception ex) {
            LOGGER.error("IO error while filtering tweets", ex);
            LOGGER.trace(null, ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("name", "username", "description", "location", "followers", "numberstatuses", "time", "tweets"));
    }
}
