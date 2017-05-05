/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.app.bolts;

import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import joptsimple.internal.Strings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Mohammad ALSHAER <malshaer at LYON && Beirut>
 */
public class StanfordNERBolt extends BaseRichBolt {
    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(StanfordNERBolt.class);
    private static String DIR_PATH;
    private CRFClassifier<CoreLabel> _classifier;
    private OutputCollector _oc;

    /**
     * identify Name,organization location etc entities and return Map<List>
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        this._oc = oc;
        try {
            String env = "REALTIMECLUSTERING";
            DIR_PATH = System.getenv(env);
            if (DIR_PATH != null) {
                LOGGER.error(String.format("%s=%s%n", env, DIR_PATH));
            } else {
                LOGGER.error(String.format("%s is"
                        + " not assigned.%n", env));
            }
            String classifierPath = DIR_PATH + "bin//ner_extractor//english.muc.7class.distsim.crf.ser.gz";
            _classifier = CRFClassifier.getClassifierNoExceptions(classifierPath);
        } catch (Exception ex) {
            Logger.getLogger(StanfordNERBolt.class.getName()).log(Level.SEVERE, null, ex);
        }


    }

    @Override
    public void execute(Tuple input) {
        try {
            String record = input.getString(input.fieldIndex("record"));
            String[] record_splitted = record.split(",");
            String text = record_splitted[record_splitted.length - 1];
            LinkedHashMap<String, LinkedHashSet<String>> map = new <String, LinkedHashSet<String>>LinkedHashMap();
            List<List<CoreLabel>> out = _classifier.classify(text);
            for (List<CoreLabel> sentence : out) {
                String s = "";
                String prevLabel = null;
                for (CoreLabel word : sentence) {
                    String category = word.get(CoreAnnotations.AnswerAnnotation.class);
                    if (prevLabel == null || prevLabel.equals(category)) {

                        s = s + " " + word;
                        prevLabel = category;

                    } else {
                        if (!prevLabel.equals("O")) {
                            System.out.println(s.trim() + '/' + prevLabel + ' ');
                            //Add to map
                            if (map.containsKey(prevLabel)) {
                                // key is already their just insert in arraylist
                                map.get(prevLabel).add(s.trim());
                            } else {
                                LinkedHashSet<String> temp = new LinkedHashSet<String>();
                                temp.add(s.trim());
                                map.put(prevLabel, temp);
                            }
                        }
                        s = " " + word;
                        prevLabel = category;
                    }
                }
                if (!prevLabel.equals("O")) {
                    System.out.println(s + '/' + prevLabel + ' ');
                    //Add to map
                    if (map.containsKey(prevLabel)) {
                        // key is already their just insert in arraylist
                        map.get(prevLabel).add(s.trim());
                    } else {
                        LinkedHashSet<String> temp = new LinkedHashSet<String>();
                        temp.add(s.trim());
                        map.put(prevLabel, temp);
                    }
                }
            }

            StringBuilder sb = new StringBuilder();
            for (String key : map.keySet()) {
                LinkedHashSet<String> hashset = map.get(key);
                Iterator iter = hashset.iterator();
                switch (key) {
                    case "LOCATION":
                        while (iter.hasNext()) {
                            String str = "LOC_" + iter.next().toString();
                            sb.append(str + ",");
                        }

                        break;
                    case "ORGANIZATION":
                        while (iter.hasNext()) {
                            String str = "ORG_" + iter.next().toString();
                            sb.append(str + ",");
                        }
                        break;
                    case "PERSON":
                        while (iter.hasNext()) {
                            String str = "PER_" + iter.next().toString();
                            sb.append(str + ",");
                        }
                        break;
                    case "DATE":
                        while (iter.hasNext()) {
                            String str = "DT_" + iter.next().toString();
                            sb.append(str + ",");
                        }
                        break;
                    case "MONEY":
                        while (iter.hasNext()) {
                            String str = "MNY_" + iter.next().toString();
                            sb.append(str + ",");
                        }
                        break;
                }

            }

            String context = sb.toString();
            if (context.endsWith(",")) {
                context = context.substring(0, context.length() - 1);
            }
            record_splitted[record_splitted.length - 1] = context;
            record = Strings.join(record_splitted,",");
            record = record + "," + text;

            _oc.emit(new Values(record));

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
