/*
   Author : Nikhila Chireddy
   Date : 10-24-2017
*/
package Twitter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map;

public class HashTagBolt extends BaseRichBolt {

    private OutputCollector collector;
    // private List<Object> tweets = null;
    private FileWriter fileWriter;
    private BufferedWriter bw;
    private String output;

    public HashTagBolt(String o) {
        output = o;
    }

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        // this.tweets = new ArrayList<Object>();

        try {
            fileWriter = new FileWriter(output + "/HashTagLog.txt", true);
            bw = new BufferedWriter(fileWriter);
        } catch (Exception e) {
            System.out.println("UNABLE TO WRITE FILE :: 1 ");
            e.printStackTrace();
        }

    }

    public void execute(Tuple tuple) {
        Status tweetsFromBolt = (Status) tuple.getValueByField("tweets");

        collector.ack(tuple);

        // Status tweetsFromBolt = tweets_from_bolt;

        for (HashtagEntity ht : tweetsFromBolt.getHashtagEntities()) {

            String hashTag = ht.getText().toLowerCase();

            if (!hashTag.isEmpty()) {
                try {
                    fileWriter = new FileWriter(output + "/HashTagLog.txt", true);
                    bw = new BufferedWriter(fileWriter);
                    bw.write(hashTag + "\n");
                    bw.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                this.collector.emit(new Values(hashTag));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("hashTag"));

    }

}
