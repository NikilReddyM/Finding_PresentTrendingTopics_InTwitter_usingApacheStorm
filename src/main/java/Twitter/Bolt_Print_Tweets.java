package Twitter;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import twitter4j.Status;

public class Bolt_Print_Tweets extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Status tweet = (Status) tuple.getValueByField("tweets");
        System.out.println("tweet:  "+tweet.getText());
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("call"));

    }
}