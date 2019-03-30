package Twitter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import twitter4j.Status;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class Bolt_SaveTweets_File extends BaseRichBolt
{
    private FileWriter fileWriter;
    private BufferedWriter buffer;
    private SimpleDateFormat formatter;
    private String each_tweet_inFile;

    public void prepare(Map config, TopologyContext context, OutputCollector collector)
    {
        try {
            fileWriter = new FileWriter("Output/Tweets.txt",true);
            buffer = new BufferedWriter(fileWriter);
            formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple)
    {
        Date date = new Date();
        Status tweet = (Status) tuple.getValueByField("tweets");
        try {
            each_tweet_inFile = "----------------------------------------------------------------------\n\n"+
                    "date: "+formatter.format(date)+"\n"+
                    "tweet: \n"+tweet+"\n";
            buffer.write(each_tweet_inFile);
            buffer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
