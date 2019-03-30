/*
   Author : Nikhila Chireddy
   Date : 10-24-2017
*/
package Twitter;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream twitterStream;
    PrintWriter _log;

    String consumerKey = "otcixfSHqptxYVgVuA4gl2y3y";
    String consumerSecret = "vWAzcLz700yGLIGdRh3qYtT0trB4tltol978VdXR4gD0z0W3Tm";
    String accessToken = "881222655000559616-lNAeNGg4lQ5Wq0IB0tvenOS8ULCPaMe";
    String accessTokenSecret = "qy6GKGLsIKwMUbJEJB640ma6Xye3URU8k6nvad24w8JY8";

    private String output;

    public TwitterSpout(String o) {
        output = o;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = collector;
        try {
            _log = new PrintWriter(new File(output + "/tweets.txt"));
        } catch (Exception e) {
            System.out.println("Error in writing to file");
            e.printStackTrace();
        }

        StatusListener listener = new StatusListener() {

            public void onStatus(Status status) {
                try {
                    queue.put(status);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            public void onTrackLimitationNotice(int i) {
            }

            public void onScrubGeo(long l, long l1) {
            }

            public void onException(Exception ex) {
            }

            public void onStallWarning(StallWarning arg0) {

            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);

        twitterStream = new TwitterStreamFactory(new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        twitterStream.setOAuthAccessToken(token);
        twitterStream.sample();

    }

    public void nextTuple() {
        Status ret = queue.poll();

        if (ret == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(ret));
            _log.write(ret.getText() + "\n");
            _log.flush();
        }
    }

    public void close() {
        twitterStream.shutdown();
    }

    public Map<String, java.lang.Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    public void ack(Object id) {
    }

    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweets"));
    }

}
