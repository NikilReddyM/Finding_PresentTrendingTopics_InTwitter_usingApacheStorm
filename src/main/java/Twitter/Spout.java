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

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Spout extends BaseRichSpout
{

    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream twitterStream;

    String consumerKey = "otcixfSHqptxYVgVuA4gl2y3y";
    String consumerSecret = "vWAzcLz700yGLIGdRh3qYtT0trB4tltol978VdXR4gD0z0W3Tm";
    String accessToken = "881222655000559616-lNAeNGg4lQ5Wq0IB0tvenOS8ULCPaMe";
    String accessTokenSecret = "qy6GKGLsIKwMUbJEJB640ma6Xye3URU8k6nvad24w8JY8";

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {


        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = spoutOutputCollector;


        StatusListener listener = new StatusListener() {

            public void onStatus(Status status) {
//                String data = status.getText();
//                if(data != null)
//                {
                    try {
                        queue.put(status);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
//                }
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

        /*TwitterStreamFactory tsf = new TwitterStreamFactory(cb.build());
        twitterStream = tsf.getInstance();

        twitterStream.addListener(listener);
        twitterStream.sample();*/

    }

    public void nextTuple() {
        Status ret = queue.poll();

        if (ret == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(ret));

        }

//        _collector.emit(new Values(ret));


    }

    public void close() {
        twitterStream.shutdown();
    }


    public void ack(Object id) {
    }

    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweets"));

    }
}