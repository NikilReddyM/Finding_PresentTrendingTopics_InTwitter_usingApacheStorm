import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Arrays;
import java.util.List;


public class Twitter4J_example
{
    public static void main(String args[]) throws TwitterException {
        /*
        String consumerKey = "otcixfSHqptxYVgVuA4gl2y3y";
        String consumerSecret = "vWAzcLz700yGLIGdRh3qYtT0trB4tltol978VdXR4gD0z0W3Tm";
        String accessToken = "881222655000559616-lNAeNGg4lQ5Wq0IB0tvenOS8ULCPaMe";
        String accessTokenSecret = "qy6GKGLsIKwMUbJEJB640ma6Xye3URU8k6nvad24w8JY8";
        */

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("otcixfSHqptxYVgVuA4gl2y3y")
                .setOAuthConsumerSecret("vWAzcLz700yGLIGdRh3qYtT0trB4tltol978VdXR4gD0z0W3Tm")
                .setOAuthAccessToken("881222655000559616-lNAeNGg4lQ5Wq0IB0tvenOS8ULCPaMe")
                .setOAuthAccessTokenSecret("qy6GKGLsIKwMUbJEJB640ma6Xye3URU8k6nvad24w8JY8");

        StatusListener listener = new StatusListener() {

            public void onStatus(Status status) {



                List<HashtagEntity> lhte = Arrays.asList(status.getHashtagEntities());

                if(lhte.size()>0)
                {
                    System.out.println("******************************************");
                    for(HashtagEntity hte: lhte)
                    {
                        System.out.println(hte.getText());
                    }
                    System.out.println();
                }


               // System.out.println(status.);


            }


            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }


            public void onTrackLimitationNotice(int i) {

            }


            public void onScrubGeo(long l, long l1) {

            }


            public void onStallWarning(StallWarning stallWarning) {

            }


            public void onException(Exception e) {

            }
        };

        TwitterStreamFactory tsf = new TwitterStreamFactory(cb.build());
        TwitterStream ts = tsf.getInstance();

        ts.addListener(listener);

        ts.sample();


    }
}
