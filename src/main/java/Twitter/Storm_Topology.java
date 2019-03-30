package Twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Storm_Topology
{
    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new Spout_TwitterStream_Tweets());
        builder.setBolt("twitter-bolt",new Bolt_SaveTweets_File()).shuffleGrouping("twitter-spout");



        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config,
                builder.createTopology());
        //Thread.sleep(10000);
//        cluster.shutdown();
    }
}