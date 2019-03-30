package Twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Arrays;

public class Topology
{
    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new Spout());
        builder.setBolt("twitter-bolt",new Bolt()).shuffleGrouping("twitter-spout");



        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config,
                builder.createTopology());
        //Thread.sleep(10000);
//        cluster.shutdown();
    }
}