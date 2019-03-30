/*
   Author : Nikhila Chireddy
   Date : 10-24-2017
*/
package Twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class TwitterTopology {
    public static double e;
    public static Double threshold = -1.0;

    public static void main(String[] args) throws Exception {

        Config conf = new Config();

        conf.setDebug(true);
        // conf.setNumWorkers(4);

        TopologyBuilder builder = new TopologyBuilder();
        e = Double.parseDouble(args[1]);
        if (args.length > 2) {
            threshold = Double.parseDouble(args[2]);
        }
        builder.setSpout("twitter-spout", new TwitterSpout(args[3]));
        builder.setBolt("Hashtag-bolt", new HashTagBolt(args[3])).shuffleGrouping("twitter-spout");

        LossyCounting lc1 = new LossyCounting(e, threshold);
        builder.setBolt("HashTag-Lossy", lc1).shuffleGrouping("Hashtag-bolt");
        builder.setBolt("Report-HashTag-Bolt", new OutputBolt(args[3])).globalGrouping("HashTag-Lossy");

//        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

         LocalCluster cluster = new LocalCluster();

         cluster.submitTopology("test", conf, builder.createTopology());

        Utils.sleep(10000);
    }
}
