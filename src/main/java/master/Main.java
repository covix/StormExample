package master;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by covix on 04/11/2016.
 */
public class Main {
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tempSpout", new TempSpout());
        builder.setBolt("filterBolt", new FilterBolt(TempSpout.ROOM_TEMPERATURE, 100, "A1"))
                .localOrShuffleGrouping("tempSpout", TempSpout.ROOM_STREAM);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("roomTopology", new Config(), builder.createTopology());

        Utils.sleep(10000);

        cluster.killTopology("roomTopology");

        cluster.shutdown();
    }
}
