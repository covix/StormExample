package master;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("currencySpout", new CurrencySpout());
        builder.setBolt("converterBolt", new ConverterBolt()).localOrShuffleGrouping("currencySpout", CurrencySpout.CURRENCYOUSTREAM);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("converterTopology", new Config(), builder.createTopology());

        Utils.sleep(10000);

        cluster.killTopology("converterTopology");

        cluster.shutdown();
    }
}
