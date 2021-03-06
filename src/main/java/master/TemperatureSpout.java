package master;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by covix on 04/11/2016.
 */
class TemperatureSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    public static final String TEMPERATURE_STREAMNAME = "tempstream";
    public static final String ROOM_FIELDNAME = "roomID";
    public static final String TEMPERATURE_FIELDNAME = "temperature";

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        // CREATE EVENTS FOR 1000 ROOMS WITH TEMPERATURE BETWEEN 0 AND 199 C
        Values values = new Values("room" + new Random().nextInt(1000), new Random().nextInt(200));
        collector.emit(TEMPERATURE_STREAMNAME, values);
        System.out.println("Emitting: " + values);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TEMPERATURE_STREAMNAME, new Fields(ROOM_FIELDNAME, TEMPERATURE_FIELDNAME));
    }
}
