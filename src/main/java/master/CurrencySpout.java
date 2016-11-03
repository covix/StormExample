package master;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by denis on 3/11/16.
 */
public class CurrencySpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    public static final String CURRENCYFIELDNAME ="currencyID";
    public static final String CURRENCYFIELDVALUE ="value";
    public static final String CURRENCYOUSTREAM ="currencyStream";

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    private Values randomValue(){
        double value = Math.random()*1000;
        return new Values(AvailableCurrency.getRandomCurrency(), value);
    }

    public void nextTuple() {
        Values randomValue = this.randomValue();
        System.out.println("emitting " + randomValue);
        collector.emit(CURRENCYOUSTREAM, randomValue);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(CURRENCYOUSTREAM, new Fields(CURRENCYFIELDNAME, CURRENCYFIELDVALUE));
    }
}
