package master;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by denis on 3/11/16.
 */
public class ConverterBolt extends BaseRichBolt {
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        AvailableCurrency.Currency currencyID = (AvailableCurrency.Currency) tuple.getValueByField(CurrencySpout.CURRENCYFIELDNAME);
        double valueField = (Double) tuple.getValueByField(CurrencySpout.CURRENCYFIELDVALUE);
        double rate = AvailableCurrency.getRate(currencyID);
        double eurovalue = valueField * rate;
        System.out.println("Eur: "+ eurovalue + ", " +currencyID + ": " + valueField);
        }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
