package com.wf.option.pricing;

import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.gson.JsonObject;

public class OptionDataReaderBolt extends BaseBasicBolt{
	
	private Integer emitFrequency;
	
	public OptionDataReaderBolt(){
		emitFrequency = 5;
	}
	
	public OptionDataReaderBolt(Integer emitFrequency){
		this.emitFrequency = emitFrequency;
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
		return conf;
	}
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
			&& tuple.getSourceStreamId().equals(Constants.METRICS_TICK_STREAM_ID)){
			List<Object> data = tuple.getValues();
			JsonObject jsonObj = (JsonObject) data.get(0);
			collector.emit(new Values(jsonObj));
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jsonotpiondata"));
	}

}
