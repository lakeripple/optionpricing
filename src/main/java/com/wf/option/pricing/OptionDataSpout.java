package com.wf.option.pricing;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class OptionDataSpout extends BaseRichSpout{	
	SpoutOutputCollector _collector;
	Double underlyingTickPrice;
	public void nextTuple() {
		underlyingTickPrice = 202.0;
		URL url = null;
		HttpURLConnection conn = null;
		String inline = "";
		String strURL = "http://localhost:8080/refdata/option";
		JsonObject jObj = null;
		try{
			url = new URL(strURL);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.connect();
			
			InputStream in = new BufferedInputStream(conn.getInputStream());
			StringBuilder sb = new StringBuilder();
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
			while((line = reader.readLine()) != null){
				sb.append(line);
			}
			JsonParser parser = new JsonParser();
			Object obj = parser.parse(sb.toString());
			JsonArray jarray = (JsonArray)obj;
			for(int i=0; i < jarray.size(); i++){
				jObj = jarray.get(i).getAsJsonObject();
				_collector.emit(new Values(jObj,underlyingTickPrice));
			}
		}catch(MalformedURLException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	

	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("optiondata","underlyingPrice"));
	}

}
