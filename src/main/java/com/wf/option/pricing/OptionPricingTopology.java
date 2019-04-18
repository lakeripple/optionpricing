package com.wf.option.pricing;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class OptionPricingTopology {
	
		public static void main(String[] args) throws Exception{
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("spout", new OptionDataSpout(),5);
			builder.setBolt("optiondata", new OptionDataReaderBolt(),8).shuffleGrouping("spout");
			builder.setBolt("pricer", new OptionPricerBolt(),12).shuffleGrouping("optiondata");
			Config conf = new Config();
			conf.setDebug(false);
			
			if(args != null && args.length > 0){
				conf.setMaxTaskParallelism(1);
			
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("OptionPrierTopology", conf, builder.createTopology());
			}else{
				conf.setNumWorkers(2);
				StormSubmitter.submitTopology("OptionPrierTopology", conf, builder.createTopology());
			}
		}

}
