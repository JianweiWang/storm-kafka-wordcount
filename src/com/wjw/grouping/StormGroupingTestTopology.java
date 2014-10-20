package com.wjw.grouping;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.Cluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class StormGroupingTestTopology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sentenceSpout", new SentenceSpout(),2);
		//builder.setBolt("sentenceSplitBolt",new SentenceSplitBolt(),3).setNumTasks(5).noneGrouping("sentenceSpout");
		//builder.setBolt("sentenceSplitBolt",new SentenceSplitBolt(),3).setNumTasks(5).allGrouping("sentenceSpout");

		//builder.setBolt("sentenceSplitBolt",new SentenceSplitBolt(),3).setNumTasks(5).directGrouping("sentenceSpout", "sentence");
		
		//builder.setBolt("sentenceSplitBolt",new SentenceSplitBolt(),3).setNumTasks(5).globalGrouping("sentenceSpout");
		builder.setBolt("sentenceSplitBolt",new SentenceSplitBolt(),2).setNumTasks(10).shuffleGrouping("sentenceSpout");
		//builder.setBolt("sentenceSplitBolt",new SentenceSplitBolt(),3).setNumTasks(5).localOrShuffleGrouping("sentenceSpout");
		builder.setBolt("wordCountBolt", new WordCountBolt(),2).setNumTasks(10).fieldsGrouping("sentenceSplitBolt",new Fields("word"));
		Config config = new Config();
		config.setNumAckers(3);
		config.setDebug(false);
		config.setStatsSampleRate(1);
		if(args != null && args.length >= 1) {
			config.setNumWorkers(3);
			//TopologySubmitter submitter = new TopologySubmitter();
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			config.setMaxTaskParallelism(6);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordcount", config, builder.createTopology());
			Utils.sleep(1000000);
			cluster.shutdown();
		}
	
	
	}
}
