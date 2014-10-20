package com.wjw.grouping;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt implements IRichBolt{
	Map<String,Integer> counts = new HashMap<String,Integer>();
	OutputCollector collector = null;
	/*@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word","count"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		int count = 0;
		String word = input.getString(0);
		if(counts.get(word) == null) {
			counts.put(word, 1);
//			//count = 1;
		} else {
			count = counts.get(word);
			count++;
			counts.put(word, count);
		}
		collector.emit(new Values(word,count));
		
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}*/

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		int count = 0;
		String word = input.getString(0);
		if(counts.get(word) == null) {
			counts.put(word, 1);
			//count = 1;
		} else {
			count = counts.get(word);
			count++;
			counts.put(word, count);
		}
		collector.emit(new Values(word,count));
		collector.ack(input);
		
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
