package com.wjw.grouping;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentenceSplitBolt implements IRichBolt{
    public void test() {

    }
	OutputCollector collector = null;
	/*@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}
	//WorkerTopologyContext wtc = new WorkerTopologyContext();
//	public void execute(Tuple input, BasicOutputCollector collector) {
//		// TODO Auto-generated method stub
//		String str[] = input.getString(0).split(" ");
//		for(String word: str) {
//			collector.emit(new Values(word));
//		}
//
//		//collector.emit(new Values(input.getString(0)));
//
//	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}*/

//	@Override
//	public void prepare(Map stormConf, TopologyContext context,
//			OutputCollector collector) {
//		// TODO Auto-generated method stub
//		this.collector = collector;
//		
//	}

//	@Override
//	public void execute(Tuple input) {
//		// TODO Auto-generated method stub
//		String str[] = input.getString(0).split(" ");
//		for(String word: str) {
//
//
//			collector.emit(new Values(word));
//		}
//
//	}



	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub

		String str[] = input.getString(0).split(" ");
        System.out.println(input.getString(0));
		for(String word: str) {
			collector.emit(new Values(word));
			collector.ack(input);
		}
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
