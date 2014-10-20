package com.wjw.grouping;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;

public class SentenceSpout extends BaseRichSpout{
	public SpoutOutputCollector _collector = null;
	Long msgId = new Long(0);
//	Long startTime = System.currentTimeMillis();
//	int timeIntervalFlag = 0;
//	long timeInterval[] = {300,480,600,900};
//	long sleepTime[] = {1,0,2};
//	long finishTime;
//	long time = 1;
    List<KafkaStream<byte[], byte[]>> streams = null;
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
        //"10.128.12.72:2181", "group-1", "page_visits"
		// TODO Auto-generated method stub
		_collector = collector;
        final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(createConsumerConfig("192.168.0.8:2181",  "group-1"));
        final String topic =  "page_visits";
        //ExecutorService executor;
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        streams = consumerMap.get(topic);
	}
    public void fun() {
        final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(createConsumerConfig("10.128.12.72:2181",  "group-1"));
        final String topic =  "page_visits";
        //ExecutorService executor;
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        streams = consumerMap.get(topic);
        nextTuple();
    }
    private static ConsumerConfig createConsumerConfig(String a_zookeeper,
                                                       String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "1000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	String[] sentences = {
			"the cow jumped over the moon", 
			"an apple a day keeps the doctor away",
	       "four score and seven years ago", 
	       "snow white and the seven dwarfs",
	       "i am at two with nature"
	};
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
//		finishTime = System.currentTimeMillis();
		
//		if(((finishTime - startTime)/1000)%timeInterval[timeIntervalFlag] == 0)
//			{
//				time = sleepTime[timeIntervalFlag];
//				timeIntervalFlag = (timeIntervalFlag+1)%timeInterval.length;
//			}
//		if(msgId % 500000 == 0) {
//			time = sleepTime [(int) ((msgId / 500000) % (sleepTime.length))];
//			//System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA sleep :" + time);
//		}
        String sentence = null;
        for(KafkaStream stream: streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while(it.hasNext()) {
               sentence = new String(it.next().message());
                System.out.println("=============================================" + sentence);
                break;
            }
        }


//        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
//        while (it.hasNext())
//            System.out.println("Thread " + m_threadNumber + ": "
//                    + new String(it.next().message()));

        /**
         *
          */
        //Utils.sleep(0);
		Random rand = new Random(System.currentTimeMillis());
		//String sentence = sentences[rand.nextInt(sentences.length)];
		//_collector.emit(new Values(sentence));
		_collector.emit(new Values(sentence),msgId);
		msgId++;
		
		//_collector.emitDirect(34, "sentence", new Values(sentence));
		//count++;
//		if(msgId == 100)
//			return;
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("messageId: " + msgId);
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("sentence"));
		//declarer.declareStream("sentence", true, new Fields("sentence"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
    public static void main(String[] args) {
        //SentenceSpout sentenceSpout = new SentenceSpout();
        //sentenceSpout.fun();
    }

}
