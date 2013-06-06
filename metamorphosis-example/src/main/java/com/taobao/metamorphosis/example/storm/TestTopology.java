package com.taobao.metamorphosis.example.storm;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.storm.scheme.StringScheme;
import com.taobao.metamorphosis.storm.spout.MetaSpout;


public class TestTopology {
    public static class FailEveryOther extends BaseRichBolt {

        OutputCollector _collector;
        int i = 0;


        @Override
        public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
            this._collector = collector;
        }


        @Override
        public void execute(Tuple tuple) {
            this.i++;
            if (this.i % 2 == 0) {
                this._collector.fail(tuple);
            }
            else {
                this._collector.ack(tuple);
            }
        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }


    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",
            new MetaSpout(initMetaConfig(), new ConsumerConfig("storm-spout"), new StringScheme()), 10);
        builder.setBolt("bolt", new FailEveryOther()).shuffleGrouping("spout");

        Config conf = new Config();
        // Set the consume topic
        conf.put(MetaSpout.TOPIC, "neta-test");
        // Set the max buffer size in bytes to fetch messages.
        conf.put(MetaSpout.FETCH_MAX_SIZE, 1024 * 1024);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
    }
}
