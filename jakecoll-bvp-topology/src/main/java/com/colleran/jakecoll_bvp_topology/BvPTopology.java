package com.colleran.jakecoll_bvp_topology;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class BvPTopology {

	static class FilterBvPBolt extends BaseBasicBolt {
		Pattern matchupPattern;
		Pattern statsPattern;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			matchupPattern = Pattern.compile("<matchup>([^<]*)</matchup>");
			statsPattern = Pattern.compile("<stats>([^<]*)</stats>");
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String report = tuple.getString(0);
			Matcher matchupMatcher = matchupPattern.matcher(report);
			if(!matchupMatcher.find()) {
				return;
			}
			Matcher statsMatcher = statsPattern.matcher(report);
			if(!statsMatcher.find()) {
				return;
			}
			collector.emit(new Values(matchupMatcher.group(1), statsMatcher.group(1)));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("matchup", "stats"));
		}

	}

	static class ExtractStatsBolt extends BaseBasicBolt {

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String stats = input.getStringByField("stats");
			boolean PA = stats.contains("PA");
			boolean AB = stats.contains("AB");
			boolean K = stats.contains("K");
			boolean walk = stats.contains("walk");
			boolean IW = stats.contains("IW");
			boolean HBP = stats.contains("HBP");
			boolean h1B = stats.contains("h1B");
			boolean h2B = stats.contains("h2B");
			boolean h3B = stats.contains("h2B");
			boolean HR = stats.contains("HR");	
			collector.emit(new Values
					(input.getStringByField("matchup"),
							PA, AB, K, walk, IW, HBP, h1B, h2B, h3B, HR));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("matchup", "PA", "AB", "K", "walk", "IW", "HBP", "h1B", "h2B", "h3B", "HR"));
		}

	}

	static class UpdateCurrentWeatherBolt extends BaseBasicBolt {
		private org.apache.hadoop.conf.Configuration conf;
		private Connection hbaseConnection;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				conf = HBaseConfiguration.create();
			    conf.set("hbase.zookeeper.property.clientPort", "2181");
			    conf.set("hbase.zookeeper.quorum", StringUtils.join((List<String>)(stormConf.get("storm.zookeeper.servers")), ","));
			    String znParent = (String)stormConf.get("zookeeper.znode.parent");
			    if(znParent == null)
			    	znParent = new String("/hbase");
				conf.set("zookeeper.znode.parent", znParent);
				hbaseConnection = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		@Override
		public void cleanup() {
			try {
				hbaseConnection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			super.cleanup();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				
				Table table = hbaseConnection.getTable(TableName.valueOf("jakecoll_batter_v_pitcher_2015"));	
				Increment increment = new Increment(Bytes.toBytes(input.getStringByField("matchup")));
				
				if(input.getBooleanByField("PA")) {
					increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("PA"), 1);
				}
				if(input.getBooleanByField("AB")) {
					increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("AB"), 1);
				}
				if(input.getBooleanByField("K")) {
					increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("K"), 1);
				}
				if(input.getBooleanByField("walk"))  {
					increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("walk"), 1);
				}
				if(input.getBooleanByField("IW"))  {
					increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("IW"), 1);
				}
				if(input.getBooleanByField("HBP"))  {
					increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("HBP"), 1);
				}
				if(input.getBooleanByField("h1B"))  {
					increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("h1B"), 1);
				}
				if(input.getBooleanByField("h2B"))  {
					increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("h2B"), 1);
				}
				if(input.getBooleanByField("h3B"))  {
					increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("h3B"), 1);
				}
				if(input.getBooleanByField("HR"))  {
					increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("HR"), 1);
				}
				table.increment(increment);
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		Map stormConf = Utils.readStormConfig();
		String zookeepers = StringUtils.join((List<String>)(stormConf.get("storm.zookeeper.servers")), ",");
		System.out.println(zookeepers);
		ZkHosts zkHosts = new ZkHosts(zookeepers);
		
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "jakecoll-bvp-events", "/jakecoll-bvp-events","matchup");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		// kafkaConfig.zkServers = (List<String>)stormConf.get("storm.zookeeper.servers");
		kafkaConfig.zkRoot = "/jakecoll-bvp-events";
		// kafkaConfig.zkPort = 2181;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("raw-weather-events", kafkaSpout, 1);
		builder.setBolt("filter-airports", new FilterBvPBolt(), 1).shuffleGrouping("raw-weather-events");
		builder.setBolt("extract-weather", new ExtractStatsBolt(), 1).shuffleGrouping("filter-airports");
		builder.setBolt("update-current-weather", new UpdateCurrentWeatherBolt(), 1).fieldsGrouping("extract-weather", new Fields("matchup"));


		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 4);

		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
			LocalCluster cluster = new LocalCluster(zookeepers, 2181L);
			cluster.submitTopology("jakecoll-bvp-topology", conf, builder.createTopology());
		} 
	} 
}
