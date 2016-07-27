package kafka.sink.consume;

import java.util.Collections;
import java.util.List;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.sink.conf.KafkaConsumeConfig;
import kafka.sink.conf.ZookeeperConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class KafkaConnectService {

	private static final Logger _logger = LoggerFactory.getLogger(KafkaConnectService.class);
	
	private KafkaConsumeConfig kafkaConsumeConf;
	private ZookeeperConfig zkConf;
	
	private CuratorFramework curator;
	private SimpleConsumer simpleConsumer;
	
	private int partition = -1;
	private String kafkaClientId;
	private String[] kafkaBrokersArray;
	private String leaderBrokerHost;
	private int leaderBrokerPort;
	private String leaderBrokerAddress;
	
	public KafkaConnectService() {
	}

	public KafkaConnectService(int partition) {
		this.partition = partition;
	}
	
	public void init() throws Exception{
		if (partition < 0) {
			throw new IllegalArgumentException("Partition id is not be assigned.");
		}
		
		_logger.info("Initializing kafka client for topic={}, partition={}...", kafkaConsumeConf.getTopic(), partition);

		kafkaClientId = kafkaConsumeConf.getKafkaConsumerGroupName() + "-partition-" + partition;
		kafkaBrokersArray = kafkaConsumeConf.getKafkaBrokersList().trim().split(",");
		
	}
	
	public void connectToZookeeper() {
		try {
			curator = CuratorFrameworkFactory.newClient(kafkaConsumeConf.getKafkaZookeeperList(), zkConf.getZkSessionTimeout(), zkConf.getZkConnectionTimeout(), new RetryNTimes(zkConf.getZkCuratorRetryTimes(), zkConf.getZkCuratorRetryDelayMs()));
			curator.start();
			_logger.info("Kafka client [{}] connect to kafka zookeeper successfully.", kafkaClientId);
		} catch (Exception e) {
			_logger.error("Kafka client [{}] connect to kafka zookeeper failed.", kafkaClientId, e);
			throw new RuntimeException("Kafka client [" + kafkaClientId + "] connect to kafka zookeeper failed.", e);
		}
	}
	
	public PartitionMetadata findPartitionLeader() throws Exception {
		return null;
	}
	
	public PartitionMetadata findPartitionLeaderWithRetry() throws Exception {
		PartitionMetadata leaderPartitionMetaData = null ;
		int retryTimes = 0;
		do {
			for (int i = 0; i < kafkaBrokersArray.length; i++) {
				String brokerStr = kafkaBrokersArray[i];
				BrokerInfo brokerInfo = null;
				try {
					brokerInfo = new BrokerInfo(brokerStr.split(":"));
				} catch (Exception e) {
					_logger.error("Failed to find partition leader, broker info=[{}]. ", brokerStr);
					throw new RuntimeException("Failed to find partition leader, broker info=[" + brokerStr + "].", e);
				}
				leaderPartitionMetaData = findPartitionLeaderWithBroker(brokerInfo.ip, brokerInfo.port);
				// find leader
				if (leaderPartitionMetaData != null) {
					return leaderPartitionMetaData;
				}
			}
			retryTimes++;
			_logger.warn("Leader final attempt {} time, retry after sleep.", retryTimes);
			Thread.sleep(1000);
		} while (retryTimes <= kafkaConsumeConf.getLeaderFindRetryCount());
		
		return leaderPartitionMetaData;
	}
	
	private PartitionMetadata findPartitionLeaderWithBroker(String host, int port) {
		_logger.info("Looking for leader for partition {}, Kafka broker={}, port={}, topic={}.", partition, host, port, kafkaConsumeConf.getTopic());
		PartitionMetadata leaderPartitionMetadata = null;
		SimpleConsumer leaderFindConsumer = null;
		
		try {
			leaderFindConsumer = new SimpleConsumer(host, port, kafkaConsumeConf.getKafkaConsumerSocketTimeoutMs(), kafkaConsumeConf.getKafkaConsumerSocketBufferSize(), "leaderLookup");
			List<String> topics = Collections.singletonList(kafkaConsumeConf.getTopic());
			TopicMetadataRequest request = new TopicMetadataRequest(topics);
			TopicMetadataResponse topicMetadataResponse = leaderFindConsumer.send(request);

			List<TopicMetadata> metadatas = topicMetadataResponse.topicsMetadata();
			for (TopicMetadata metadata : metadatas) {
				for (PartitionMetadata part : metadata.partitionsMetadata()) {
					if (part.partitionId() == partition) {
						_logger.info("Find leader for partition {}, Kafka broker={}, port={}, topic={}, leader broker={}:{}.", partition, host, port, kafkaConsumeConf.getTopic(), part.leader().host(), part.leader().port() );
						leaderPartitionMetadata = part;
						break;
					}
				}
				if (leaderPartitionMetadata != null) {
					break;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (leaderFindConsumer != null) {
				leaderFindConsumer.close();
			}
		}
		
		return leaderPartitionMetadata;
	}
	
	static class BrokerInfo {
		String ip;
		int port;
		
		public BrokerInfo(String[] broker) {
			if (broker.length < 2) {
				throw new IllegalArgumentException("Config kafka brokers wrong, brokers info should be like localhost:9002");
			}
			this.ip = broker[0];
			this.port = Integer.parseInt(broker[1]);
		}
		
	}
}
