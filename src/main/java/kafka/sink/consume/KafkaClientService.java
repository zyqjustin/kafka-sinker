package kafka.sink.consume;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.sink.conf.KafkaConsumeConfig;
import kafka.sink.conf.ZookeeperConfig;
import kafka.sink.exception.KafkaClientRecoverableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class KafkaClientService implements KafkaClient {

	private static final Logger _logger = LoggerFactory.getLogger(KafkaClientService.class);
	
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
	
	public KafkaClientService() {
	}

	public KafkaClientService(int partition) {
		this.partition = partition;
	}
	
	public void init() throws Exception{
		if (partition < 0) {
			throw new IllegalArgumentException("Partition id is not be assigned.");
		}
		
		_logger.info("Initializing kafka client for topic={}, partition={}...", kafkaConsumeConf.getTopic(), partition);

		kafkaClientId = kafkaConsumeConf.getKafkaConsumerGroupName() + "-partition-" + partition;
		kafkaBrokersArray = kafkaConsumeConf.getKafkaBrokersList().trim().split(",");
		
		connectToZookeeper();
		findPartitionLeader();
		initConsumer();
	}
	
	@Override
	public void connectToZookeeper() {
		try {
			curator = CuratorFrameworkFactory.newClient(
					kafkaConsumeConf.getKafkaZookeeperList(),
					zkConf.getZkSessionTimeout(),
					zkConf.getZkConnectionTimeout(),
					new RetryNTimes(zkConf.getZkCuratorRetryTimes(), zkConf
							.getZkCuratorRetryDelayMs()));
			curator.start();
			_logger.info("Kafka client [{}] connect to kafka zookeeper successfully.", kafkaClientId);
		} catch (Exception e) {
			_logger.error("Kafka client [{}] connect to kafka zookeeper failed.", kafkaClientId, e);
			throw new RuntimeException("Kafka client [" + kafkaClientId + "] connect to kafka zookeeper failed.", e);
		}
	}
	
	@Override
	public PartitionMetadata findPartitionLeader() throws Exception {
		_logger.info("Looking for leader for partition {}...", partition);
		PartitionMetadata leaderPartitionMetaData = findPartitionLeaderWithRetry();
		// find leader failed or leader is null
		if (leaderPartitionMetaData == null || leaderPartitionMetaData.leader() == null) {
			_logger.error(
					"Failed to find leader for topic=[{}], partition=[{}], kafka brokers list=[{}], partition metadata is null.",
					kafkaConsumeConf.getTopic(), partition, kafkaConsumeConf.getKafkaBrokersList());
			throw new RuntimeException(
					"Failed to find partition leader for topic=[" + kafkaConsumeConf.getTopic() + "], partition=["
							+ partition + "]. kafka brokers list=[" + kafkaConsumeConf.getKafkaBrokersList() + "], partition metadata is null.");
		}
		
		leaderBrokerHost = leaderPartitionMetaData.leader().host();
		leaderBrokerPort = leaderPartitionMetaData.leader().port();
		leaderBrokerAddress = leaderBrokerHost + ":" + leaderBrokerPort;
		_logger.info("Partition=[{}] find leader broker address=[{}]", partition, leaderBrokerAddress);
		
		return leaderPartitionMetaData;
	}
	
	@Override
	public void initConsumer() throws Exception {
		try {
			this.simpleConsumer = new SimpleConsumer(leaderBrokerHost, leaderBrokerPort, kafkaConsumeConf.getKafkaConsumerSocketTimeoutMs(), kafkaConsumeConf.getKafkaConsumerSocketBufferSize(), kafkaClientId);
			_logger.info("Initialized kafka consumer successfully for partition {}.", partition);
		} catch (Exception e) {
			_logger.error("Failed to initialize kafka consumer for partition " + partition, e);
			throw new RuntimeException("Failed to initialize kafka consumer for partition " + partition, e);
		}
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
				// find leader, get out of this loop
				if (leaderPartitionMetadata != null) {
					break;
				}
			}
		} catch (Exception e) {
			_logger.warn("Failed to find leader of parition {}, Kafka broker={}, port={}, topic={}. Error: {}", partition, host, port, kafkaConsumeConf.getTopic(), e.getMessage());
		} finally {
			if (leaderFindConsumer != null) {
				leaderFindConsumer.close();
			}
		}
		
		return leaderPartitionMetadata;
	}

	@Override
	public void close() {
		if (curator != null) {
			curator.close();
			_logger.info("Curator/Zookeeper connection closed...");
		}
	}

	@Override
	public void reInitKafka() throws Exception {
		for (int i = 0; i < kafkaConsumeConf.getKafkaReinitCount(); i++) {
			try {
				close();
				Thread.sleep(kafkaConsumeConf.getKafkaReinitSleepTimeMs());
				
				connectToZookeeper();
				findPartitionLeader();
				initConsumer();
				
				checkKafkaOffsets();
				_logger.info("Re-initializing kafka client for partition [{}] successfully.", partition);
				return;
			} catch (Exception e) {
				_logger.error("Re-initializing kafka client for partition [{}] after {} attempts failed. Error: {}", partition, i, e.getMessage());
				throw new KafkaClientRecoverableException("Re-initializing kafka client for partition " + partition + " after " + i + " attempts failed.", e);
			}
		}
	}

	@Override
	public void checkKafkaOffsets() throws Exception {
		try {
			long currentOffset = fetchCurrentOffsetFromKafka();
			long earliestOffset = getEarliestOffset();
			long latestOffset = getLastestOffset();
			_logger.info("Offset for partition=[{}]: currentOffset={}, earliestOffset={}, latestOffset={}", partition, currentOffset, earliestOffset, latestOffset);
		} catch (Exception e) {
			throw new RuntimeException("Failed to check kafka offset for partition " + partition, e);
		}
	}

	@Override
	public long computeInitialOffset() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long fetchCurrentOffsetFromKafka() throws KafkaClientRecoverableException {
		short versionId = 0;
		int correlationId = 0;
		
		try {
			TopicAndPartition myTopicAndPartition = new TopicAndPartition(kafkaConsumeConf.getTopic(), partition);
			List<TopicAndPartition> topicPartitionList = Collections.singletonList(myTopicAndPartition);
			OffsetFetchRequest offsetFetchRequest = new OffsetFetchRequest(kafkaClientId, topicPartitionList, versionId, correlationId, kafkaClientId);
			OffsetFetchResponse offsetFetchResponse = simpleConsumer.fetchOffsets(offsetFetchRequest);
			
			long currentOffset = offsetFetchResponse.offsets().get(myTopicAndPartition).offset();
			return currentOffset;
		} catch (Exception e) {
			_logger.error("Failed to fetch current offset for partition=[{}]. Error: {}.", partition, e.getMessage());
			throw new KafkaClientRecoverableException("Failed to fetch current offset for partition=" + partition, e);
		}
		
	}

	@Override
	public long getEarliestOffset() throws KafkaClientRecoverableException {
		return getOffset(kafkaConsumeConf.getTopic(), partition, kafka.api.OffsetRequest.EarliestTime(), kafkaClientId);
	}

	@Override
	public long getLastestOffset() throws KafkaClientRecoverableException {
		return getOffset(kafkaConsumeConf.getTopic(), partition, kafka.api.OffsetRequest.LatestTime(), kafkaClientId);
	}

	private long getOffset(String topic, int partition, long whichTime, String clientId) throws KafkaClientRecoverableException {
		try {
			TopicAndPartition topicAndPartition = new TopicAndPartition(kafkaConsumeConf.getTopic(), partition);
			Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
			
			OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
			OffsetResponse response = simpleConsumer.getOffsetsBefore(request);
			
			if (response.hasError()) {
				int errorCode = response.errorCode(kafkaConsumeConf.getTopic(), partition);
				_logger.error("Error fetching offset from kafka for topic=[{}], partition=[{}], error code=[{}].", topic, partition, errorCode);
				throw new KafkaClientRecoverableException("Error fetching offset from kafka for topic=[" + topic + "], partition=[" +
				partition + "], error code=[" + errorCode + "].");
			}
			
			long[] offsets = response.offsets(topic, partition);
			return offsets[0];
		} catch (Exception e) {
			throw new KafkaClientRecoverableException("Failed to fetch offset for partition " + partition, e);
		}
	}
	
	@Override
	public FetchResponse getMessagesFromKafka(long offset) throws KafkaClientRecoverableException {
		// TODO
		return null;
	}

	@Override
	public Long handleErrorFromFetchMessages(short errorCode, long offsetForThisRound) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void saveOffset(long offset, short errorCode) throws KafkaClientRecoverableException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
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
