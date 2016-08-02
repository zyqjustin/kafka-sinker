package kafka.sink.consume;

import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.sink.exception.KafkaClientRecoverableException;

public interface KafkaClient {
	
	/**
	 * close zookeeper connector
	 */
	public void close();
	
	/**
	 * initialize kafka simple consumer
	 * @throws Exception
	 */
	public void initConsumer() throws Exception;
	
	/**
	 * re-init kafka connect
	 * @throws Exception
	 */
	public void reInitKafka() throws Exception;
	
	/**
	 * connect to zookeeper
	 */
	public void connectToZookeeper();
	
	/**
	 * find partition leader for topic, support retry find
	 * @return partition metadata
	 * @throws Exception 
	 */
	public PartitionMetadata findPartitionLeader() throws Exception;

	/**
	 * check kafka offset, including earliest offset, latest offset, current offset
	 * @throws Exception
	 */
	public void checkKafkaOffsets() throws Exception;
	
	/**
	 * compute initial offset, dependency on user set
	 * @return consumer initial offset
	 * @throws Exception
	 */
	public long computeInitialOffset() throws Exception;
	
	/**
	 * fetch current offset
	 * @return current offset
	 * @throws KafkaClientRecoverableException
	 */
	public long fetchCurrentOffsetFromKafka() throws KafkaClientRecoverableException;
	
	/**
	 * fetch earliest offset
	 * @return earliest offset
	 * @throws KafkaClientRecoverableException
	 */
	public long getEarliestOffset() throws KafkaClientRecoverableException;
	
	/**
	 * fetch latest offset
	 * @return latest offset
	 * @throws KafkaClientRecoverableException
	 */
	public long getLastestOffset() throws KafkaClientRecoverableException;
	
	/**
	 * fetch messages from kafka
	 * @param offset
	 * @return
	 * @throws KafkaClientRecoverableException
	 */
	public FetchResponse getMessagesFromKafka(long offset) throws KafkaClientRecoverableException;
	
	/**
	 * handle error when encounter fetch messages for this round
	 * @param errorCode
	 * @param offsetForThisRound
	 * @return
	 * @throws Exception
	 */
	public Long handleErrorFromFetchMessages(short errorCode, long offsetForThisRound) throws Exception;
	
	/**
	 * save offset after consume
	 * @param offset
	 * @param errorCode
	 * @throws KafkaClientRecoverableException
	 */
	public void saveOffset(long offset, short errorCode) throws KafkaClientRecoverableException;
	
	public int getPartition();
}
