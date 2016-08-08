package kafka.sink.conf;

public class KafkaConsumeConfig {

	/*-----kafka basic conf-----*/
	// consumer topic
	@ConfigableField(name="kafka.consumer.topic")
	private String topic;
	// kafka zookeeper's [ip:port] list
	private String kafkaZookeeperList;
	// kafka brokers' [ip:post] list
	private String kafkaBrokersList;
	/*-----kafka consume conf-----*/
	// kafka consumer reinit time;
	private int kafkaReinitCount;
	// kafka consumer reinit sleep time (unit: ms)
	private int kafkaReinitSleepTimeMs;
	// kafka consumer consume start offset
	// values: CUSTOM/EARLIEST/LATEST/RESTART
	private String startOffsetFrom;
	// if startOffsetFrom = CUSTOM, this value has to be set as int value, which means consume start offset 
	private int startOffset;
	// kafka consume group
	private String kafkaConsumerGroupName;
	// SimpleConsumer socket buffersize
	private int kafkaConsumerSocketBufferSize;
	// SimpleConsumer socket timeout (unit: ms)
	private int kafkaConsumerSocketTimeoutMs;
	// fetch size
	private int kafkaFetchSize;
	// times of retry find partitions's leader 
	private int leaderFindRetryCount;

	// setters and getters
	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getKafkaZookeeperList() {
		return kafkaZookeeperList;
	}

	public void setKafkaZookeeperList(String kafkaZookeeperList) {
		this.kafkaZookeeperList = kafkaZookeeperList;
	}

	public String getKafkaBrokersList() {
		return kafkaBrokersList;
	}

	public void setKafkaBrokersList(String kafkaBrokersList) {
		this.kafkaBrokersList = kafkaBrokersList;
	}

	public int getKafkaReinitCount() {
		return kafkaReinitCount;
	}

	public void setKafkaReinitCount(int kafkaReinitCount) {
		this.kafkaReinitCount = kafkaReinitCount;
	}

	public int getKafkaReinitSleepTimeMs() {
		return kafkaReinitSleepTimeMs;
	}

	public void setKafkaReinitSleepTimeMs(int kafkaReinitSleepTimeMs) {
		this.kafkaReinitSleepTimeMs = kafkaReinitSleepTimeMs;
	}

	public String getStartOffsetFrom() {
		return startOffsetFrom;
	}

	public void setStartOffsetFrom(String startOffsetFrom) {
		this.startOffsetFrom = startOffsetFrom;
	}

	public int getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(int startOffset) {
		this.startOffset = startOffset;
	}

	public String getKafkaConsumerGroupName() {
		return kafkaConsumerGroupName;
	}

	public void setKafkaConsumerGroupName(String kafkaConsumerGroupName) {
		this.kafkaConsumerGroupName = kafkaConsumerGroupName;
	}

	public int getKafkaConsumerSocketBufferSize() {
		return kafkaConsumerSocketBufferSize;
	}

	public void setKafkaConsumerSocketBufferSize(int kafkaConsumerSocketBufferSize) {
		this.kafkaConsumerSocketBufferSize = kafkaConsumerSocketBufferSize;
	}

	public int getKafkaConsumerSocketTimeoutMs() {
		return kafkaConsumerSocketTimeoutMs;
	}

	public void setKafkaConsumerSocketTimeoutMs(int kafkaConsumerSocketTimeoutMs) {
		this.kafkaConsumerSocketTimeoutMs = kafkaConsumerSocketTimeoutMs;
	}

	public int getKafkaFetchSize() {
		return kafkaFetchSize;
	}

	public void setKafkaFetchSize(int kafkaFetchSize) {
		this.kafkaFetchSize = kafkaFetchSize;
	}

	public int getLeaderFindRetryCount() {
		return leaderFindRetryCount;
	}

	public void setLeaderFindRetryCount(int leaderFindRetryCount) {
		this.leaderFindRetryCount = leaderFindRetryCount;
	}
	
}
