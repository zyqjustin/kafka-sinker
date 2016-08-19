package kafka.sink.conf;

/**
 * TODO create a consumer.properties.template !!!!!
 * @author zhuyuqiang
 * @date 2016年8月10日 下午2:38:19
 * @version 1.0
 */
public class KafkaConsumeConfig {

	/*-----kafka basic conf-----*/
	// consumer topic
	@ConfigableField(name = "kafka.consumer.topic")
	private String topic;
	// kafka zookeeper's [ip:port] list
	@ConfigableField(name = "kafka.zookeeper.list")
	private String kafkaZookeeperList;
	// kafka brokers' [ip:post] list
	@ConfigableField(name = "kafka.broker.list")
	private String kafkaBrokersList;
	/*-----kafka consume conf-----*/
	// kafka consumer reinit time;
	@ConfigableField(name = "kafka.client.reinit.count", required = false, defaultValue = "5")
	private int kafkaReinitCount;
	// kafka consumer reinit sleep time (unit: ms)
	@ConfigableField(name = "kafka.client.reinit.sleep.ms", required = false, defaultValue = "2000")
	private int kafkaReinitSleepTimeMs;
	// kafka consumer consume start offset
	// values: CUSTOM/EARLIEST/LATEST/RESTART
	@ConfigableField(name = "kafka.consumer.startOffset.from", required = false, defaultValue = "RESTART")
	private String startOffsetFrom;
	// if startOffsetFrom = CUSTOM, this value has to be set as int value, which means consume start offset
	@ConfigableField(name = "kafka.consumer.startOffset", required = false, defaultValue = "0")
	private int startOffset;
	// kafka consume group
	@ConfigableField(name = "kafka.consumer.group.name", required = false, defaultValue = "kafka-sinker")
	private String kafkaConsumerGroupName;
	// SimpleConsumer socket buffersize
	// default: "31457280(bytes) = 10 * 1024 * 1024 * 3"
	@ConfigableField(name = "kafka.consumer.socket.buffer.bytes", required = false, defaultValue = "31457280")
	private int kafkaConsumerSocketBufferSize;
	// SimpleConsumer socket timeout (unit: ms)
	@ConfigableField(name = "kafka.consumer.socket.timeout.ms", required = false, defaultValue = "10000")
	private int kafkaConsumerSocketTimeoutMs;
	// fetch size
	// default: "31457280(bytes) = 10 * 1024 * 1024 * 3"
	@ConfigableField(name = "kafka.consumer.fetch.bytes", required = false, defaultValue = "31457280")
	private int kafkaFetchSize;
	// times of retry find partitions's leader
	@ConfigableField(name = "kafka.client.find.leader.retry.count", required = false, defaultValue = "10")
	private int leaderFindRetryCount;
	
	// write kafka messages to where.
	// default: hdfs, or is local
	@ConfigableField(name = "kafka.consumer.write.to", required = false, defaultValue = "hdfs")
	private String kafkaConsumerWriteTo;
	// write file rotate policy
	// values: size/time/both
	@ConfigableField(name = "kafka.consumer.write.rotate", required = true)
	private String rotate;
	@ConfigableField(name = "kafka.consumer.rotate.size.kb", required = false, defaultValue = "0")
	private int rotateSizeKb;
	@ConfigableField(name = "kafka.consumer.rotate.size.mb", required = false, defaultValue = "0")
	private int rotateSizeMb;
	@ConfigableField(name = "kafka.consumer.rotate.size.gb", required = false, defaultValue = "0")
	private int rotateSizeGb;
	@ConfigableField(name = "kafka.consumer.rotate.size.tb", required = false, defaultValue = "0")
	private int rotateSizeTb;
	@ConfigableField(name = "kafka.consumer.rotate.time.sec", required = false, defaultValue = "0")
	private int rotateTimeSec;
	@ConfigableField(name = "kafka.consumer.rotate.time.min", required = false, defaultValue = "0")
	private int rotateTimeMin;
	@ConfigableField(name = "kafka.consumer.rotate.time.hour", required = false, defaultValue = "0")
	private int rotateTimeHour;
	@ConfigableField(name = "kafka.consumer.rotate.time.day", required = false, defaultValue = "0")
	private int rotateTimeDay;
	
	// write file root path
	// write file dir like /log/kafkasinker/2016/08/12/21/05...
	@ConfigableField(name = "kafka.consumer.write.file.path", required = false, defaultValue = "/log/kafkasinker")
	private String writeFilePath;
	// write file name's prefix
	@ConfigableField(name = "kafka.consumer.write.file.prefix", required = false, defaultValue = "")
	private String writeFilePrefix;
	// write file name's extension
	@ConfigableField(name = "kafka.consumer.write.file.extension", required = false, defaultValue = ".txt")
	private String writeFileExtension;
	
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

	public String getKafkaConsumerWriteTo() {
		return kafkaConsumerWriteTo;
	}

	public void setKafkaConsumerWriteTo(String kafkaConsumerWriteTo) {
		this.kafkaConsumerWriteTo = kafkaConsumerWriteTo;
	}

	public String getRotate() {
		return rotate;
	}

	public void setRotate(String rotate) {
		this.rotate = rotate;
	}

	public String getWriteFilePath() {
		return writeFilePath;
	}

	public void setWriteFilePath(String writeFilePath) {
		this.writeFilePath = writeFilePath;
	}

	public String getWriteFilePrefix() {
		return writeFilePrefix;
	}

	public void setWriteFilePrefix(String writeFilePrefix) {
		this.writeFilePrefix = writeFilePrefix;
	}

	public String getWriteFileExtension() {
		return writeFileExtension;
	}

	public void setWriteFileExtension(String writeFileExtension) {
		this.writeFileExtension = writeFileExtension;
	}

	public int getRotateSizeKb() {
		return rotateSizeKb;
	}

	public void setRotateSizeKb(int rotateSizeKb) {
		this.rotateSizeKb = rotateSizeKb;
	}

	public int getRotateSizeMb() {
		return rotateSizeMb;
	}

	public void setRotateSizeMb(int rotateSizeMb) {
		this.rotateSizeMb = rotateSizeMb;
	}

	public int getRotateSizeGb() {
		return rotateSizeGb;
	}

	public void setRotateSizeGb(int rotateSizeGb) {
		this.rotateSizeGb = rotateSizeGb;
	}

	public int getRotateSizeTb() {
		return rotateSizeTb;
	}

	public void setRotateSizeTb(int rotateSizeTb) {
		this.rotateSizeTb = rotateSizeTb;
	}

	public int getRotateTimeSec() {
		return rotateTimeSec;
	}

	public void setRotateTimeSec(int rotateTimeSec) {
		this.rotateTimeSec = rotateTimeSec;
	}

	public int getRotateTimeMin() {
		return rotateTimeMin;
	}

	public void setRotateTimeMin(int rotateTimeMin) {
		this.rotateTimeMin = rotateTimeMin;
	}

	public int getRotateTimeHour() {
		return rotateTimeHour;
	}

	public void setRotateTimeHour(int rotateTimeHour) {
		this.rotateTimeHour = rotateTimeHour;
	}

	public int getRotateTimeDay() {
		return rotateTimeDay;
	}

	public void setRotateTimeDay(int rotateTimeDay) {
		this.rotateTimeDay = rotateTimeDay;
	}
	
}
