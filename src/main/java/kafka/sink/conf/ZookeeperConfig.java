package kafka.sink.conf;

public class ZookeeperConfig {

	// zookeeper session timeout (unit: ms)
	@ConfigableField(name = "zk.session.timeout.ms", required = false, defaultValue = "10000")
	private int zkSessionTimeout;
	// zookeeper connection timeout (unit: ms)
	@ConfigableField(name = "zk.connection.timeout.ms", required = false, defaultValue = "15000")
	private int zkConnectionTimeout;
	// zookeeper retry times of create curator client
	@ConfigableField(name = "zk.curator.retry.count", required = false, defaultValue = "10")
	private int zkCuratorRetryTimes;
	// zookeeper delay time of retry creating curator client
	@ConfigableField(name = "zk.curator.retry.delay.ms", required = false, defaultValue = "2000")
	private int zkCuratorRetryDelayMs;
	
	public int getZkSessionTimeout() {
		return zkSessionTimeout;
	}

	public void setZkSessionTimeout(int zkSessionTimeout) {
		this.zkSessionTimeout = zkSessionTimeout;
	}

	public int getZkConnectionTimeout() {
		return zkConnectionTimeout;
	}

	public void setZkConnectionTimeout(int zkConnectionTimeout) {
		this.zkConnectionTimeout = zkConnectionTimeout;
	}

	public int getZkCuratorRetryTimes() {
		return zkCuratorRetryTimes;
	}

	public void setZkCuratorRetryTimes(int zkCuratorRetryTimes) {
		this.zkCuratorRetryTimes = zkCuratorRetryTimes;
	}

	public int getZkCuratorRetryDelayMs() {
		return zkCuratorRetryDelayMs;
	}

	public void setZkCuratorRetryDelayMs(int zkCuratorRetryDelayMs) {
		this.zkCuratorRetryDelayMs = zkCuratorRetryDelayMs;
	}
	
}
