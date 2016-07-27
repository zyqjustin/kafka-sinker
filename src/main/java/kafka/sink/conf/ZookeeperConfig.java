package kafka.sink.conf;

public class ZookeeperConfig {

	// zookeeper session timeout (unit: ms)
	private int zkSessionTimeout;
	// zookeeper connection timeout (unit: ms)
	private int zkConnectionTimeout;
	// zookeeper retry times of create curator client
	private int zkCuratorRetryTimes;
	// zookeeper delay time of retry creating curator client
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
