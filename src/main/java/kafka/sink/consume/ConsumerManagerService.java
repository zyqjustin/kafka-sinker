package kafka.sink.consume;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import kafka.sink.conf.ConfigLoader;
import kafka.sink.conf.KafkaConsumeConfig;
import kafka.sink.conf.ZookeeperConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

@Service
public class ConsumerManagerService {
	private static Logger _logger = LoggerFactory.getLogger(ConsumerManagerService.class);
	
	private static final String KAFKA_CONSUMER_POOL_NAME_FORMAT = "kafka-sinker-thread-%d";

	@Autowired
	private ApplicationContext applicationContext;
	@Autowired
	private ConfigLoader confLoader;
	private KafkaConsumeConfig kafkaConsumeConf;
	private ZookeeperConfig zkConf;
	
	private ConcurrentHashMap<Integer, KafkaConsumerService> consumerTasks;
	private ExecutorService executorService;
	
	public void processAll(String path) throws Exception {
		// load consumer.properties
		confLoader.load(path);
		kafkaConsumeConf = confLoader.getConsumerConf();
		zkConf = confLoader.getZkConf();
		
		ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(KAFKA_CONSUMER_POOL_NAME_FORMAT).build();
		executorService = Executors.newCachedThreadPool(threadFactory);
		// TODO need ?
		consumerTasks = new ConcurrentHashMap<Integer, KafkaConsumerService>();
		
		
		// TODO create partition consumer and start
		// executorService.execute(new Thread(new KafkaConsumerService()));
	}
	
}
