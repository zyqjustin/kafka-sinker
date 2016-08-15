package kafka.sink.consume;

import kafka.sink.conf.KafkaConsumeConfig;
import kafka.sink.conf.ZookeeperConfig;
import kafka.sink.util.KafkaConsumeTaskStatus;
import kafka.sink.util.RotateEnum;
import kafka.sink.util.WriteFileToEnum;
import kafka.sink.write.DefaultFileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerService implements Runnable {
	private static Logger _logger = LoggerFactory.getLogger(KafkaConsumerService.class);
	
	private KafkaClientService kafkaClient;
	private KafkaConsumeConfig kafkaConsumeConf;
	private ZookeeperConfig zkConf;
	private DefaultFileWriter fileWriter; // TODO choose file writer
	
	// Task status
	private KafkaConsumeTaskStatus consumeTaskStatus;
	
	public KafkaConsumerService(int partition, KafkaConsumeConfig kafkaConsumeConf, ZookeeperConfig zkConf) {
		super();
		this.kafkaClient = new KafkaClientService(partition, kafkaConsumeConf, zkConf);
		this.consumeTaskStatus = KafkaConsumeTaskStatus.Created;
	}

	@Override
	public void run() {
		try {
			kafkaClient.init();
			this.consumeTaskStatus = KafkaConsumeTaskStatus.Initialized;
		} catch (Exception e) {
			_logger.error("Init kafka client service failed, error messages: {}.", e.getMessage());
			throw new RuntimeException("Init kafka client service failed", e);
		}
		
		
	}

	// TODO create file writer, rotator, fileNameFormat
	private void createFileWriter() throws InstantiationException, IllegalAccessException {
		RotateEnum rotate = RotateEnum.getRotate(kafkaConsumeConf.getRotate());
		rotate.getRotateClass().newInstance(); // TODO

		WriteFileToEnum writeFileTo = WriteFileToEnum.getWriteFileTo(kafkaConsumeConf.getKafkaConsumerWriteTo());
		fileWriter = (DefaultFileWriter) writeFileTo.getWriteClass().newInstance(); // TODO
		
	}
	
}
