package kafka.sink.consume;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.javaapi.FetchResponse;
import kafka.message.MessageAndOffset;
import kafka.sink.conf.KafkaConsumeConfig;
import kafka.sink.conf.ZookeeperConfig;
import kafka.sink.exception.KafkaClientRecoverableException;
import kafka.sink.rotate.DefaultTimeFileNameFormat;
import kafka.sink.rotate.FileNameFormat;
import kafka.sink.rotate.FileSizeRotator;
import kafka.sink.rotate.FileSizeRotator.SizeUnits;
import kafka.sink.rotate.Rotator;
import kafka.sink.rotate.TimedAndFileSizeRotator;
import kafka.sink.rotate.TimedRotator;
import kafka.sink.rotate.TimedRotator.TimeUnits;
import kafka.sink.util.KafkaConsumeTaskStatus;
import kafka.sink.util.RotateEnum;
import kafka.sink.util.WriteFileToEnum;
import kafka.sink.write.DefaultFileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerService implements Runnable {
	private static Logger _logger = LoggerFactory.getLogger(KafkaConsumerService.class);
	
	private final static int ERROR_RETRY_COUNT = 100;
	
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
			createFileWriter();
			this.consumeTaskStatus = KafkaConsumeTaskStatus.Initialized;
		} catch (Exception e) {
			_logger.error("Init kafka consume service failed, error messages: {}.", e.getMessage());
			throw new RuntimeException("Init kafka consume service failed", e);
		}
		
		long lastOffset = 0L;
		try {
			lastOffset = kafkaClient.computeInitialOffset();
		} catch (Exception e) {
			_logger.error("Compute topic partition [{}] initial offset failed.", kafkaClient.getPartition());
			throw new RuntimeException("Init kafka consume partition " + kafkaClient.getPartition() + " initial offset failed.");
		}
		
		Rotator rotator = fileWriter.getRotator();
		Queue<String> messages = new ConcurrentLinkedQueue<String>();
		long lastTime = 0L;
		int errorCount = 0;
		
		// start task
		this.consumeTaskStatus = KafkaConsumeTaskStatus.Started;
		while (true) {
			FetchResponse fetchResponse = kafkaClient.getMessagesFromKafka(lastOffset);
			if (fetchResponse.hasError()) {
				errorCount++;
				short errorCode = fetchResponse.errorCode(kafkaConsumeConf.getTopic(), kafkaClient.getPartition());
				if (errorCount > ERROR_RETRY_COUNT) break;
				try {
					kafkaClient.handleErrorFromFetchMessages(errorCode, lastOffset);
					Thread.sleep(5000);
				} catch (KafkaClientRecoverableException e) {
					continue;
				} catch (IllegalArgumentException e) {
					this.consumeTaskStatus = KafkaConsumeTaskStatus.Failed;
					throw new IllegalArgumentException(e);
				} catch (InterruptedException e) {
					continue;
				}
			}
			
			// reset
			errorCount = 0;
			
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(kafkaConsumeConf.getTopic(), kafkaClient.getPartition())) {
				long currentOffset = messageAndOffset.offset();
				if (lastOffset > currentOffset) {
					_logger.warn("Fetch expired offset: [{}], expecting offset: [{}].", currentOffset, lastOffset);
					continue;
				}
				
				lastOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();
				int bytesNum = payload.limit();
				byte[] bytes = new byte[bytesNum];
				payload.get(bytes);
				String mes = null;
				try {
					mes = new String(bytes, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					_logger.warn("unspport encoding, error: {}", e.getMessage());
				}
				
				boolean isInQueue = messages.offer(mes);
				long currentTime = System.currentTimeMillis() / 1000;
				if (0 == lastTime || lastTime + 10 <= currentTime || !isInQueue) {
					// if true, should rotate file
					if (rotator.mark(mes, lastOffset)) { // TODO should add every message!!!!!
						try {
							fileWriter.rotateWriteFile();
						} catch (IOException e) {
							_logger.error("Rotate write file failed, file: [{}], error messagses: {}", fileWriter.getFilePath());
							continue;
						}
						
					}
				}
			}
		}
		
		stopKafkaClient();
	}

	private void stopKafkaClient() {
		if (kafkaClient != null) {
			kafkaClient.close();
		}
	}
	
	// TODO create file writer, rotator, fileNameFormat
	private void createFileWriter() throws Exception {
		Rotator rotator = createRotator();
		
		DefaultTimeFileNameFormat timeFileNameFormat = new DefaultTimeFileNameFormat();
		timeFileNameFormat.withClientId("")
				.withExtension(kafkaConsumeConf.getWriteFileExtension())
				.withPath(kafkaConsumeConf.getWriteFilePath())
				.withPrefix(kafkaConsumeConf.getWriteFilePrefix());

		WriteFileToEnum writeFileTo = WriteFileToEnum.getWriteFileTo(kafkaConsumeConf.getKafkaConsumerWriteTo());
		Constructor<?> writeFileConstructor = writeFileTo.getWriteClass().getDeclaredConstructor(new Class[]{ FileNameFormat.class, Rotator.class });
		fileWriter = (DefaultFileWriter)writeFileConstructor.newInstance(new Object[]{ timeFileNameFormat, rotator });
	}
	
	private Rotator createRotator() throws Exception {
		RotateEnum rotate = RotateEnum.getRotate(kafkaConsumeConf.getRotate());
		
		Class<?> rotateClass = rotate.getRotateClass();
		if (rotateClass == FileSizeRotator.class) {
			return new FileSizeRotator(getFileSizeCount(), getFileSizeUnit());
		} else if (rotateClass == TimedRotator.class) {
			return new TimedRotator(getTimeCount(), getTimeUnit());
		} else if (rotateClass == TimedAndFileSizeRotator.class) {
			FileSizeRotator fileSizeRotator = new FileSizeRotator(getFileSizeCount(), getFileSizeUnit());
			TimedRotator timedRotator = new TimedRotator(getTimeCount(), getTimeUnit());
			return new TimedAndFileSizeRotator(timedRotator, fileSizeRotator);
		} else {
			throw new IllegalArgumentException("Init rotator failed, could find appropriate rotator policy...");
		}
	}
	
	private int getFileSizeCount() {
		int size = kafkaConsumeConf.getRotateSizeKb() + kafkaConsumeConf.getRotateSizeMb() + kafkaConsumeConf.getRotateSizeGb() + kafkaConsumeConf.getRotateSizeTb();
		return size > 0 ? size : 100;
	}
	
	private SizeUnits getFileSizeUnit() {
		if (kafkaConsumeConf.getRotateSizeKb() > 0) {
			return SizeUnits.KB;
		} else if (kafkaConsumeConf.getRotateSizeMb() > 0) {
			return SizeUnits.MB;
		} else if (kafkaConsumeConf.getRotateSizeGb() > 0) {
			return SizeUnits.GB;
		} else if (kafkaConsumeConf.getRotateSizeTb() > 0) {
			return SizeUnits.TB;
		} else {
			return SizeUnits.MB;
		}
	}
	
	private int getTimeCount() {
		int count = kafkaConsumeConf.getRotateTimeSec() + kafkaConsumeConf.getRotateTimeMin() + kafkaConsumeConf.getRotateTimeHour() + kafkaConsumeConf.getRotateTimeDay();
		return count > 0 ? count : 10;
	}

	private TimeUnits getTimeUnit() {
		if (kafkaConsumeConf.getRotateTimeSec() > 0) {
			return TimeUnits.SECONDS;
		} else if (kafkaConsumeConf.getRotateTimeMin() > 0) {
			return TimeUnits.MINUTES;
		} else if (kafkaConsumeConf.getRotateTimeHour() > 0) {
			return TimeUnits.HOURS;
		} else if (kafkaConsumeConf.getRotateTimeDay() > 0) {
			return TimeUnits.DAYS;
		} else {
			return TimeUnits.MINUTES;
		}
	}
}
