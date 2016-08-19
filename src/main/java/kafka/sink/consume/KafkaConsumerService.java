package kafka.sink.consume;

import java.lang.reflect.Constructor;

import kafka.sink.conf.KafkaConsumeConfig;
import kafka.sink.conf.ZookeeperConfig;
import kafka.sink.rotate.DefaultTimeFileNameFormat;
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
	private void createFileWriter() throws Exception {
		Rotator rotator = createRotator();
		
		DefaultTimeFileNameFormat timeFileNameFormat = new DefaultTimeFileNameFormat();
		timeFileNameFormat.withClientId("")
				.withExtension(kafkaConsumeConf.getWriteFileExtension())
				.withPath(kafkaConsumeConf.getWriteFilePath())
				.withPrefix(kafkaConsumeConf.getWriteFilePrefix());

		WriteFileToEnum writeFileTo = WriteFileToEnum.getWriteFileTo(kafkaConsumeConf.getKafkaConsumerWriteTo());
		fileWriter = (DefaultFileWriter) writeFileTo.getWriteClass().newInstance(); // TODO
		
	}
	
	private Rotator createRotator() throws Exception {
		RotateEnum rotate = RotateEnum.getRotate(kafkaConsumeConf.getRotate());
		
		Class<?> rotateClass = rotate.getRotateClass();
		if (rotateClass == FileSizeRotator.class) {
			Constructor<FileSizeRotator> constructor = FileSizeRotator.class.getDeclaredConstructor(new Class[] {int.class, SizeUnits.class});
			FileSizeRotator fileSizeRotator = constructor.newInstance(new Object[] { getFileSizeCount(), getFileSizeUnit() });
			return fileSizeRotator;
		} else if (rotateClass == TimedRotator.class) {
			Constructor<TimedRotator> constructor = TimedRotator.class.getDeclaredConstructor(new Class[] { int.class, TimeUnits.class });
			TimedRotator timedRotator = constructor.newInstance(new Object[] { getTimeCount(), getTimeUnit() });
			return timedRotator;
		} else if (rotateClass == TimedAndFileSizeRotator.class) {
			return null;
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
