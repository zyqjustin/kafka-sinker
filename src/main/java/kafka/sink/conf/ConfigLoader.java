package kafka.sink.conf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * why not create a project for tool code
 * @author zhuyuqiang
 * @date 2016年8月3日 下午1:44:57
 * @version 1.0
 */
public class ConfigLoader {
	private static final Logger _logger = LoggerFactory.getLogger(ConfigLoader.class);

	private Properties conf;
	private KafkaConsumeConfig consumerConf;
	private ZookeeperConfig zkConf;

	public ConfigLoader(String path) {
		this.conf = new Properties();
		this.consumerConf = new KafkaConsumeConfig();
		this.zkConf = new ZookeeperConfig();
		
		try {
			conf.load(new FileInputStream(path));
		} catch (FileNotFoundException e) {
			_logger.error("Not found kafka-sinker config file, path is {}. Error Message: {}", path, e.getMessage());
			throw new IllegalArgumentException("Not found kafka-sinker config file, path is " + path, e);
		} catch (IOException e) {
			_logger.error("Failed to load kafka-sinker config file, path is {}. Error Message: {}", path, e.getMessage());
			throw new  IllegalArgumentException("Failed to load kafka-sinker config file, path is " + path, e);
		}
		
		try {
			assembleProperties(KafkaConsumeConfig.class);
			assembleProperties(ZookeeperConfig.class);
		} catch (Exception e) {
			_logger.error("Assemble properties for consumer failed,");
		}
	}

	private void assembleProperties(Class<?> clazz) throws Exception {
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			ConfigableField annotation = field.getAnnotation(ConfigableField.class);
			String propertyName = annotation.name();
			boolean required = annotation.required();
			String defaultValue = annotation.defaultValue();
			
			field.setAccessible(true);
			Class<?> fieldType = field.getType();
			String propertyValue = conf.getProperty(propertyName);
			if (required && propertyValue == null) {
				throw new IllegalArgumentException("Perperty " + propertyName + " was absent.");
			}
			if (!required && propertyValue == null) {
				setValue(consumerConf, fieldType, field, defaultValue);
			} else {
				setValue(consumerConf, fieldType, field, propertyValue);
			}
		}
	}
	
	private void setValue(Object obj, Class<?> fieldType, Field field, String value) throws Exception {
		if (fieldType == int.class) {
			field.set(obj, Integer.parseInt(value));
		} else if (fieldType == String.class) {
			field.set(obj, value);
		} else {
			throw new IllegalArgumentException("Unsupport class type " + fieldType + " for field value set.");
		}
	}
	
	public KafkaConsumeConfig getConsumerConf() {
		return consumerConf;
	}

	public ZookeeperConfig getZkConf() {
		return zkConf;
	}	
	
}
