package kafka.sink;

import kafka.sink.consume.ConsumerManagerService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class KafkaSinker {
	private static final Logger _logger = LoggerFactory.getLogger(KafkaSinker.class);
	
	public static void main(String[] args) throws Exception {
		String confPath = "";
        if (args != null && args.length == 1) {
            confPath = args[0];
        } else {
        	_logger.error("Invalid patameters.Args should be only one parameter, and ");
        }
		_logger.info("Now start KafkaSinker......");
		
		AbstractApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:application-context.xml");
		applicationContext.registerShutdownHook();
		applicationContext.getBean(ConsumerManagerService.class).processAll(confPath);
		
		_logger.info("KafkaSinker start OK....");
	}

}
