package kafka.sink.util;

public enum KafkaConsumeTaskStatus {

	Created,
	
	Initialized,
	
	Started,
	
	Progressing,
	
	Hanging,
	
	Stopped,
	
	Cancelled,
	
	Failed;
}
