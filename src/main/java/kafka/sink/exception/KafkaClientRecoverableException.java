package kafka.sink.exception;

public class KafkaClientRecoverableException extends RuntimeException {

	public KafkaClientRecoverableException() {
		super();
	}

	public KafkaClientRecoverableException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public KafkaClientRecoverableException(String message, Throwable cause) {
		super(message, cause);
	}

	public KafkaClientRecoverableException(String message) {
		super(message);
	}

	public KafkaClientRecoverableException(Throwable cause) {
		super(cause);
	}
	
}
