package kafka.sink.exception;

public class KafkaClientNotRecoverableException extends Exception {

	public KafkaClientNotRecoverableException() {
		super();
	}

	public KafkaClientNotRecoverableException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public KafkaClientNotRecoverableException(String message, Throwable cause) {
		super(message, cause);
	}

	public KafkaClientNotRecoverableException(String message) {
		super(message);
	}

	public KafkaClientNotRecoverableException(Throwable cause) {
		super(cause);
	}
	
}
