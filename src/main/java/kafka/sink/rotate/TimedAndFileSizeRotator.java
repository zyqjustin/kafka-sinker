package kafka.sink.rotate;

public class TimedAndFileSizeRotator implements Rotator {

	private TimedRotator timedRotator;
	private FileSizeRotator fileSizeRotator;
	
	public TimedAndFileSizeRotator(TimedRotator timedRotator, FileSizeRotator fileSizeRotator) {
		this.timedRotator = timedRotator;
		this.fileSizeRotator = fileSizeRotator;
	}

	@Override
	public boolean mark(String mes, long offset) {
		return timedRotator.mark(mes, offset) || fileSizeRotator.mark(mes, offset);
	}

	@Override
	public void reset() {
		timedRotator.reset();
		fileSizeRotator.reset();
	}

	public TimedRotator getTimedRotator() {
		return timedRotator;
	}

	public FileSizeRotator getFileSizeRotator() {
		return fileSizeRotator;
	}
	
}
