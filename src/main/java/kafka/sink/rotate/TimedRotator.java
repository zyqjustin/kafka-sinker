package kafka.sink.rotate;

public class TimedRotator implements Rotator {
	
	public static enum TimeUnits {
		
		SECONDS(1000L),
		MINUTES(1000L * 60),
		HOURS(1000L * 60 * 60),
		DAYS(1000L * 60 * 60 *24);
		
		private long milliSeconds;
		
		private TimeUnits(long milliSeconds) {
			this.milliSeconds = milliSeconds;
		}

		public long getMilliSeconds() {
			return milliSeconds;
		}
	}
	
	private long interval;

	public TimedRotator(int count, TimeUnits units) {
		this.interval = (long)(count * units.getMilliSeconds());
	}

	@Override
	public boolean mark(String mes, long offset) {
		return false;
	}

	@Override
	public void reset() {
		// do nothing
	}

	public long getInterval() {
		return this.interval;
	}

}
