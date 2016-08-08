package kafka.sink.rotate;

public class TimedRotator implements Rotator {
	
	public static enum TimeUnit {
		
		SECONDS(1000L),
		MINUTES(1000L * 60),
		HOURS(1000L * 60 * 60),
		DAYS(1000L * 60 * 60 *24);
		
		private long milliSeconds;
		
		private TimeUnit(long milliSeconds) {
			this.milliSeconds = milliSeconds;
		}

		public long getMilliSeconds() {
			return milliSeconds;
		}
	}
	
	private long interval;

	public TimedRotator(float count, TimeUnit units) {
		this.interval = (long)(count * units.getMilliSeconds());
	}

	@Override
	public boolean rotate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
	}

	public long getInterval() {
		return interval;
	}
	
}
