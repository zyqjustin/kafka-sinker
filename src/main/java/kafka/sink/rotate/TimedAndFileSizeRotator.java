package kafka.sink.rotate;

import kafka.sink.rotate.FileSizeRotator.SizeUnits;

public class TimedAndFileSizeRotator extends TimedRotator {

	private long maxBytes;
	private long currentBytesWritten = 0; 
	
	public TimedAndFileSizeRotator(float count, TimeUnit units, float fileCount, SizeUnits sizeUnits) {
		super(count, units);
		this.maxBytes = (long)(fileCount * sizeUnits.getByteCount());
	}

	@Override
	public boolean rotate() {
		return this.currentBytesWritten >= this.maxBytes;
	}

	@Override
	public void reset() {
		this.currentBytesWritten = 0L;
	}

	public long getMaxBytes() {
		return maxBytes;
	}

	public long getCurrentBytesWritten() {
		return currentBytesWritten;
	}
	
}
