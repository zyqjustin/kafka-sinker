package kafka.sink.rotate;

public class FileSizeRotator implements Rotator {
	
	public static enum SizeUnits {
		
		KB((long)Math.pow(2, 10)),
		MB((long)Math.pow(2, 20)),
		GB((long)Math.pow(2, 30)),
		TB((long)Math.pow(2, 40));
		
		private long byteCount;

		private SizeUnits(long byteCount) {
			this.byteCount = byteCount;
		}

		public long getByteCount() {
			return byteCount;
		}
		
	}
	
	private long maxBytes;
	private long lastOffset = 0;
	private long currentBytesWritten = 0;
	
	public FileSizeRotator(float count, SizeUnits units) {
		this.maxBytes = (long)(count * units.getByteCount());
	}
	
	@Override
	public boolean mark(String mes, long offset) {
		long diff = offset - this.lastOffset;
		this.currentBytesWritten += diff;
		this.lastOffset = offset;
		return this.currentBytesWritten >= this.maxBytes;
	}

	@Override
	public void reset() {
		this.lastOffset = 0;
		this.currentBytesWritten = 0L;
	}

	public long getMaxBytes() {
		return maxBytes;
	}

	public long getCurrentBytesWritten() {
		return currentBytesWritten;
	}
	
}
