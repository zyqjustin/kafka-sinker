package kafka.sink.write;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import kafka.sink.rotate.FileNameFormat;
import kafka.sink.rotate.Rotator;
import kafka.sink.rotate.TimedAndFileSizeRotator;
import kafka.sink.rotate.TimedRotator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DefaultFileWriter {
	private static final Logger _logger = LoggerFactory.getLogger(DefaultFileWriter.class);

	protected FileNameFormat fileNameFormat;
	protected Rotator rotator;
	protected Object writeLock;
	
	protected int rotationCount = 0;
	protected Timer rotationTimer; 
	
	public DefaultFileWriter(FileNameFormat fileNameFormat, Rotator rotator) {
		this.fileNameFormat = fileNameFormat;
		this.rotator = rotator;
		
		if (rotator == null) {
			throw new IllegalArgumentException("File rotation policy must be specified.");
		}
		
		this.writeLock = new Object();
		if (this.rotator instanceof TimedRotator || this.rotator instanceof TimedAndFileSizeRotator) {
			long interval = getInterval();
			TimerTask task = new TimerTask() {
				@Override
				public void run() {
					try {
						rotateWriteFile();
					} catch (IOException e) {
						_logger.warn("Rotate write file encounter IOException.", e);
					}
				}
			};
			// scheduled rotate
			this.rotationTimer.scheduleAtFixedRate(task, interval, interval);
		}
	}

	public void rotateWriteFile() throws IOException {
		_logger.info("Rotating write file...");
		long start = System.currentTimeMillis();
		synchronized (this.writeLock) {
			closeWriteFile();
			this.rotationCount++;
			String filePathStr = createWriteFile();
			_logger.info("Rotate file {} times, current file path=[{}].", rotationCount, filePathStr);
		}
		long period = System.currentTimeMillis() - start;
		_logger.info("File rotation took {} ms.", period);
	}
	
	public abstract void closeWriteFile() throws IOException;
	
	public abstract String createWriteFile() throws IOException;
	
	private long getInterval() {
		if (this.rotator instanceof TimedRotator) {
			return ((TimedRotator)this.rotator).getInterval();
		} else if (this.rotator instanceof TimedAndFileSizeRotator) {
			return ((TimedAndFileSizeRotator)this.rotator).getTimedRotator().getInterval();
		}
		
		throw new IllegalArgumentException("Could not get interval for TimeRotator.");
	}
	
	public FileNameFormat getFileNameFormat() {
		return fileNameFormat;
	}

	public Rotator getRotator() {
		return rotator;
	}

}
