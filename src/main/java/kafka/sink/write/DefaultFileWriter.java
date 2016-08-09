package kafka.sink.write;

import kafka.sink.rotate.FileNameFormat;
import kafka.sink.rotate.Rotator;

public abstract class DefaultFileWriter {

	protected FileNameFormat fileNameFormat;
	protected Rotator rotator;
	
	public DefaultFileWriter(FileNameFormat fileNameFormat, Rotator rotator) {
		this.fileNameFormat = fileNameFormat;
		this.rotator = rotator;
	}

	public FileNameFormat getFileNameFormat() {
		return fileNameFormat;
	}

	public Rotator getRotator() {
		return rotator;
	}

}
