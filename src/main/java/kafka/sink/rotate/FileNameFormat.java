package kafka.sink.rotate;

public interface FileNameFormat {

	public String getName(long timestamp);
	
	public String getPath();
}
