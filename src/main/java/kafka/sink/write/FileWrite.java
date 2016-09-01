package kafka.sink.write;

import java.io.IOException;
import java.util.Queue;

public interface FileWrite {

	public void batchWrite(Queue<String> messages) throws IOException;
	
	public String getFilePath();
}
