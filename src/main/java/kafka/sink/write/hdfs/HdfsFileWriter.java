package kafka.sink.write.hdfs;

import java.io.IOException;
import java.util.Queue;

import kafka.sink.rotate.FileNameFormat;
import kafka.sink.rotate.Rotator;
import kafka.sink.write.DefaultFileWriter;

/**
 * TODO finish
 * @author zhuyuqiang
 * @date 2016年8月15日 下午5:22:17
 * @version 1.0
 */
public class HdfsFileWriter extends DefaultFileWriter {

	public HdfsFileWriter(FileNameFormat fileNameFormat, Rotator rotator) {
		super(fileNameFormat, rotator);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void batchWrite(Queue<String> messages) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void closeWriteFile() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String createWriteFile() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
