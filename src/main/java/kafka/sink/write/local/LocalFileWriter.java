package kafka.sink.write.local;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.Queue;

import kafka.sink.rotate.FileNameFormat;
import kafka.sink.rotate.Rotator;
import kafka.sink.write.DefaultFileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO thinking how to update offset in rotator, then rotate file!
 * @author zhuyuqiang
 * @date 2016年8月12日 下午4:26:54
 * @version 1.0
 */
public class LocalFileWriter extends DefaultFileWriter {
	private static final Logger _logger = LoggerFactory.getLogger(LocalFileWriter.class);

	private String fileDir;
	private FileOutputStream fileOutputStream;
	private OutputStreamWriter outputStreamWriter;
	private BufferedWriter bufferedWriter;

	public LocalFileWriter(FileNameFormat fileNameFormat, Rotator rotator) {
		super(fileNameFormat, rotator);
		
		try {
			createWriteFile();
		} catch (IOException e) {
			throw new RuntimeException("Create buffered writer for local file failed, file dir: " + fileDir, e);
		}
	}

	@Override
	public void batchWrite(Queue<String> messages) throws IOException {
		if (_logger.isDebugEnabled()) {
			_logger.debug("Write local file=[{}].", fileDir);
		}
		
		String mes;
		while ((mes = messages.poll()) != null) {
			bufferedWriter.append(mes);
			bufferedWriter.newLine();
			bufferedWriter.flush();
		}
	}
	
	@Override
	public void closeWriteFile() throws IOException {
		try {
			if (bufferedWriter != null) {
				bufferedWriter.close();
			}
			if (outputStreamWriter != null) {
				outputStreamWriter.close();
			}
			if (fileOutputStream != null) {
				fileOutputStream.close();
			}
		} catch (IOException e) {
			_logger.warn("Close local file buffered writer failed. Error messages: {}", e.getMessage());
		}
	}
	
	@Override
	public String createWriteFile() throws IOException {
		this.fileDir = fileNameFormat.getPath() + "/" + fileNameFormat.getName(new Date().getTime());
		
		File filePath = new File(fileNameFormat.getPath());
		if (!filePath.exists()) {
			filePath.mkdirs();
		}
		
		File file = new File(this.fileDir);
		if (!file.exists()) {
			file.createNewFile();
		}
		
		fileOutputStream = new FileOutputStream(file, true);
		outputStreamWriter = new OutputStreamWriter(fileOutputStream);
		bufferedWriter = new BufferedWriter(outputStreamWriter);		
		
		return fileDir;
	}
	
}
