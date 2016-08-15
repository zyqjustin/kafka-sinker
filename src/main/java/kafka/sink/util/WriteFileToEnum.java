package kafka.sink.util;

import java.util.HashMap;
import java.util.Map;

import kafka.sink.write.hdfs.HdfsFileWriter;
import kafka.sink.write.local.LocalFileWriter;

public enum WriteFileToEnum {
	
	LOCAL(LocalFileWriter.class),
	
	HDFS(HdfsFileWriter.class);
	
	private static Map<String, WriteFileToEnum> writeFileToMap;
	private Class<?> writeClass;
	
	private WriteFileToEnum(Class<?> writeClass) {
		this.writeClass = writeClass;
	}

	static {
		writeFileToMap = new HashMap<String, WriteFileToEnum>();
		for (WriteFileToEnum to : WriteFileToEnum.values()) {
			writeFileToMap.put(to.toString().toLowerCase(), to);
		}
	}
	
	public static WriteFileToEnum getWriteFileTo(String writeFileTo) {
		return writeFileToMap.get(writeFileTo);
	}

	public Class<?> getWriteClass() {
		return writeClass;
	}
	
}
