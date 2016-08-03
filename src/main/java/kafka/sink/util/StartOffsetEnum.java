package kafka.sink.util;

import java.util.HashMap;
import java.util.Map;

public enum StartOffsetEnum {
	ERROR,

	CUSTOM,
	
	EARLIEST,
	
	LATEST,
	
	RESTART;
	
	private static Map<String, StartOffsetEnum> typeMap;
	
	static {
		typeMap = new HashMap<String, StartOffsetEnum>();
		for (StartOffsetEnum e : StartOffsetEnum.values()) {
			typeMap.put(e.toString(), e);
		}
	}
	
	public boolean equals(String startOffsetFrom) {
		return this.toString().equals(startOffsetFrom);
	}
	
	public static StartOffsetEnum getStartOffsetType(String startOffsetFrom) {
		StartOffsetEnum type = typeMap.get(startOffsetFrom);
		return type != null ? type : ERROR;
	}
	
}
