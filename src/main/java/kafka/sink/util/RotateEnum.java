package kafka.sink.util;

import java.util.HashMap;
import java.util.Map;

import kafka.sink.rotate.FileSizeRotator;
import kafka.sink.rotate.TimedAndFileSizeRotator;
import kafka.sink.rotate.TimedRotator;

public enum RotateEnum {
	
	SIZE(FileSizeRotator.class),
	
	TIME(TimedRotator.class),
	
	BOTH(TimedAndFileSizeRotator.class);

	private static Map<String, RotateEnum> rotateMap;
	private Class<?> rotateClass;
	
	private RotateEnum(Class<?> rotateClass) {
		this.rotateClass = rotateClass;
	}

	static {
		rotateMap = new HashMap<String, RotateEnum>();
		for (RotateEnum rotate : RotateEnum.values()) {
			rotateMap.put(rotate.toString().toLowerCase(), rotate);
		}
	}

	public static RotateEnum getRotate(String rotate) {
		return rotateMap.get(rotate);
	}
	
	public Class<?> getRotateClass() {
		return rotateClass;
	}
	
}
