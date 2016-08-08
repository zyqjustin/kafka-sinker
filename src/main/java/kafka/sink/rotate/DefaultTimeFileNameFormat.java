package kafka.sink.rotate;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DefaultTimeFileNameFormat implements FileNameFormat {

	private String clientId;
	private String path = "kafkasinker";
	private String prefix = "";
	private String extension = ".txt";
	
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH/mm");

	// TODO need?
	public DefaultTimeFileNameFormat withClientId(String clientId) {
		this.clientId = clientId;
		return this;
	}
	
	public DefaultTimeFileNameFormat withPrefix(String prefix) {
		this.prefix = prefix;
		return this;
	}
	
	public DefaultTimeFileNameFormat withPath(String path) {
		this.path = path;
		return this;
	}
	
	public DefaultTimeFileNameFormat withExtension(String extension) {
		this.extension = extension;
		return this;
	}
	
	@Override
	public String getName(long timestamp) {
		return this.prefix + '-' + this.clientId + "-" + timestamp + this.extension;
	}

	@Override
	public String getPath() {
		return getCorrectPath(this.path) + dateFormat.format(new Date());
	}
	
	private String getCorrectPath(String path) {
		return (path.charAt(path.length() - 1) != '/') ? path + '/' : path;
	}
	
}
