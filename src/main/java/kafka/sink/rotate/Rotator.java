package kafka.sink.rotate;

public interface Rotator {

	public boolean mark(String mes, long offset);
	
	public void reset();
}
