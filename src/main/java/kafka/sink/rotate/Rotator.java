package kafka.sink.rotate;

public interface Rotator {

	public boolean rotate();
	
	public void reset();
}
