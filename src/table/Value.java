package table;

import java.io.Serializable;

public class Value implements Serializable {
	
	private static final long serialVersionUID = -3713899881140283412L;
	
	public Object value;
	public VectorClock clock;
	
	public Value (Object value, VectorClock clock) {
		this.value = value;
		this.clock = clock;
	}
	
	
	public String toString () {
		return "[Element: " + this.value + " Vector Clock: " + this.clock + "]";
	}
}