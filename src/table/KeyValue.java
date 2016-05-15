package table;

import java.io.Serializable;

public class KeyValue implements Serializable{

	private static final long serialVersionUID = -3583313633984155977L;
	private int key;
	private Object value;
	
	public KeyValue(int key, Object value) {
		this.setKey(key);
		this.set(value);
	}

	public void setKey(int key){
		this.key = key;
	}
	public void set(Object value){
		this.value = value;
	}
	
	public int getKey(){
		return this.key;

	}
	public Object getValue(){
		return this.value;
	}
}
