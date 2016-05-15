package table;

import java.io.Serializable;

public class VectorClock implements Comparable<VectorClock>,Serializable {
	
	private static final long serialVersionUID = 385994735238873832L;
	private int[] Vector;
	private int len;
	
	public VectorClock (int size) {
		Vector = new int[size];
		len = size;
		for(int i= 0; i<len;i++) {
			Vector[i] = 0;
		}
	}

	public int[] getVector() {
		return Vector;
	}

	public void setVector(int[] vector) {
		Vector = vector;
	}
	
	public void updateTimeStamp(VectorClock base) {// start from 0 
		
		for(int i = 0; i < len; i++) {
				Vector[i] = Math.max(Vector[i], base.getVector()[i]);
		}
		
	}
	
	public int Increment(int currentID) {
		if(currentID<len) {
			Vector[currentID] ++;
			return 1;
		}else {
			return -1;			// id exceeds the limitation
		}
	}
	
	public int compareTo(VectorClock o) {
		int len = Vector.length;
		int counter = 0;
		
		for(int i = 0; i < len; i++) {
			if(o.Vector[i] >= Vector[i]) {
				counter ++;
			}
		}
		if(counter == len) {
			return 1; 			// return 1 if happened before
		}
		counter = 0;
		for(int i = 0; i < len; i++) {
			if(o.Vector[i] <= Vector[i]) {
				counter ++;
			}
		}
		if(counter == len) {
			return -1;			// return -1 if happened after
		}
		
		return 0;				// return 0 if concurrent
		
	}
	
	public void issueTimeStamp(int id) {
		for(int i=0; i<len;i++) {
			if(i == id) {
				System.out.print(Vector[i]+1);
			}else{
				System.out.print(Vector[i]);
			}
			
		}
	}
	// TODO:  add resize the Vector
	public String toString() {
		String t = new String();
		for(int i = 0; i < len; i++) {
			if (i == len - 1) {
				t = t + Vector[i];
			} else {
				t = t + Vector[i] + ",";
			}
		}
		
		return t;
	}
}
