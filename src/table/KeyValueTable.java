package table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import chord.Chord;

public class KeyValueTable {

	public int MAX_KEYS = 1000000;
	private ConcurrentHashMap<Integer, List<Value>> hashMap;

	public KeyValueTable() {
		this.hashMap = new ConcurrentHashMap<Integer, List<Value>>();
	}

	public boolean delete(int key) {
		Object removedObject = this.hashMap.remove(key);
		if (removedObject == null) {
			return false;
		} else {
			return true;
		}
	}

	public Object lookUp(int key) {
		Object retVal = this.hashMap.get(key);
		return retVal;
	}

	public boolean update(int key, Object value, VectorClock vc) {
		// Check
		if (this.hashMap.contains(key)) { 
			List<Value> old = this.hashMap.get(key);
			boolean upd = false;
			for (int i = 0; i < old.size (); i++) {
				Value oldvalue = old.get(i);
				int flag = oldvalue.clock.compareTo(vc);
				if (flag == -1) {
					// Do update
					oldvalue.clock.updateTimeStamp(vc);
					oldvalue.value = value;
					upd = true;
				} else if (flag == 1) {
					// Do not need update
					return false;
				}
			}
			// Make it concurrent 
			if (!upd) {
				old.add (new Value (value, vc));
			}
			return true;
		} else {
			List<Value> curr = new ArrayList<Value> ();
			curr.add (new Value (value, vc));
			this.hashMap.put(key, curr);
			
			return true;
		}
	}

	public boolean insert(int key, Object value, VectorClock vc) {		
		return update (key, value, vc);
	}

	public void printHashElements() {
		System.out.print(getHashElementsString());
	}

	public String getHashElementsString() {
		String retVal = "";
		String ownIp = Chord.getIpAddress();
		int serverRingKey = Chord.getRingID(ownIp);
		retVal+="%%%%%% Current Table Keys for"+ serverRingKey+": %%%%%%\n";
		Enumeration<Integer> keys = this.hashMap.keys();
		while (keys.hasMoreElements()) {
			String key = Integer.toString(keys.nextElement());
			retVal+="\tKey: " +key+ "\tRingKey: "+Chord.getRingID(key)+"\n";
		}
		retVal+="%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%Size: "+this.hashMap.size()+"\n";
		return retVal;
	}

	public Collection<KeyValue> getHashElements() {
		Collection<KeyValue> retKVCollection = new ArrayList<KeyValue>();
		Enumeration<Integer> keys = this.hashMap.keys();
		while (keys.hasMoreElements()) {
			int key = keys.nextElement();
			KeyValue kv = new KeyValue(key, this.hashMap.get(key));
			retKVCollection.add(kv);
		}
		return retKVCollection;
	}

	public int size(){
		return hashMap.size();
	}
}
