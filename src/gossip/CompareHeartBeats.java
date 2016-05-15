package gossip;

import java.util.Comparator;

public class CompareHeartBeats implements Comparator<HeartBeat>{

	@Override
	public int compare(HeartBeat o1, HeartBeat o2) {
		int retVal =  o1.getId()-o2.getId();
		return retVal;
	}
	
}