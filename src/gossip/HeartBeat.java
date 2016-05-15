package gossip;

import java.io.Serializable;
import java.sql.Timestamp;

public class HeartBeat implements Serializable {

	private static final long serialVersionUID = 1L;

	private String ipAddress;
	private long heartBeatCounter;
	private Timestamp timesStamp;
	private int id;

	public HeartBeat(String ip, boolean setTimeStamp,int id) {
		this.ipAddress = ip;
		heartBeatCounter = 0;
		if(setTimeStamp)
		this.setIncarnationTimeStamp();
		this.id = id;
	}

	public synchronized long getHeartBeatCounter() {
		return heartBeatCounter;
	}

	public synchronized String getIpAddress() {
		return ipAddress;
	}

	public synchronized void setAndCompareHeartBeatCounter(long otherHeartBeat) {
		if (otherHeartBeat > this.heartBeatCounter) {
			this.heartBeatCounter = otherHeartBeat;
		}
	}

	public synchronized Timestamp setIncarnationTimeStamp() {
		return this.timesStamp = new Timestamp(System.currentTimeMillis());
	}

	public synchronized Timestamp getTimeStamp(){
		return this.timesStamp;
	}
	
	public int getId(){
		return this.id;
	}

	public void setIncarnationTimeStamp(Timestamp timeStamp) {
		this.timesStamp= timeStamp;
		
	}
}
