package gossip;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import log.Log;

public class HeartBeatTable {

	public static long WAIT_TIME = 500;
	public static long CLEAN_UP = 1000;
	public static long FAIL_TIME = 1000;
	public AtomicInteger numConnections = new AtomicInteger(1);
	ConcurrentHashMap<String, Timestamp> hasFinalRemoved = new ConcurrentHashMap<String, Timestamp>();
	ConcurrentHashMap<String, HeartBeat> heartBeatMap;
	ConcurrentHashMap<String, Long> localTimeMap;
	ConcurrentHashMap<String, HeartBeat> hasFailedMap;
	public Log logger;
	public HeartBeat own;

	public HeartBeatTable(HeartBeat own, Log logger) {
		this.own = own;
		setupMaps(own);
		this.logger = logger;

	}

	private void setupMaps(HeartBeat own) {
		this.heartBeatMap = new ConcurrentHashMap<String, HeartBeat>();
		this.localTimeMap = new ConcurrentHashMap<String, Long>();
		this.hasFailedMap = new ConcurrentHashMap<String, HeartBeat>();
		this.heartBeatMap.put(own.getIpAddress(), own);
		this.localTimeMap.put(own.getIpAddress(), System.currentTimeMillis());
	}

	public void updateTable(ArrayList<HeartBeat> receivedTable) {
		for (HeartBeat hb : receivedTable) {
			updateTable(hb);
		}
	}

	public void updateTable(HeartBeat hb) {

		// we don't want to change our own heart beat
		if (hb.getIpAddress() == this.own.getIpAddress()) {
			return;
		}

		String key = hb.getIpAddress();
		if (!this.heartBeatMap.containsKey(key)) {// check if new node
			addNewNodes(hb, key);

		} else {
			mergeNewNodes(hb, key);

		}
	}

	private void mergeNewNodes(HeartBeat hb, String key) {
		HeartBeat old = this.heartBeatMap.get(key);// merge new values
		if (old.getHeartBeatCounter() < hb.getHeartBeatCounter()) {
			
				if (old.getTimeStamp()==null) {
					System.out.println("updated timestamp");
					old.setIncarnationTimeStamp(hb.getTimeStamp());
				}
			
			this.localTimeMap.put(key, System.currentTimeMillis());
			old.setAndCompareHeartBeatCounter(hb.getHeartBeatCounter());
			if (this.hasFailedMap.containsKey(key)) {
				this.hasFailedMap.remove(key);
				if (logger != null) {
					logger.writeLogMessage("Unmarked for Failure "
							+ hb.getIpAddress());
				}
			}
		}
	}

	private void addNewNodes(HeartBeat hb, String key) {
		if (checkIfPermanentlyFailed(hb))
			return;
		this.heartBeatMap.put(key, hb);
		this.localTimeMap.put(key, System.currentTimeMillis());
		this.updateReplicationManager();
		if (logger != null) {
			logger.writeLogMessage("JOIN Machine " + key + " with keyId: " + hb.getId()
					+ " incarnation time stamp " + hb.getTimeStamp());
		}
		System.out.println("JOIN Machine " + key + " with keyId: " + hb.getId());
	}

	private boolean checkIfPermanentlyFailed(HeartBeat hb) {
		Timestamp start = hb.getTimeStamp();
		String ip = hb.getIpAddress();
		Timestamp compareTS = this.hasFinalRemoved.get(ip);
		boolean ipFailed = false;
		if (compareTS != null) {
			ipFailed = compareTS.equals(start);

		}
		logger.writeLogMessage(ip + " hb start:"
				+ (start == null ? null : start.toString()));
		logger.writeLogMessage(ip + "hb compareTS:"
				+ (compareTS == null ? null : compareTS.toString()));
		return ipFailed;
	}

	public void increaseOwnHeartBeat() {
		long ownHeartBeat = own.getHeartBeatCounter();
		own.setAndCompareHeartBeatCounter(ownHeartBeat + 1);
		this.localTimeMap.put(own.getIpAddress(), System.currentTimeMillis());
	}

	public void removeHeartBeat(HeartBeat hb) {
		String key = hb.getIpAddress();
		this.heartBeatMap.remove(key);
		this.hasFailedMap.remove(key);
		this.localTimeMap.remove(key);
	}

	public ArrayList<HeartBeat> maintain() {
		increaseOwnHeartBeat();
		checkForFailures();
		cleanUp();
		return getCurrentHeartBeatTable();
	}

	private void checkForFailures() {
		Collection<HeartBeat> collection = this.heartBeatMap.values();
		long currentTime = System.currentTimeMillis();
		for (HeartBeat hb : collection) {
			long localTime = this.localTimeMap.get(hb.getIpAddress());
			if (currentTime - localTime >= FAIL_TIME) {
				if (!this.hasFailedMap.containsKey(hb.getIpAddress())) {
					this.hasFailedMap.put(hb.getIpAddress(), hb);
					if (logger != null) {
						logger.writeLogMessage("Marked Fail "
								+ hb.getIpAddress());
					}
				}
			}
		}

	}

	private void cleanUp() {

		Collection<HeartBeat> collection = Collections
				.synchronizedCollection(this.hasFailedMap.values());
		long currentTime = System.currentTimeMillis();
		for (HeartBeat hb : collection) {
			long localTime = this.localTimeMap.get(hb.getIpAddress());
			if (currentTime - localTime >= FAIL_TIME + CLEAN_UP) {
				this.removeHeartBeat(hb);
				this.updateReplicationManager();
				assert (hb != null);
				assert (this.hasFinalRemoved != null);
				assert (hb.getIpAddress() != null);
				assert (hb.getTimeStamp () != null);
				
				if (hb.getTimeStamp() != null) {
					this.hasFinalRemoved.put(
							hb.getIpAddress(), 
							hb.getTimeStamp());
					if (logger != null) {
						logger.writeLogMessage("REMOVED" + hb.getIpAddress());
					}
					System.out.println("REMOVED " + hb.getIpAddress());
				}
			}
		}

	}

	private ArrayList<HeartBeat> getCurrentHeartBeatTable() {
		// return all heart beat values
		ArrayList<HeartBeat> retVal = new ArrayList<HeartBeat>();
		Collection<HeartBeat> collection = this.heartBeatMap.values();
		for (HeartBeat hb : collection) {
			retVal.add(hb);
		}
		return retVal;
	}

	public int getOwnId() {
		return own.getId();
	}

	public ArrayList<HeartBeat> getCurrentMembersSortedById() {
		ArrayList<HeartBeat> hbs = this.getCurrentHeartBeatTable();
		Comparator<HeartBeat> hbComparer = new CompareHeartBeats();
		Collections.sort(hbs, hbComparer);
		return hbs;
	}

	public int getSize() {
		return this.heartBeatMap.size();
	}

	public void reincarnate(boolean isContactNode) {
		own.setIncarnationTimeStamp();
		if (!isContactNode)
			this.setupMaps(own);
		else
			this.restoreTable();
	}

	private void restoreTable() {
		Collection<HeartBeat> collection = this.heartBeatMap.values();
		for (HeartBeat hb : collection) {
			this.localTimeMap
					.put(hb.getIpAddress(), System.currentTimeMillis());
		}

	}

	public List<String> selectMembers(ArrayList<HeartBeat> sendList) {
		List<String> randomMembers = new ArrayList<String>();
		Random randomGenerator = new Random();
		int size = sendList.size();
		if (size % 2 == 0) {
			this.numConnections.set(size / 2);
		} else {
			this.numConnections.set((size + 1) / 2);
		}
		while (randomMembers.size() < numConnections.get()
				&& randomMembers.size() < sendList.size() - 1) {
			int randIndex = randomGenerator.nextInt(sendList.size());
			HeartBeat toBeAddedHB = sendList.get(randIndex);
			String nextAddress = toBeAddedHB.getIpAddress();

			if (nextAddress != own.getIpAddress()) {// not own address
				if (!randomMembers.contains(nextAddress)) {// not already
															// selected
					randomMembers.add(nextAddress);
				}
			}
		}
		writeLog(randomMembers, sendList.size());
		return randomMembers;
	}

	private void writeLog(List<String> members, int size) {
		if (logger == null)
			return;
		logger.writeLogMessage("---------------");
		for (String ip : members) {
			logger.writeLogMessage("Sending Gossip to " + ip
					+ ". Current List size " + size);
		}
		String[] messages = this.getTableStateAsString().split("\\r?\\n");
		for (String message : messages) {
			logger.writeLogMessage(message);
		}
	}

	public String getTableStateAsString() {
		String retVal = "(ip,id, heart beat count,incarnationTimeStamp#)\n";
		ArrayList<HeartBeat> hbTable = this.getCurrentHeartBeatTable();
		for (HeartBeat hb : hbTable) {
			String ipAddress = hb.getIpAddress();
			int id = hb.getId();
			Long hbCount = hb.getHeartBeatCounter();
			Timestamp incarnNum = hb.getTimeStamp();
			retVal += "(" + ipAddress + " , " + id + " , " + hbCount + " , "
					+ incarnNum + " )\n";
		}
		return retVal;
	}

	public void setNumConnections(int setNumConnections) {
		this.numConnections.set(setNumConnections);

	}

	boolean first = false;

	public void updateReplicationManager() {
		Thread t = new Thread(new UpdateReplicationManager(
				this.getCurrentMembersSortedById()));
		t.start();
	}

}
