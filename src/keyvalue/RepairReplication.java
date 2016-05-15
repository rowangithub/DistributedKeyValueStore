package keyvalue;

import gossip.HeartBeat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import log.Log;
import serverclient.UDPClient;
import serverclient.UDPServer;
import table.Insert;
import table.KeyValue;
import table.KeyValueTable;
import table.Value;
import chord.Chord;

public class RepairReplication implements Runnable {
	public final static int PORT_NUMBER = 5990;
	private UDPServer server;
	private List<HeartBeat> currentList;
	private KeyValueTable kvTable;
	Log logger;

	public RepairReplication(KeyValueTable table) {
		this.logger = new Log("rm." + Chord.getIpAddress() + ".log");
		server = new UDPServer(PORT_NUMBER);
		currentList = new ArrayList<HeartBeat>();
		kvTable = table;

		Thread serverThread = new Thread(server);
		serverThread.start();
	}

	@Override
	public void run() {
		while (true)
			updateCurrentList();
	}


	@SuppressWarnings("unchecked")
	private void updateCurrentList() {
		ArrayList<HeartBeat> heartBeatList = (ArrayList<HeartBeat>) server
				.getNextObject();
		if (heartBeatList.size() > currentList.size()) {
			String addedNodeIp = getAddedNodeIP(this.currentList, heartBeatList);
			this.currentList = heartBeatList;
			repairJoin(addedNodeIp);
		} else if (heartBeatList.size() < currentList.size()) {
			repairFailure(heartBeatList);
		}
		this.currentList = heartBeatList;
	}

	private void repairFailure(List<HeartBeat> heartBeatList) {
		Collection<KeyValue> keyValues = kvTable.getHashElements();
		if (keyValues.size() != 0) {
			printMessage("Repair Failure triggered!!", logger);
			for (KeyValue kv : keyValues) {
				if (checkIfPrimary(kv, heartBeatList, Chord.getIpAddress())) {
					replicateToSuccessors(kv, heartBeatList);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void replicateToSuccessors(KeyValue kv, List<HeartBeat> heartBeatList) {
		String[] successors = getSuccessors(Chord.getIpAddress(), 2,
				heartBeatList);
		for (String successor : successors) {
			UDPClient client = new UDPClient(successor,
					ReplicationManager.SERVER_PORT);
			
			for (Value v : (List<Value>) kv.getValue ()) {
				Insert insertCommand = new Insert(kv.getKey(), v.value,
						Chord.getIpAddress(), "repair");
				insertCommand.setVC(v.clock);
				insertCommand.setRepairStage();
				client.send(insertCommand);
			}
		}
		printMessage("Replication done!", logger);
	}


	public static String[] getSuccessors(String start, int numberOfSuccessors,
			List<HeartBeat> heartBeatList) {
		HeartBeat hb = findHeartBeat(start, heartBeatList);
		int curr = heartBeatList.indexOf(hb);
		String[] successorsIPs = new String[numberOfSuccessors];
		int index = 0;
		while (index != numberOfSuccessors) {
			curr = ((curr + 1) >= heartBeatList.size()) ? 0 : curr + 1;
			successorsIPs[index++] = heartBeatList.get(curr).getIpAddress();
		}
		return successorsIPs;
	}

	private static HeartBeat findHeartBeat(String ip,
			List<HeartBeat> heartBeatList) {
		for (HeartBeat hb : heartBeatList) {
			if (hb.getIpAddress().equals(ip)) {
				return hb;
			}
		}
		return null;
	}

	public static boolean checkIfPrimary(KeyValue kv,
			List<HeartBeat> heartBeatList, String ip) {
		String primary = Chord.getAddressForKey(kv.getKey(), heartBeatList);
		boolean retVal = ip.equals(primary);
		return retVal;
	}

	private void repairJoin(String addedNodeIp) {
		Collection<KeyValue> keyValues = kvTable.getHashElements();
		if (keyValues.size() != 0) {
			printMessage("Repair join triggered!!", logger);
			for (KeyValue kv : keyValues) {
				if (checkIfAddressInGroup(kv, addedNodeIp, this.currentList,
						logger)) {
					printMessage("sending to added node in group...", logger);
					replicateToAddedNode(kv, addedNodeIp);
				}
				if (!checkIfAddressInGroup(kv, Chord.getIpAddress(),
						this.currentList, logger)) {
					printMessage("I don't belong here.... :(", logger);
					this.kvTable.delete(kv.getKey());
				}
			}
		}
	}

	public static boolean checkIfAddressInGroup(KeyValue kv,
			String addedNodeIp, List<HeartBeat> currentList, Log logger) {
		int key = kv.getKey();
		List<String> group = getGroup(key, currentList);

		boolean retVal = group.contains(addedNodeIp);

		return retVal;
	}

	private static List<String> getGroup(int key, List<HeartBeat> currentList) {
		List<String> retVal = new ArrayList<String>(3);
		String primary = Chord.getAddressForKey(key, currentList);
		retVal.add(primary);
		String[] successors = getSuccessors(primary, 2, currentList);
		retVal.add(successors[0]);
		retVal.add(successors[1]);
		return retVal;
	}

	@SuppressWarnings("unchecked")
	private void replicateToAddedNode(KeyValue kv, String addedNodeIp) {
		UDPClient client = new UDPClient(addedNodeIp,
				ReplicationManager.SERVER_PORT);
		
		for (Value v : (List<Value>) kv.getValue ()) {
			Insert insertCommand = new Insert(kv.getKey(), v.value,
					Chord.getIpAddress(), "repair");
			insertCommand.setVC(v.clock);
			insertCommand.setRepairStage();
			client.send(insertCommand);	
		}
	}

	public static String getAddedNodeIP(List<HeartBeat> currentList2,
			List<HeartBeat> heartBeatList) {
		HashMap<String, String> map = new HashMap<String, String>();
		String ip = null;
		for (HeartBeat hb : currentList2)
			map.put(hb.getIpAddress(), "add");
		for (HeartBeat hb : heartBeatList)
			if (!map.containsKey(hb.getIpAddress())
					&& !(hb.getIpAddress().equalsIgnoreCase(Chord
							.getIpAddress()))) {
				ip = hb.getIpAddress();
			}
		System.out.println("Added Node: " + ip);
		return ip;
	}


	private static void printMessage(String msg, Log logger) {
		logger.writeLogMessage(msg);
		System.out.println(msg);
	}
	public List<HeartBeat> getHeartBeatList(){
		return this.currentList;
	}
}
