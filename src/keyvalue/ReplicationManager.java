package keyvalue;

import gossip.Gossip;
import gossip.HeartBeat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import log.Log;
import serverclient.UDPClient;
import serverclient.UDPServer;
import table.Command;
import table.KeyValueTable;
import table.VectorClock;
import chord.Chord;

public class ReplicationManager {

	KeyValueTable table;

	public static final int RM_SERVER_PORT = 5991;
	public static final int SERVER_PORT = 5998;
	public static final int CLIENT_PORT = 5997;
	public static final int GOSSIP_RM_SERVER_PORT = 5996;
	public static final int GOSSIP_RM_CLIENT_PORT = 5995;
	public static final long STABILITY_TIME = 4000;
	private static final String IPADDRESSLIST = "IpAddressList.txt";
	
	// We only have 21 xinu machines
	private int MACHINE_NUMBER = 8;
	
	private VectorClock vc;
	
	// Xinu Machine Index
	private int currentID;

	UDPServer server, RMServer, receiveTable;
	private RMShowTable rmShowTable;
	Log logger;

	// List<HeartBeat> listForBulkInsert;

	public ReplicationManager() {
		this.logger = new Log("RM." + Chord.getIpAddress() + ".log");
		table = new KeyValueTable();
		
		currentID = -1;
	
		// Look for current Xinu index from Gossip.
		findMachinIndex ();
		vc = new VectorClock (MACHINE_NUMBER);
		
		if (currentID == -1) {
			System.out.println ("Machine IP address unrecognized!");
			System.out.println 
				("Put this machine's address into IpAddressList.txt if it is a server!");
			System.exit (-1);
		}

		RepairReplication repair = new RepairReplication(table);
		rmShowTable = new RMShowTable(table, repair);
		server = new UDPServer(SERVER_PORT);
		receiveTable = new UDPServer(GOSSIP_RM_CLIENT_PORT);
		RMServer = new UDPServer(RM_SERVER_PORT);
		// listForBulkInsert = new ArrayList<HeartBeat>();

		Thread repairThread = new Thread(repair);
		Thread serverThread = new Thread(server);
		Thread RMThread = new Thread(RMServer);
		Thread receive = new Thread(receiveTable);
		Thread rmShowTableThread = new Thread(rmShowTable);
		rmShowTableThread.start();
		repairThread.start();
		serverThread.start();
		RMThread.start();
		receive.start();
	}

	private void findMachinIndex () {		
		BufferedReader br = null;
		String ipString = "";
		// Taking from text file
		try {
			br = new BufferedReader(new FileReader(IPADDRESSLIST));
		} catch (FileNotFoundException e) {
			System.out.println("File " + IPADDRESSLIST + " not found!!");
			e.printStackTrace();
			
			currentID = -1;
			return;
		}
	
		try {
			int c = 0;
			while ((ipString = br.readLine()) != null) {
				if (Chord.getIpAddress().equals (ipString)) {
					//System.out.println ("Found IP " + ipString);
					currentID = c;
				}
				c++;
			}
			br.close();// close the file reader here
			MACHINE_NUMBER = c;
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	
	public void startRM() {
		while (true) {
			getCommandsFromServer();
		}
	}

	private void getCommandsFromServer() {
		Object obj = server.getNextObject();
		vc.Increment(currentID);
		if (obj instanceof String)
			rmShowTable.insertWholeFile((String) obj, vc);
		else {
			Command command = (Command) obj;
			if (command.getStage() == -1) {
				printMessage("Executing Repair command recieved from RM");
				List<HeartBeat> hbList = getHeartBeatListFromLocalGossip();
				if (RepairReplication.checkIfAddressInGroup(
						command.getKeyValue(), Chord.getIpAddress(), hbList,
						logger)) {
					//command.setVC(vc);
					command.execute(table);
					this.rmShowTable.addCommand(command);
				}
			} else if (command.getStage() == 1) {
				printMessage("Command received from FE");
				// listForBulkInsert = getHeartBeatListFromLocalGossip();
				command.setStageRMtoRM();
				command.setVC(vc);
				coordinateWithRMS(command);
			} else if (command.getStage() == 2) {
				printMessage("Command received from Primary RM");
				//command.setVC(vc);
				command.execute(table);
				this.rmShowTable.addCommand(command);
				command.setStageTwoStatus(true);
				sendResponseBack(command, command.getRMAddress(),
						RM_SERVER_PORT);
			}
		}
	}

	/**
	 * 
	 */
	private void printMessage(String msg) {
		logger.writeLogMessage(msg);
		System.out.println(msg);
	}

	private void coordinateWithRMS(Command command) {
		int level = command.getCurrentConsistencyLevel().ordinal();
		if (level == 0) {
			// for level 0, send back response first
			command.execute(table);
			this.rmShowTable.addCommand(command);
			sendResponseBack(command, command.getClientAddress(), CLIENT_PORT);
			sendCommandToSuccessors(command);
		} else {
			// for other levels, wait for response from other RM and then
			// respond
			sendCommandToSuccessors(command);
			checkConsistencyAndRespond(command, level);
		}
	}

	/**
	 * Send response to client if consistency level has reached
	 * 
	 * @param command
	 * @param level
	 */
	private void checkConsistencyAndRespond(Command command, int level) {
		int numberOfResponse = getResponseFromTwoSuccessor(level);
		if (numberOfResponse == level) {
			command.execute(table);
			this.rmShowTable.addCommand(command);
			sendResponseBack(command, command.getClientAddress(), CLIENT_PORT);
		} else {
			System.out.println("Consistency level not reached!!");
			UDPClient response = new UDPClient(command.getClientAddress(),
					CLIENT_PORT);
			response.send("Consistency level unreachable. Try again!!");
		}
	}

	/**
	 * Get response from next 2 successors
	 * 
	 * @param command
	 * @return No. of Responses
	 */
	private int getResponseFromTwoSuccessor(int level) {
		int numberOfResponse = 0;
		for (int i = 0; i < 2; i++) {
			Command cmdResponse = (Command) RMServer.getNextObject();
			if (cmdResponse.getStageTwoStatus()) {
				printMessage("Response received from RM");
				numberOfResponse++;
				if (level == 1) {
					break;
				}
			}
		}
		return numberOfResponse;
	}

	/**
	 * @param command
	 */
	private void sendCommandToSuccessors(Command command) {
		List<HeartBeat> heartBeatList = getHeartBeatListFromLocalGossip();
		String[] successors = RepairReplication.getSuccessors(
				Chord.getIpAddress(), 2, heartBeatList);
		for (String successor : successors) {
			printMessage("Sending to successor: " + successor);
			UDPClient client = new UDPClient(successor, SERVER_PORT);
			client.send(command);
		}
	}

	@SuppressWarnings("unchecked")
	private List<HeartBeat> getHeartBeatListFromLocalGossip() {
		String contactNodeIp = Chord.getIpAddress();
		UDPClient sendRequest = new UDPClient(contactNodeIp,
				GOSSIP_RM_SERVER_PORT);
		sendRequest.send(contactNodeIp);
		return (ArrayList<HeartBeat>) receiveTable.getNextObject();
	}

	/**
	 * Sending response back to Client or PrimaryRM
	 * 
	 * @param command
	 */
	private void sendResponseBack(Command command, String address, int port) {
		
		/*if(command instanceof LookUp){
			String value = (String)command.getKeyValue().getValue();
			if(value!=null){
				if(value.length()>250){
					value = value.substring(0,250)+"...";
					command.getKeyValue().set(value);
				}
			
			}
		}*/
		printMessage("Sending response back to: " + address);
		UDPClient client = new UDPClient(address, port);
		client.send(command);
	}

	public static void main(String[] args) {
		
		Gossip gossip = new Gossip("gossip");
		gossip.start();
		
		ReplicationManager manager = new ReplicationManager();
		manager.startRM();
	}
}
