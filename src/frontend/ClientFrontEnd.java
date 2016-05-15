package frontend;

import gossip.HeartBeat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import log.Log;
import serverclient.UDPClient;
import serverclient.UDPServer;
import table.Command;
import table.CommandParser;
import chord.Chord;

public class ClientFrontEnd implements Runnable {

	private static final String HELP_STRING = 
	"Available commands:\ninsert <int key> <string value> <consistency>,\ndelete <key> <consistency>,\nlookup <key> <consistency>,\nupdate <key> <value> <consistency>\n";
	private static final String IP_FOR_FE = "IpForFE.txt";
	public static final int SERVER_PORT = 5997;
	public static final int CLIENT_PORT = 5998;
	public static final int GOSSIP_FE_SERVER_PORT = 5928;
	public static final int GOSSIP_FE_CLIENT_PORT = 5927;
	public static final int SERVER_PACKET_SIZE = 1024;
	public static ArrayList<HeartBeat> hb;
	private List<Command> receivedCommands;
	UDPServer receiveTable, receiveCommand;
	public static HashMap<String, Integer> wordMap = new HashMap<String, Integer>();

	public ClientFrontEnd() {
		this.receivedCommands = Collections
				.synchronizedList(new ArrayList<Command>());
		receiveTable = new UDPServer(GOSSIP_FE_CLIENT_PORT);
		receiveCommand = new UDPServer(SERVER_PORT, SERVER_PACKET_SIZE);
		Thread t1 = new Thread(receiveCommand);
		t1.start();
		Thread receive = new Thread(receiveTable);
		receive.start();
	}

	public static String getMessageInput(String message) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input = "";
		System.out.println(message);
		try {
			input = br.readLine();
		} catch (IOException e) {
			System.out.println("error reading");
			e.printStackTrace();
		}
		return input;
	}

	@Override
	public void run() {
		while (true) {
			try {
				interactWithUser();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println ("System is still live. Enter help for help!");
			}
		}
	}

	public void interactWithUser() throws Exception {
		String commandString = getMessageInput("Please enter command, consistency: ");
		if (commandString.toLowerCase().contains("insert10000")) {
			String[] consistencyValue = commandString.split(" ");
			long starttime = System.currentTimeMillis();
			for (int i = 0; i < 10000; i++) {
				Command command = CommandParser.getCommandFromClient("insert "
						+ i + " BatchInsert " + consistencyValue[1]);
				ArrayList<HeartBeat> hbList = getHeartBeatList();
				if (hbList == null) {
					System.out.println ("Error: All servers shut down! Retry later.");
					return;
				}
				String replicationManagerIP = Chord.getAddressForKey(
						command.getKey(), hbList);
				if (command != null) {
					command.setServerAddress(replicationManagerIP);
					command.setStageFrontEndToRM();
					sendAndReceiveCommand(command);
				}
			}
			System.out.println ("Used time: " + ((System.currentTimeMillis() - starttime) / ((double) 1000)) + "s");
		} else if (commandString.toLowerCase().contains("insert1000")) {
			String[] consistencyValue = commandString.split(" ");
			long starttime = System.currentTimeMillis();
			for (int i = 0; i < 1000; i++) {
				Command command = CommandParser.getCommandFromClient("insert "
						+ i + " BatchInsert " + consistencyValue[1]);
				ArrayList<HeartBeat> hbList = getHeartBeatList();
				if (hbList == null) {
					System.out.println ("Error: All servers shut down! Retry later.");
					return;
				}
				String replicationManagerIP = Chord.getAddressForKey(
						command.getKey(), hbList);
				if (command != null) {
					command.setServerAddress(replicationManagerIP);
					command.setStageFrontEndToRM();
					sendAndReceiveCommand(command);
				}
			}
			System.out.println ("Used time: " + ((System.currentTimeMillis() - starttime) / ((double) 1000)) + "s");
		} else if (commandString.toLowerCase().contains("insert100")) {
			String[] consistencyValue = commandString.split(" ");
			long starttime = System.currentTimeMillis();
			for (int i = 0; i < 100; i++) {
				Command command = CommandParser.getCommandFromClient("insert "
						+ i + " BatchInsert " + consistencyValue[1]);
				ArrayList<HeartBeat> hbList = getHeartBeatList();
				if (hbList == null) {
					System.out.println ("Error: All servers shut down! Retry later.");
					return;
				}
				String replicationManagerIP = Chord.getAddressForKey(
						command.getKey(), hbList);
				if (command != null) {
					command.setServerAddress(replicationManagerIP);
					command.setStageFrontEndToRM();
					sendAndReceiveCommand(command);
				}
			}
			System.out.println ("Used time: " + ((System.currentTimeMillis() - starttime) / ((double) 1000)) + "s");
		} else if (commandString.toLowerCase().contains("insert10")) {
			String[] consistencyValue = commandString.split(" ");
			long starttime = System.currentTimeMillis();
			for (int i = 0; i < 10; i++) {
				Command command = CommandParser.getCommandFromClient("insert "
						+ i + " BatchInsert " + consistencyValue[1]);
				ArrayList<HeartBeat> hbList = getHeartBeatList();
				if (hbList == null) {
					System.out.println ("Error: All servers shut down! Retry later.");
					return;
				}
				String replicationManagerIP = Chord.getAddressForKey(
						command.getKey(), hbList);
				if (command != null) {
					command.setServerAddress(replicationManagerIP);
					command.setStageFrontEndToRM();
					sendAndReceiveCommand(command);
				}
			}
			System.out.println ("Used time: " + ((System.currentTimeMillis() - starttime) / ((double) 1000)) + "s");
		} else if (commandString.toLowerCase().contains("lookup10000")) {
			String[] consistencyValue = commandString.split(" ");
			long starttime = System.currentTimeMillis();
			for (int i = 0; i < 10000; i++) {
				Command command = CommandParser.getCommandFromClient("lookup "
						+ i + " " + consistencyValue[1]);
				ArrayList<HeartBeat> hbList = getHeartBeatList();
				if (hbList == null) {
					System.out.println ("Error: All servers shut down! Retry later.");
					return;
				}
				String replicationManagerIP = Chord.getAddressForKey(
						command.getKey(), hbList);
				if (command != null) {
					command.setServerAddress(replicationManagerIP);
					command.setStageFrontEndToRM();
					sendAndReceiveCommand(command);
				}
			}
			System.out.println ("Used time: " + ((System.currentTimeMillis() - starttime) / ((double) 1000)) + "s");
		} else if (commandString.toLowerCase().contains("lookup1000")) {
			String[] consistencyValue = commandString.split(" ");
			long starttime = System.currentTimeMillis();
			for (int i = 0; i < 1000; i++) {
				Command command = CommandParser.getCommandFromClient("lookup "
						+ i + " " + consistencyValue[1]);
				ArrayList<HeartBeat> hbList = getHeartBeatList();
				if (hbList == null) {
					System.out.println ("Error: All servers shut down! Retry later.");
					return;
				}
				String replicationManagerIP = Chord.getAddressForKey(
						command.getKey(), hbList);
				if (command != null) {
					command.setServerAddress(replicationManagerIP);
					command.setStageFrontEndToRM();
					sendAndReceiveCommand(command);
				}
			}
			System.out.println ("Used time: " + ((System.currentTimeMillis() - starttime) / ((double) 1000)) + "s");
		} else if (commandString.toLowerCase().contains("lookup100")) {
			String[] consistencyValue = commandString.split(" ");
			long starttime = System.currentTimeMillis();
			for (int i = 0; i < 100; i++) {
				Command command = CommandParser.getCommandFromClient("lookup "
						+ i + " " + consistencyValue[1]);
				ArrayList<HeartBeat> hbList = getHeartBeatList();
				if (hbList == null) {
					System.out.println ("Error: All servers shut down! Retry later.");
					return;
				}
				String replicationManagerIP = Chord.getAddressForKey(
						command.getKey(), hbList);
				if (command != null) {
					command.setServerAddress(replicationManagerIP);
					command.setStageFrontEndToRM();
					sendAndReceiveCommand(command);
				}
			}
			System.out.println ("Used time: " + ((System.currentTimeMillis() - starttime) / ((double) 1000)) + "s");
		} else if (commandString.toLowerCase().contains("lookup10")) {
			String[] consistencyValue = commandString.split(" ");
			long starttime = System.currentTimeMillis();
			for (int i = 0; i < 10; i++) {
				Command command = CommandParser.getCommandFromClient("lookup "
						+ i + " " + consistencyValue[1]);
				ArrayList<HeartBeat> hbList = getHeartBeatList();
				if (hbList == null) {
					System.out.println ("Error: All servers shut down! Retry later.");
					return;
				}
				String replicationManagerIP = Chord.getAddressForKey(
						command.getKey(), hbList);
				if (command != null) {
					command.setServerAddress(replicationManagerIP);
					command.setStageFrontEndToRM();
					sendAndReceiveCommand(command);
				}
			}
			System.out.println ("Used time: " + ((System.currentTimeMillis() - starttime) / ((double) 1000)) + "s");
		} else if (commandString.toLowerCase().equals("time")) {
			showHistogram();
		} else if (commandString.toLowerCase().equals("help")) {
			System.out.println(HELP_STRING);
		} else {
			Command command = CommandParser.getCommandFromClient(commandString);
			if (command == null)
				return;
			ArrayList<HeartBeat> hbList = getHeartBeatList();
			if (hbList == null) {
				System.out.println ("Error: All servers shut down! Retry later.");
				return;
			}
			String replicationManagerIP = Chord.getAddressForKey(
					command.getKey(), hbList);
			command.setServerAddress(replicationManagerIP);
			command.setStageFrontEndToRM();
			sendAndReceiveCommand(command);
		}
	}

	public void showHistogram() {
		Log logger = new Log(Chord.getIpAddress() + ".txt");
		for (Command c : this.receivedCommands) {
			logger.writeLogMessage(c.getTimeDiff().toString());
		}
	}

	public void sendAndReceiveCommand(Command command) {
		UDPClient sendCommand = new UDPClient(command.getServerAddress(),
				CLIENT_PORT);
		sendCommand.send(command);
		System.out.println("Command sent to RM: " + command.getServerAddress());
		Object cmdResponse = receiveCommand.getNextObject();
		// System.out.println("Response received back from RM");
		if (cmdResponse instanceof String) {
			System.out.println(cmdResponse);
		} else {
			Command commandResponse = (Command) cmdResponse;
			if (commandResponse == null)
				System.out.println("Command Failed!");
			else
				System.out.println(commandResponse.showResults());
	
			receivedCommands.add(commandResponse);
		}
	}

	// Optimization: make the function stateful
	private String contactNodeIp = null;
	
	@SuppressWarnings("unchecked")
	public ArrayList<HeartBeat> getHeartBeatList() {
		BufferedReader br = null;
		//String contactNodeIp = "";
		
		if (contactNodeIp != null) {
			UDPClient sendRequest = new UDPClient(contactNodeIp,
					GOSSIP_FE_SERVER_PORT);
			String myIP = Chord.getIpAddress();
			
			//System.out.println ("Connecting to server " + contactNodeIp);
			sendRequest.send(myIP);
			// Hope to receive HeartBeat list during 3 seconds
			ArrayList<HeartBeat> res = (ArrayList<HeartBeat>) receiveTable.getNextObjectWithTimeOut(3);
			
			if (res != null) return res;
			else// Taking from text file
				contactNodeIp = null;
		}
		
		try {
			br = new BufferedReader(new FileReader(IP_FOR_FE));
			//contactNodeIp = br.readLine();
			//br.close();
		} catch (FileNotFoundException e) {
			System.out.println("Please configure " + IP_FOR_FE + " with server IP for connecting!!");
			e.printStackTrace();
		}
		
		try {
			while ((contactNodeIp = br.readLine()) != null) {
				UDPClient sendRequest = new UDPClient(contactNodeIp,
						GOSSIP_FE_SERVER_PORT);
				String myIP = Chord.getIpAddress();
				if (myIP.equals (contactNodeIp)) {
					continue;
				}
				//System.out.println ("Connecting to server " + contactNodeIp);
				sendRequest.send(myIP);
				// Hope to receive HeartBeat list during 3 seconds
				ArrayList<HeartBeat> res = (ArrayList<HeartBeat>) receiveTable.getNextObjectWithTimeOut(3);
				if (res != null) {
					br.close (); return res;
				}
				//System.out.println ("Connection timeout!");
			}
			br.close ();// close the file reader here
			contactNodeIp = null;
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			contactNodeIp = null;
			return null;
		}
	}

	public static void main(String args[]) throws Exception {
		System.out.println 
			("Welcome to Keyvalue Store (Please only run one instance in a single machine)");
		System.out.println(HELP_STRING);
		ClientFrontEnd client = new ClientFrontEnd();
		Thread t = new Thread(client);
		t.start();
	}

}
