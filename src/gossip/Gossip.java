package gossip;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import log.Log;
import chord.Chord;

public class Gossip {
	public HeartBeatTable table;
	protected ClientHeartBeatSender hbSender;
	private ServerHeartBeatListener hbListener;
	private FETableRespond FETableServer;
	private RMTableRespond RMTableServer;
	public Log logger;
	Thread server;
	Thread client;
	public final static String LEAVE = "leave";
	public final static String JOIN = "join";
	private boolean isContactNode = false;

	public enum State {
		Connected, Disconnected
	};

	private State currentState;

	public Gossip(String sessionName) {
		try {
			this.logger = new Log("machine."
					+ InetAddress.getLocalHost().getHostAddress() + ".log");
			if (sessionName != null) {
				logger.setSessionName(sessionName);
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();

		}
	}

	public void setState(State state) {
		this.currentState = state;
	}

	public boolean isConnected() {
		boolean retVal = this.currentState == State.Connected;
		return retVal;
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

	public void interactWithUser() {
		String showOptions = "Note: You can type 'leave', 'join', 'session <session name>', 'drop <true/false> <percentage int>', '<contact true/false>: ";
		String userInput = getMessageInput(showOptions);
		userInput = userInput.trim().toLowerCase();

		if (userInput.equals(LEAVE) && this.isConnected()) {
			disconnectFromGroup();

		} else if (userInput.equals(JOIN) && !this.isConnected()) {
			connectToGroup();
		} else {
			String[] setArguments = userInput.split(" ");

			setPacketLoss(setArguments);
			setSessionName(setArguments);
			setNumConnections(setArguments);
			setContactNode(setArguments);
		}
	}

	private void setContactNode(String[] setArguments) {
		if (setArguments.length != 2)
			return;
		if (setArguments[0].equals("contact")) {
			if (setArguments[1].equals("true")) {
				System.out.println("This is a contact node!");
				this.isContactNode = true;
			} else {
				System.out.println("This is not a contact node...");
				this.isContactNode = false;
			}
		}

	}

	private void setNumConnections(String[] setArguments) {
		if (setArguments.length != 2)
			return;

		int setNumConnections = 2;
		if (setArguments[0].equals("connections")) {
			setNumConnections = Integer.parseInt(setArguments[1]);
		}
		this.table.setNumConnections(setNumConnections);
	}

	private void setSessionName(String[] setArguments) {
		if (setArguments.length != 2)
			return;

		String sessionName = null;
		if (setArguments[0].equals("session")) {
			sessionName = setArguments[1];
		}
		if (sessionName != null) {
			this.logger.setSessionName(sessionName);
		}

	}

	private void setPacketLoss(String[] setArguments) {
		if (setArguments.length < 2)
			return;

		boolean shouldDropPackets = false;
		int packetLossPercent = 0;

		if (setArguments[0].equals("drop")) {
			if (setArguments[1].equals("true") && setArguments.length == 3) {
				shouldDropPackets = true;
				packetLossPercent = Integer.parseInt(setArguments[2]);

			} else {
				shouldDropPackets = false;
			}
		}
		this.hbSender.setPacketLoss(shouldDropPackets, packetLossPercent);
	}

	private void connectToGroup() {
		System.out.println("Connecting to the group");
		this.setState(State.Connected);
		this.table.reincarnate(this.isContactNode);

		this.hbSender = new ClientHeartBeatSender(table);
		this.client = new Thread(this.hbSender);
		this.client.start();
	}

	public void disconnectFromGroup() {
		System.out.println("Leaving Group");
		this.setState(State.Disconnected);
		this.hbSender.stopClient();
		server.interrupt();
		try {
			this.client.join();

		} catch (InterruptedException e) {
			System.out
					.println("There was an error joining server/client threads");
			e.printStackTrace();
		}
	}

	public void start() {
		String ipAddress = Chord.getIpAddress();
		int id = Chord.getRingID(ipAddress);
		HeartBeat own = new HeartBeat(ipAddress, true, id);
		this.table = new HeartBeatTable(own, this.logger);

		this.hbListener = new ServerHeartBeatListener(table);
		this.hbSender = new ClientHeartBeatSender(table);
		this.FETableServer = new FETableRespond(table);
		this.RMTableServer = new RMTableRespond(table);

		this.server = new Thread(this.hbListener);
		this.server.start();
		this.client = new Thread(this.hbSender);
		this.client.start();

		Thread FETableServerThread = new Thread(this.FETableServer);
		FETableServerThread.start();
		Thread RMTableServerThread = new Thread(this.RMTableServer);
		RMTableServerThread.start();
		this.currentState = State.Connected;
	}

	public String getSuccessorIP(int id) {
		ArrayList<HeartBeat> serverList = table.getCurrentMembersSortedById();
		// int id = table.getOwnId();
		int index = -1;
		// get index
		for (HeartBeat hb : serverList) {
			System.out.println("list:" + hb.getIpAddress());
			index++;
			if (id == hb.getId())
				break;
		}
		index++;
		System.out.println("Index " + index);
		return serverList.get(index % serverList.size()).getIpAddress();
	}

	public int getMemberShipSize() {
		return table.getSize();
	}

	public static void main(String args[]) {
		Gossip gossip = new Gossip("gossip");
		gossip.start();
	}

}
