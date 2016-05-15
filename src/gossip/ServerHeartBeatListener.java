package gossip;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;

public class ServerHeartBeatListener implements Runnable {

	public static final int PORT = 5988;
	private HeartBeatTable table;
	public static final int PACKETSIZE = 1024;

	public ServerHeartBeatListener(HeartBeatTable table) {
		this.table = table;
	}

	@SuppressWarnings("unchecked")
	public void receiveGossip() {
		DatagramSocket serverSocket;
		try {
			serverSocket = new DatagramSocket(PORT);
			byte[] receiveData = new byte[PACKETSIZE];
			while (true) {
				DatagramPacket receivePacket = new DatagramPacket(receiveData,
						receiveData.length);
				serverSocket.receive(receivePacket);
				receivePacket.getData();
				// Get list out of the Received packet
				ObjectInputStream objectStream = new ObjectInputStream(
						new ByteArrayInputStream(receivePacket.getData()));
				Object readObject = objectStream.readObject();
				if (readObject instanceof ArrayList<?>) {
					ArrayList<HeartBeat> hbList = (ArrayList<HeartBeat>) readObject;

					// Update local list with received list
					table.updateTable(hbList);
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		receiveGossip();
	}

}
