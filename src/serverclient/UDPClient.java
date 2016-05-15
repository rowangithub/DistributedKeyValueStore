package serverclient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class UDPClient{

	private String serverAddress;
	private int serverPort;
	private static Integer PACKET_SIZE = 1024;
	/**
	 * Constructor
	 * 
	 * @param serverAddress
	 *            - Who the client will be talking to
	 */
	public UDPClient(String serverAddress, int serverPort) {
		this.serverAddress = serverAddress;
		this.serverPort = serverPort;
	}
	public UDPClient(String serverAddress, int serverPort,int packetSize) {
		this.serverAddress = serverAddress;
		this.serverPort = serverPort;
		this.PACKET_SIZE = packetSize;
	}
	
	/**
	 * Sends an object over the network
	 * 
	 * @param object
	 *            to send
	 * @return false on known failure true if no known failures
	 */
	public boolean send(Object object) {
		DatagramSocket clientSocket;
		byte[] sendData = new byte[PACKET_SIZE];

		try {
			InetAddress ipAddress = InetAddress.getByName(serverAddress); 
			clientSocket = new DatagramSocket();
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
			objectStream.writeObject(object);
			sendData = byteStream.toByteArray();
			// send the packet
			DatagramPacket sendPacket = new DatagramPacket(sendData,
					sendData.length, ipAddress, serverPort);
			clientSocket.send(sendPacket);
			clientSocket.close();
		} catch (SocketException e) {
			e.printStackTrace();
			return false;
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
}
