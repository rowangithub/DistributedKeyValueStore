package serverclient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class UDPServer implements Runnable {

	// private List<Object> objectList;
	private DatagramSocket serverSocket;
	private byte[] receiveData;
	private static Integer PACKET_SIZE = 1024;
	private BlockingQueue<Object> objectList;

	public UDPServer(int serverPort) {
		try {
			serverSocket = new DatagramSocket(serverPort);
			objectList = new ArrayBlockingQueue<Object>(262144);
			receiveData = new byte[PACKET_SIZE];
		} catch (SocketException e) {
			e.printStackTrace();
		}
	}
	public UDPServer(int serverPort,int packetSize) {
		try {
			this.PACKET_SIZE = packetSize;
			serverSocket = new DatagramSocket(serverPort);
			objectList = new ArrayBlockingQueue<Object>(262144);
			receiveData = new byte[PACKET_SIZE];
			
		} catch (SocketException e) {
			e.printStackTrace();
		}
	}
	
	public Object getNextObject() {
		Object retVal = null;
		try {
			retVal = objectList.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return retVal;
	}
	
	public Object getNextObjectWithTimeOut (int sec) {
		Object retVal = null;
		try {
			retVal = objectList.poll(sec, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return retVal;
	}

	private void waitForRequests() {
		try {
			DatagramPacket receivePacket = new DatagramPacket(receiveData,
					receiveData.length);
			serverSocket.receive(receivePacket);
			receivePacket.getData();

			// Get list out of the Received packet
			ObjectInputStream objectStream = new ObjectInputStream(
					new ByteArrayInputStream(receivePacket.getData()));

			Object readObject = objectStream.readObject();
			objectList.add(readObject);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void run() {
		while (true)
			waitForRequests();
	}

}
