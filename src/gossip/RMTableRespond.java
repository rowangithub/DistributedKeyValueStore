package gossip;

import serverclient.UDPClient;
import serverclient.UDPServer;

public class RMTableRespond implements Runnable {

	public static final int GOSSIP_RM_SERVER_PORT = 5996;
	public static final int GOSSIP_RM_CLIENT_PORT = 5995;
	private HeartBeatTable table;
	UDPServer serverRequestListener;

	public RMTableRespond(HeartBeatTable table) {
		this.table = table;
		serverRequestListener = new UDPServer(GOSSIP_RM_SERVER_PORT);
		Thread sendServer = new Thread(serverRequestListener);
		sendServer.start();
	}

	@Override
	public void run() {
		while (true)
			tableRequestListener();
	}

	private void tableRequestListener() {
		String requestString = (String) serverRequestListener.getNextObject();
		UDPClient tableSender = new UDPClient(requestString,
				GOSSIP_RM_CLIENT_PORT);
		tableSender.send(table.getCurrentMembersSortedById());
	}
}
