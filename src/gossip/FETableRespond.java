package gossip;

import serverclient.UDPClient;
import serverclient.UDPServer;

public class FETableRespond implements Runnable {

	public static final int GOSSIP_FE_SERVER_PORT = 5928;
	public static final int GOSSIP_FE_CLIENT_PORT = 5927;
	private HeartBeatTable table;
	UDPServer clientRequestListener;

	public FETableRespond(HeartBeatTable table) {
		this.table = table;
		clientRequestListener = new UDPServer(GOSSIP_FE_SERVER_PORT);
		Thread sendClient = new Thread(clientRequestListener);
		sendClient.start();
	}

	@Override
	public void run() {
		while (true)
			tableRequestListener();
	}

	private void tableRequestListener() {
		String requestString = (String) clientRequestListener.getNextObject();
		UDPClient tableSender = new UDPClient(requestString,
				GOSSIP_FE_CLIENT_PORT);
		tableSender.send(table.getCurrentMembersSortedById());
	}
}
