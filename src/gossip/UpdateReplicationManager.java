package gossip;


import java.util.ArrayList;

import keyvalue.RepairReplication;

import serverclient.UDPClient;

public class UpdateReplicationManager implements Runnable {

	ArrayList<HeartBeat> rp;
	public UpdateReplicationManager(ArrayList<HeartBeat> rp){
		this.rp = rp;
	}
	
	@Override
	public void run() {
		UDPClient client = new UDPClient("localhost", RepairReplication.PORT_NUMBER);
		client.send(rp);
	}

}
