package keyvalue;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import log.Log;
import table.Command;
import table.KeyValue;
import table.KeyValueTable;
import table.LookUp;
import table.VectorClock;
import chord.Chord;

public class RMShowTable implements Runnable {
	private BlockingQueue<Command> commandHistoryReads;
	private BlockingQueue<Command> commandHistoryWrites;
	private KeyValueTable kvTable;
	private RepairReplication rp;

	public RMShowTable(KeyValueTable table, RepairReplication rp) {
		commandHistoryReads = new ArrayBlockingQueue<Command>(10);
		commandHistoryWrites = new ArrayBlockingQueue<Command>(10);
		this.kvTable = table;
		this.rp = rp;
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

	public void addCommand(Command command) {
		if (command instanceof LookUp) {
			if (commandHistoryReads.size() == 10) {
				commandHistoryReads.remove();
			}
			commandHistoryReads.add(command);
		} else {
			if (commandHistoryWrites.size() == 10) {
				commandHistoryWrites.remove();
			}
			commandHistoryWrites.add(command);
		}
	}

	@Override
	public void run() {
		while (true) {
			String input = getMessageInput("Enter Show to show table:");
			if (input.toLowerCase().trim().equals("show")) {
				System.out.println("Current table size :" + kvTable.size());
				for (Command c : commandHistoryReads) {
					System.out.println("Read on Key: " + c.getKey() + " "
							+ c.getType());
				}
				for (Command c : commandHistoryWrites) {
					System.out.println("Write on Key: " + c.getKey()+ " "
							+ c.getType());
				}
			}
		}
	}

	/**
	 * 
	 */
	public void insertWholeFile (String filename, VectorClock vc) {
		InputStream fis;
		BufferedReader br;

		try {
			fis = new FileInputStream(filename);
			br = new BufferedReader(new InputStreamReader(fis,
					Charset.forName("UTF-8")));
			System.out
					.println("populating distributed keyvalue stores with a file ... ");
			String line;
			int tIndex = 0;
			while ((line = br.readLine()) != null) {
				process(line, tIndex, vc);
				tIndex++;
			}
			System.out
					.println("done populatng file!");
			br.close();
			fis.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void process(String line, int tIndex, VectorClock vc) {
		String[] words = line.split("\\t");
		if (words.length < 1)
			return;
		String replicationManagerIP = Chord.getAddressForKey(tIndex,
				rp.getHeartBeatList());
		Log logger = new Log("upload.log");
		KeyValue kv = new KeyValue(tIndex, null);
		if (RepairReplication.checkIfAddressInGroup(kv, replicationManagerIP,
				rp.getHeartBeatList(), logger)) {
			StringBuilder value = new StringBuilder();
			for (int k = 1; k < words.length; k++) {
				value.append("\t" + words[k]);
			}
			kvTable.insert(tIndex, value.toString(), vc);
		}
	}

}
