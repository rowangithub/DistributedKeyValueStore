package chord;

import gossip.HeartBeat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import table.Command;

public class Chord {

	public final static int SIZE = 1000000;

	private Chord() {
	}

	private static MessageDigest createSHAInstance() {
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("SHA-1");
			return md;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static int HashFunction(String inputKey) {
		MessageDigest md = Chord.createSHAInstance();
		byte[] mdbytes = md.digest(inputKey.getBytes());

		md.reset();
		int value = 0;
		for (int i = 0; i < 4; i++) {
			value += ((int) mdbytes[i] & 0xffL) << (8 * i);
		}
		value = value < 0 ? -value : value;
		value = value % SIZE;
		return value;
	}

	public static String getAddressForKey(int keyId, List<HeartBeat> serverList) {
		int keyRingId = Chord.getRingID(Integer.toString(keyId));
		String ipAddress = serverList.get(0).getIpAddress();

		for (HeartBeat hb : serverList) {
			if (keyRingId <= hb.getId()) {
				ipAddress = hb.getIpAddress();
				break;
			}
		}
		return ipAddress;
	}

	public static int getRingID(String key) {
		int retVal = HashFunction(key);
		return retVal;
	}

	public static void setCommandServerAddress(Command command, String serverIP) {
		if (command.getKey() != -1) {
			command.setServerAddress(serverIP);
		}
	}

	public static String getIpAddress() {
		String retVal = null;
		try {
			retVal = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return retVal;
	}
}
