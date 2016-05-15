package table;

import java.io.Serializable;

import chord.Chord;

public abstract class Command implements Serializable, CommandInterface {

	public enum ConsistencyLevel {
		one, quorum, all
	};
	
	private static int COMMAND_COUNT = 0;
	private String commandID;
	private int stage = 0;
	private ConsistencyLevel currentConsistencyLevel = ConsistencyLevel.one;
	/**
	 * 
	 */
	private static final long serialVersionUID = -902590735488224065L;

	protected KeyValue keyVal;
	protected String frontEndClientAddress; // client computer who will request
											// the record
	protected String primaryRMAddress;// the coordinator that sends stage
	protected String serverAddress; // server computer who will have the record
	protected int serverPosrt;
	protected Long startTime;
	protected Long endTime;
	protected String commandString;
	private boolean stageOneStatus = false;
	private boolean stageTwoStatus = false;

	public Command(String receiverAddress, String commandString) {
		this.frontEndClientAddress = receiverAddress;
		this.commandID = receiverAddress + COMMAND_COUNT++;
		startTime = System.currentTimeMillis();
		this.commandString = commandString;
	}

	public Command(int key, Object value, String receiverAddress,
			String commandString) {
		this.keyVal = new KeyValue(key, value);
		this.frontEndClientAddress = receiverAddress;
		this.commandID = receiverAddress + COMMAND_COUNT++;
		startTime = System.currentTimeMillis();
		this.commandString = commandString;
	}

	public String getCommandID() {
		return this.commandID;
	}

	public void setServerAddress(String serverAddress) {
		this.serverAddress = serverAddress;

	}

	public String getServerAddress() {
		return serverAddress;

	}

	public int getKey() {
		return this.keyVal.getKey();
	}

	public Long getTimeDiff() {
		return endTime - startTime;
	}

	public static String[] getCommandStrings(String commandString) {
		String[] commandStrings = commandString.split(" ");
		int commandLength = commandStrings.length;
		boolean retVal = commandLength <= 4;
		if (retVal) {
			return commandStrings;
		} else {
			return null;
		}
	}

	public static String getCommandString(String[] commandStrings) {
		if (commandStrings == null) {
			return null;
		}
		String commandName = commandStrings[0];
		return commandName;
	}

	public static int getCommandKey(String[] commandStrings) {
		if (commandStrings == null) {
			return -1;
		}
		int commandKey = -1;
		if (commandStrings.length >= 2 && commandStrings[1] != null)
			try{
			commandKey = Integer.parseInt(commandStrings[1]);
			}catch (NumberFormatException e){
				//this is a movie
			}
		return commandKey;
	}

	public static String getCommandValue(String[] commandStrings) {
		if (commandStrings == null || commandStrings.length <= 3) {
			return null;
		}
		String commandValue = commandStrings[2];
		return commandValue;
	}

	public void showResults(String string) {
		this.endTime = System.currentTimeMillis();
	}

	public String getOriginalCommandString() {
		return this.commandString;
	}

	public int getStage() {
		return this.stage;
	}

	public void setStageFrontEndToRM() {
		this.stage = 1;
		this.frontEndClientAddress = Chord.getIpAddress();
	}

	public void setStageRMtoRM() {
		this.stage = 2;
		setRMAddress();
	}

	public void setStageBulkToRM() {
		this.stage = 0;
		setRMAddress();
	}

	public void setRepairStage() {
		this.stage = -1;
		setRMAddress();
	}

	public String getClientAddress() {
		return this.frontEndClientAddress;
	}

	public String getRMAddress() {
		return this.primaryRMAddress;
	}

	/**
	 * Sets the current ip as the RM address
	 */
	public void setRMAddress() {
		this.primaryRMAddress = Chord.getIpAddress();
	}

	public ConsistencyLevel getCurrentConsistencyLevel() {
		return currentConsistencyLevel;
	}

	public boolean getStageOneStatus() {
		return this.stageOneStatus;
	}

	public void setStageOneStatus(boolean status) {
		this.stageOneStatus = status;
	}

	public boolean getStageTwoStatus() {
		return this.stageTwoStatus;
	}

	public void setStageTwoStatus(boolean status) {
		this.stageTwoStatus = status;
	}

	public void setConsistencyLevel(int level) {
		if (level == 0) {
			this.currentConsistencyLevel = ConsistencyLevel.one;
		} else if (level == 1) {
			this.currentConsistencyLevel = ConsistencyLevel.quorum;
		} else {
			this.currentConsistencyLevel = ConsistencyLevel.all;
		}
	}

	public KeyValue getKeyValue() {
		return this.keyVal;
	}

	public static int getConsistencyLevel(String[] commandStrings) {
		int length = commandStrings.length;
		if (length <= 0) {
			return 0;
		}
		String level = commandStrings[length - 1];
		level = level.toUpperCase();
		int retVal;
		if (level.equals("ONE")) {
			retVal = 0;
		} else if (level.equals("QUORUM")) {
			retVal = 1;
		} else {
			retVal = 2;
		}
		return retVal;
	}
	public String getType(){
		return null;
	};
}
