package table;


public class Delete extends Command {

	private static final long serialVersionUID = 2473056506143986432L;
	boolean cmdSuccessful;
	public Delete(int key, String receiverAddress, String commandString) {
		super(key, null, receiverAddress,commandString);
	}

	@Override
	public boolean execute(KeyValueTable table) {
		cmdSuccessful =table.delete(keyVal.getKey());
		return cmdSuccessful;
	}

	@Override
	public String showResults() {
		super.showResults(null);
		if (cmdSuccessful)
			return "Key " + keyVal.getKey() + " was deleted.";
		else
			return "Key " + keyVal.getKey() + " not found in the table";
	}

	@Override
	public String getType() {
		return "Delete";
	}

	@Override
	public void setVC(VectorClock vc) {
	}

}
