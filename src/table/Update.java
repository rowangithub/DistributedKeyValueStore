package table;


public class Update extends Command {

	private static final long serialVersionUID = -1354124611340336825L;
	boolean cmdSuccessful;
	public Update(int key, Object value, String receiverAddress,String commandString) {
		super(key, value, receiverAddress,commandString);

	}
	
	VectorClock vc;
	
	public void setVC (VectorClock vc) {
		this.vc = vc;
	}

	@Override
	public boolean execute(KeyValueTable table) {
		assert (vc != null);
		cmdSuccessful= table.update(keyVal.getKey(), keyVal.getValue(), this.vc);
		return cmdSuccessful;
	}

	@Override
	public String showResults() {
		super.showResults(null);
		if (cmdSuccessful)
			return "Updated Key=" + keyVal.getKey() + " successfully";
		else
			return "Key " + keyVal.getKey() + " not found in the table";
	}
	@Override
	public String getType() {
		return "Update";
	}
}
