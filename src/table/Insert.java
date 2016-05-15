package table;


public class Insert extends Command {

	private static final long serialVersionUID = 1L;

	boolean cmdSuccessful;
	
	VectorClock vc;
	
	public void setVC (VectorClock vc) {
		this.vc = vc;
	}
	
	public Insert(int key, Object value, String receiverAddress,String commandString) {
		super(key, value, receiverAddress,commandString);
	}

	@Override
	public boolean execute(KeyValueTable table) {
		cmdSuccessful= table.insert(keyVal.getKey(), keyVal.getValue(), vc);
		return cmdSuccessful;
	}

	@Override
	public String showResults() {
		super.showResults(null);
		if (cmdSuccessful)
			return "Key " + keyVal.getKey() + " was inserted";
		else
			return "Key " + keyVal.getKey()
					+ " already exists. Insert command failed";
	}
	@Override
	public String getType() {
		return "Insert";
	}
}
