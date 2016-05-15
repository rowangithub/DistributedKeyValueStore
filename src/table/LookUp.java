package table;

import java.util.Collection;

public class LookUp extends Command {

	private static final long serialVersionUID = -8053881603680081640L;
	boolean cmdSuccessful;
	
	public LookUp(int key, String receiverAddress, String commandString) {
		super(key, null, receiverAddress, commandString);
	}

	@Override
	public boolean execute(KeyValueTable table) {
		Object value = table.lookUp(keyVal.getKey());
		if (value == null) {
			cmdSuccessful = false;
		} else {
			this.keyVal.set(value);
			cmdSuccessful = true;
		}
		return cmdSuccessful;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String showResults() {
		super.showResults(null);
		if (cmdSuccessful) {
			int key = this.keyVal.getKey();
			Object val = this.keyVal.getValue();
			
			if (val instanceof Collection<?>) {
				StringBuilder sb = new StringBuilder ();
				sb.append ("Key: " + key + " [");
				for (Value v : (Collection<Value>) val) {
					sb.append ("Element: " + v.value + " VectorClock: (" + v.clock + ");");
				}
				sb.append ("]");
				return sb.toString();
			} else {
				return "Key: " + key + " Value: " + val;
			}
		} else
			return "Key " + keyVal.getKey() + " not found in the table";
	}
	@Override
	public String getType() {
		return "LookUp";
	}

	@Override
	public void setVC(VectorClock vc) {
	}
}
