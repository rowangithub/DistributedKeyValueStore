package table;


public interface CommandInterface {
	public boolean execute(KeyValueTable table);
	public String showResults();
	public String getType();
	public void setVC (VectorClock vc);
}
