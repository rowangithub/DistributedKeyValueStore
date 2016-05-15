package table;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class CommandParser {

	private static String getOwnIpaddress() {
		String address = null;
		try {
			address = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return address;
	}

	public static Command getCommandFromClient(String input) throws Exception {
		String address = getOwnIpaddress();
		Command command = null;
		String[] commandStrings = Command.getCommandStrings(input);
		if (commandStrings == null) {
			System.out.println("No Key-Value Command Found\n");
			return null;
		}

		String commandName = Command.getCommandString(commandStrings);
		commandName = commandName.toLowerCase();

		Integer key = Command.getCommandKey(commandStrings);
		String value = Command.getCommandValue(commandStrings);
		int level = Command.getConsistencyLevel(commandStrings);

		if (commandName.equals("lookup")) {
			command = new LookUp(key, address, input);

		} else if (commandName.equals("delete")) {

			command = new Delete(key, address, input);

		} else if (commandName.equals("insert")) {
			if (value != null) {
				command = new Insert(key, value, address, input);
			} else {
				System.out.println("error!");
			}

		} else if (commandName.equals("update")) {
			if (value != null) {
				command = new Update(key, value, address, input);
			} else {
				System.out.println("error!");
			}
		}

		if (command == null)
			System.out.println("There was an error parsing the command");
		else
			command.setConsistencyLevel(level);

		return command;
	}
}
