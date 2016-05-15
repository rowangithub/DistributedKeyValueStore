KeyValue is a distributed key-value store system.

Configurations of KeyValue.
-----------------------------
1) IpAddressList.txt -- the IP addresses of servers of KeyValue used by server machines.
In this version, the servers are xinu machines from xinu01 to xinu8.
They form a cluster and provide key-value insert/update/delete services to client machines.

2) IpForFE.txt -- the IP addresses of servers of KeyValue used by client machines.
Clients complains "Servers all shut down" when all servers in IpForFE are unavailable;
otherwise they pick one to send key-value insert/update/delete requests to servers of KeyValue.

How to compile KeyValue.
-----------------------------
In a xinu machine, execute
	"javac -d bin src/*/*.java"

How to run KeyValue.
----------------------------
1) To start servers, in xinu machines from xinu01 to xinu8,
execute
	"java -cp ./bin/ keyvalue.ReplicationManager"
In a server machine, you can execute
	show
to display the server's current key-value table (which might be truncated if the table is large).
	
2) To start clients, in any xinu machine from xinu17 to xinu21 (you can run more than one clients),
execute
	"java -cp ./bin/ frontend.ClientFrontEnd"
Then, you can enjoy key-value insert/update/delete services by issuing the following commands in cmd: 
	insert <int key> <value> <one,quorum>
	update <int key> <value> <one,quorum>
	lookup <int key> <one,quorum>
	delete <int key> <one,quorum>
The feedback or response of each command is also displayed so you can know whether your request is successful.
For a "lookup" request, we show the value associated with a key, together with its vector clock.		
		
We also provide batch-insert and batch-lookup commands for you to test KeyValue.
You can insert 1000 elements (0-999) in one time by:
	insert1000 <one,quorum>
You can lookup 1000 elements (0-999) in one time by:
	lookup1000 <one,quorum>



