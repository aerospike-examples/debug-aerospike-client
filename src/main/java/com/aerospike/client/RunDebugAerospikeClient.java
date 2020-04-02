package com.aerospike.client;

import com.aerospike.client.DebugAerospikeClient.Granularity;
import com.aerospike.client.DebugAerospikeClient.Options;
import com.aerospike.client.cluster.ClusterUtilites;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;

public class RunDebugAerospikeClient {
	private static void usage() {
		System.out.println("Usage: java -jar <jarfile> <options>");
		System.out.println("Options are:");
		System.out.println("   -h, --host <host>            The seed node to connect to the cluster (default: 127.0.0.1)");
		System.out.println("   -p, --port <port>            The port used to connect to the cluster (default: 3000)");
		System.out.println("   -U, --user <name>            The user to log onto the cluster as (default: none)");
		System.out.println("   -P, --password <password>    The password of the user (default: none)");
		System.out.println("   -n, --namespace <name>       The namespace to use (required)");
		System.out.println("   -s, --set <name>             The set to use (required)");
		System.out.println("   -m, --map <boolean>          Whether to show the partition map or not (default: false)");
		System.out.println("   -l, --length <len>           The maximum length of the partition map to show. 0 is unlimited. Only useful with --map option (default: 80)");
		System.out.println("   -f, --find <master,replica>  Find a key in the passed namespace and set such that the key has the designated master and replica nodes");
		System.out.println("                                Master and Replica nodes can be designated either by node name, or specify 'any' for any 2 random nodes");
		System.out.println("                                This option only works with 2 or more nodes");
		System.out.println("   -t, --test <boolean>         Perform a put, get and delete in the namespace/set with log level EVERY_CALL (default: true)");
		System.exit(-1);
	}
	
	private static Node findNode(String nodeName, Node[] nodes) {
		nodeName = nodeName.trim();
		for (Node node : nodes) {
			if (node.getName().equals(nodeName)) {
				return node;
			}
		}
		System.out.printf("There is no node in the cluster with the name '%s'\n", nodeName);
		return null;
	}
	
	public static void main(String[] args) {
		String host = "127.0.0.1";
		int port = 3000;
		String namespace = null;
		String setName = null;
		int partMapLen = 80;
		boolean showPartMap = false;
		ClientPolicy clientPolicy = new ClientPolicy();
		String nodesToFind = null;
		boolean doInsert = true;

		for (int i = 0; i < args.length-1; i+=2) {
			switch (args[i]) {
			case "-h" :
			case "-host":
				host = args[i+1];
				break;
			case "-n" :
			case "--namespace":
				namespace = args[i+1];
				break;
			case "-s" :
			case "--set":
				setName = args[i+1];
				break;
			case "-p":
			case "--port":
				port = Integer.valueOf(args[i+1]);
				break;
			case "-m":
			case "--map":
				showPartMap = Boolean.valueOf(args[i+1]);
				break;
			case "-l":
			case "--length":
				partMapLen = Integer.valueOf(args[i+1]);
				break;
			case "-U":
			case "--user":
				clientPolicy.user = args[i+1];
				break;
			case "-P":
			case "--password":
				clientPolicy.password = args[i+1];
				break;
			case "-f":
			case "--find":
				nodesToFind = args[i+1];
				break;
			case "-t":
			case "-test":
				doInsert = Boolean.valueOf(args[i+1]);
				break;
			case "-?":
			case "--usage":
				usage();
				break;
			default:
				System.out.printf("Unrecognized option %s\n", args[i]);
				usage();
			}
		}
		
		if (namespace == null || setName == null) {
			usage();
		}
		
		IAerospikeClient client = new DebugAerospikeClient(clientPolicy, host, port, new Options(Granularity.EVERY_CALL));
		ClusterUtilites utilites = new ClusterUtilites(client);
		
		// Print some information about the cluster. 
		utilites.printInfo(showPartMap, partMapLen);
		
		if (nodesToFind != null) {
			// Find a particular node which matches a certain criteria
			Node[] nodes = client.getNodes();
			if (nodes.length > 2) {
				Node masterNode = null, replicaNode = null;
				if (nodesToFind.trim().compareToIgnoreCase("any") == 0) {
					masterNode = nodes[0];
					replicaNode = nodes[1];
				}
				else {
					int index = nodesToFind.indexOf(',');
					if (index > 0) {
						masterNode = findNode(nodesToFind.substring(0, index), nodes);
						replicaNode = findNode(nodesToFind.substring(index+1), nodes);
					}
					else {
						System.out.printf("Incorrect node designation in --find argument. You specified '%s', expected format is 'any' or 'nodeName1,nodeName2'\n", nodesToFind);
					}
				}
				if (masterNode != null && replicaNode != null) {
					System.out.printf("Looking for key with master %s, replica %s\n", masterNode.getName(), replicaNode.getName());
					String result = utilites.findKeyOnSpecificNodes(nodes[0], nodes[1], namespace, setName);
					if (result == null) {
						System.out.println("   No key found.");
					}
					else {
						System.out.printf("    Key(%s, %s, %s) on partition %d\n", namespace, setName, result, utilites.getPartitionForKey(new Key(namespace, setName, result)));
					}
				}
			}
			else {
				System.out.printf("%d node(s) in the cluster, not looking for key\n");
			}
		}		

		if (doInsert) {
			Key key = new Key(namespace, setName, "testKey");
			client.put(null, key, new Bin("name", "Bunyip"), new Bin("age", 312));
			System.out.println(client.get(null, key));
			client.delete(null, key);
		}
		client.close();
	}
}
