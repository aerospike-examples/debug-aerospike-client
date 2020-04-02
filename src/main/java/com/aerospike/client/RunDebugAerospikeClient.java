package com.aerospike.client;

import com.aerospike.client.DebugAerospikeClient.Granularity;
import com.aerospike.client.DebugAerospikeClient.Options;
import com.aerospike.client.cluster.ClusterUtilites;
import com.aerospike.client.cluster.Node;

public class RunDebugAerospikeClient {
	private static void usage() {
		System.err.println("Usage: java -jar <jarfile> -n namespace -s set [-h host]");
		System.exit(-1);
	}
	
	public static void main(String[] args) {
		String host = "127.0.0.1";
		String namespace = null;
		String setName = null;
		
		if (args.length != 6 && args.length != 4) {
			usage();
		}
		for (int i = 0; i < args.length; i+=2) {
			switch (args[i]) {
			case "-h" :
				host = args[i+1];
				break;
			case "-n" :
				namespace = args[i+1];
				break;
			case "-s" :
				setName = args[i+1];
				break;
			default:
				usage();
			}
		}
		if (namespace == null || setName == null) {
			usage();
		}
		
		IAerospikeClient client = new DebugAerospikeClient(host, 3000, new Options(Granularity.EVERY_CALL));
		ClusterUtilites utilites = new ClusterUtilites(client);
		
		// Print some information about the cluster. We will print the partition map in case it's useful
		utilites.printInfo(true);
		
		// Find a particular node which matches a certain criteria
		Node[] nodes = client.getNodes();
		if (nodes.length > 2) {
			System.out.printf("Looking for key with master %s, replica %s\n", nodes[0].getName(), nodes[1].getName());
			String result = utilites.findKeyOnSpecificNodes(nodes[0], nodes[1], namespace, setName);
			if (result == null) {
				System.out.println("   No key found.");
			}
			else {
				System.out.printf("    Key(%s, %s, %s) on partition %d\n", namespace, setName, result, utilites.getPartitionForKey(new Key(namespace, setName, result)));
			}
		}
		
		Key key = new Key(namespace, setName, "testKey");
		client.put(null, key, new Bin("name", "Bunyip"), new Bin("age", 312));
		System.out.println(client.get(null, key));
		client.delete(null, key);
		client.close();
	}
}
