package com.aerospike.client.cluster;

import java.util.HashMap;

import com.aerospike.client.IAerospikeClient;

/**
 * This class can be used to perform "interesting" operations on the cluster, such as determining the nodes in the cluster, the partition map, etc.
 * @author timfaulkes
 *
 */
public class ClusterUtilites {
	private final Cluster cluster;
	private final IAerospikeClient client;
	
	public ClusterUtilites(IAerospikeClient client) {
		this.client = client;
		Node[] nodes = client.getNodes();
		this.cluster = nodes.length > 0 ? nodes[0].cluster : null;
	}
	
	public void printInfo() {
		Node[] nodes = client.getNodes();
		for (Node node : nodes) {
			System.out.printf("Node: %s [%s] %s %s\n", node.getName(), node.getAddress().getAddress().toString(), node.getAddress().getHostName(), node.getAddress().getHostString());
		}

		System.out.println(cluster.clusterName == null ? "<unnamed cluster>" : cluster.clusterName);
		System.out.println("----------------------");
		System.out.printf("   Rack Aware: %b\n", cluster.rackAware);
		
		HashMap<String, Partitions> partitionMap = this.cluster.partitionMap;
		for (String namespace : partitionMap.keySet()) {
			System.out.println(namespace);
			System.out.println("----------------------");
			Partitions theseParts = partitionMap.get(namespace);
			System.out.printf("   Strong Consistency: %b\n", theseParts.scMode);
			// TODO: This code will be used to play with the partition map, eg "Find a key which would have a master on node A, replica on node C"
			// which could be used for destructive testing especially in SC mode.
			/*
			System.out.printf("%d\n", theseParts.replicas.length);
			AtomicReferenceArray<Node>[] x = theseParts.replicas;
			AtomicReferenceArray<Node> y = theseParts.replicas[0];
			System.out.println(y.length());
			for (int i = 0; i < y.length(); i++) {
				Node z = y.get(i);
				System.out.printf("%d - %s\n", i, z.getName());
			}
			*/
		}
		System.out.println();
	}
}
