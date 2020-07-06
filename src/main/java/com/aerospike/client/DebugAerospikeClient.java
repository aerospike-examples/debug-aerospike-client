/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException.InvalidNode;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.IndexListener;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

public class DebugAerospikeClient implements IAerospikeClient {

	private static enum PutOperation {
		ADD("Add"),
		APPEND("Append"),
		DELETE("Delete"),	// TODO: Should this really be counted as a put, especially if non-durable?
		OPERATE("Operate"),
		PREPEND("Prepend"),
		PUT("Put"),
		TOUCH("Touch");
		
		private String operationName;
		private PutOperation(String name) {
			this.operationName = name;
		}
		public String getName() {
			return operationName;
		}
	}

	public static enum Granularity {
		EVERY_CALL(0),
		EVERY_SECOND(1),
		EVERY_10_SECONDS(10),
		EVERY_MINUTE(60),
		NEVER(0);
		
		private int frequency;
		private Granularity(int frequency) {
			this.frequency = frequency;
		}
		
		public int getFrequency() {
			return this.frequency;
		}
	}
	
	public static class Options {
		private Granularity batchLogging = Granularity.NEVER;
		private Granularity getLogging = Granularity.NEVER;
		private Granularity putLogging = Granularity.NEVER;
		private PrintStream stream = System.out;
		private int bitShift = 1;
		private int numColumns = 7;
		private boolean useUs = false;
		private LatencyManager batchLatencyManager = null; 
		private LatencyManager getLatencyManager = null; 
		private LatencyManager putLatencyManager = null; 
		
		public Options() {
		}

		/**
		 * Specify the options for the logging. Note that operate() calls fall into the putLogging logger options
		 * @param putLogging
		 * @param getLogging
		 * @param batchLogging
		 * @param stream
		 */
		public Options(Granularity putLogging, Granularity getLogging, Granularity batchLogging, PrintStream stream) {
			this.batchLogging = batchLogging;
			this.getLogging = getLogging;
			this.putLogging = putLogging;
			if (stream != null) {
				this.stream = stream;
			}
		}
		public Options(Granularity putLogging, Granularity getLogging, Granularity batchLogging) {
			this(putLogging, getLogging, batchLogging, null);
		}

		public Options(Granularity logging) {
			this(logging, logging, logging);
		}
		
		public Options(Granularity logging, LatencyManager latencyManager) {
			this(logging, logging, logging);
			this.setLatencyManager(latencyManager);
		}
		
		public void setLatencyManager(LatencyManager latencyManager) {
			this.batchLatencyManager = latencyManager.duplicate();
			this.getLatencyManager = latencyManager.duplicate();
			this.putLatencyManager = latencyManager.duplicate();
		}

		public PrintStream getStream() {
			return stream;
		}
		public void setStream(PrintStream stream) {
			this.stream = stream;
		}
		public Granularity getBatchLogging() {
			return batchLogging;
		}

		public void setBatchLogging(Granularity batchLogging) {
			this.batchLogging = batchLogging;
		}

		public Granularity getGetLogging() {
			return getLogging;
		}

		public void setGetLogging(Granularity getLogging) {
			this.getLogging = getLogging;
		}

		public Granularity getPutLogging() {
			return putLogging;
		}

		public void setPutLogging(Granularity putLogging) {
			this.putLogging = putLogging;
		}
		
		public int getBitShift() {
			return bitShift;
		}
		public int getNumColumns() {
			return numColumns;
		}
		public boolean isUseUs() {
			return useUs;
		}
	}
	

	private IAerospikeClient delegate;
	private String closeStackTrace = null;
	private Options options = null;
	private Thread statsPrinter = null;
	
	//-------------------------------------------------------
	// Constructors
	//-------------------------------------------------------

	/**
	 * Initialize Aerospike client.
	 * If the host connection succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * If the connection succeeds, the client is ready to process database requests.
	 * If the connection fails, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param hostname				host name
	 * @param port					host port
	 * @throws AerospikeException	if host connection fails
	 */
	public DebugAerospikeClient(String hostname, int port) throws AerospikeException {
		delegate = new AerospikeClient(hostname, port);
	}

	/**
	 * Initialize Aerospike client.
	 * The client policy is used to set defaults and size internal data structures.
	 * If the host connection succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * If the connection succeeds, the client is ready to process database requests.
	 * If the connection fails and the policy's failOnInvalidHosts is true, a connection 
	 * exception will be thrown. Otherwise, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param policy				client configuration parameters, pass in null for defaults
	 * @param hostname				host name
	 * @param port					host port
	 * @throws AerospikeException	if host connection fails
	 */
	public DebugAerospikeClient(ClientPolicy policy, String hostname, int port) throws AerospikeException {
		delegate = new AerospikeClient(policy, hostname, port);
	}

	/**
	 * Initialize Aerospike client with suitable hosts to seed the cluster map.
	 * The client policy is used to set defaults and size internal data structures.
	 * For each host connection that succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * In most cases, only one host is necessary to seed the cluster. The remaining hosts 
	 * are added as future seeds in case of a complete network failure.
	 * <p>
	 * If one connection succeeds, the client is ready to process database requests.
	 * If all connections fail and the policy's failIfNotConnected is true, a connection 
	 * exception will be thrown. Otherwise, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param policy				client configuration parameters, pass in null for defaults
	 * @param hosts					array of potential hosts to seed the cluster
	 * @throws AerospikeException	if all host connections fail
	 */
	public DebugAerospikeClient(ClientPolicy policy, Options options, Host... hosts) throws AerospikeException {
		this.delegate = new AerospikeClient(policy, hosts);
		this.setOptions(options);
	}

	/**
	 * Initialize Aerospike client.
	 * If the host connection succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * If the connection succeeds, the client is ready to process database requests.
	 * If the connection fails, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param hostname				host name
	 * @param port					host port
	 * @throws AerospikeException	if host connection fails
	 */
	public DebugAerospikeClient(String hostname, int port, Options options) throws AerospikeException {
		this.delegate = new AerospikeClient(hostname, port);
		this.setOptions(options);
	}

	/**
	 * Initialize Aerospike client.
	 * The client policy is used to set defaults and size internal data structures.
	 * If the host connection succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * If the connection succeeds, the client is ready to process database requests.
	 * If the connection fails and the policy's failOnInvalidHosts is true, a connection 
	 * exception will be thrown. Otherwise, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param policy				client configuration parameters, pass in null for defaults
	 * @param hostname				host name
	 * @param port					host port
	 * @throws AerospikeException	if host connection fails
	 */
	public DebugAerospikeClient(ClientPolicy policy, String hostname, int port, Options options) throws AerospikeException {
		this.delegate = new AerospikeClient(policy, hostname, port);
		this.setOptions(options);
	}

	/**
	 * Initialize Aerospike client with suitable hosts to seed the cluster map.
	 * The client policy is used to set defaults and size internal data structures.
	 * For each host connection that succeeds, the client will:
	 * <p>
	 * - Add host to the cluster map <br>
	 * - Request host's list of other nodes in cluster <br>
	 * - Add these nodes to cluster map <br>
	 * <p>
	 * In most cases, only one host is necessary to seed the cluster. The remaining hosts 
	 * are added as future seeds in case of a complete network failure.
	 * <p>
	 * If one connection succeeds, the client is ready to process database requests.
	 * If all connections fail and the policy's failIfNotConnected is true, a connection 
	 * exception will be thrown. Otherwise, the cluster will remain in a disconnected state
	 * until the server is activated.
	 * 
	 * @param policy				client configuration parameters, pass in null for defaults
	 * @param hosts					array of potential hosts to seed the cluster
	 * @throws AerospikeException	if all host connections fail
	 */
	public DebugAerospikeClient(ClientPolicy policy, Host... hosts) throws AerospikeException {
		delegate = new AerospikeClient(policy, hosts);
	}

	// -----------------------------
	// Logging information
	// -----------------------------
	/**
	 * This thread polls the loggers at regular intervals and dumps the stats out the passed print stream.
	 * @author timfaulkes
	 *
	 */
	private class Logger implements Runnable {
		private volatile boolean started = false;
		private int findMinimumDelayTime() {
			int minTime = options.getBatchLogging().frequency;
			if (options.getPutLogging().frequency > 0) {
				if (minTime == 0) {
					minTime = options.getPutLogging().frequency;
				}
				else {
					minTime = Math.min(minTime, options.getPutLogging().frequency);
				}
			}
			if (options.getGetLogging().frequency > 0) {
				if (minTime == 0) {
					minTime = options.getGetLogging().frequency;
				}
				else {
					minTime = Math.min(minTime, options.getGetLogging().frequency);
				}
			}
			return minTime;
		}
		
		@Override
		public void run() {
			int delayTime = findMinimumDelayTime() * 1000;
			
			// We need a latency manager for printing the header.
			LatencyManager lm = (options.getLatencyManager == null) ? (options.putLatencyManager == null) ? options.batchLatencyManager : options.putLatencyManager : options.getLatencyManager;
			started = true;
			if (lm == null) {
				// Should not happen
				System.err.println("Cannot find a non-null latency manager");
				return;
			}
			while (true) {
				try {
					Thread.sleep(delayTime);
				}
				catch (InterruptedException ie) {
					// This can happen with a clean shutdown of the AerospikeClient
//					throw new RuntimeException(ie);
					return;
				}
				boolean bufferOutput = true;
				ByteArrayOutputStream baos = null;
				PrintStream ps;
				if (bufferOutput) {
					baos = new ByteArrayOutputStream();
					ps = new PrintStream(baos);
				}
				else {
					ps = options.stream;
				}
				
				lm.printHeader(ps);
				if (options.getLatencyManager != null) {
					options.getLatencyManager.printResults(ps, "gets");
				}
				if (options.putLatencyManager != null) {
					options.putLatencyManager.printResults(ps, "puts");
				}
				if (options.batchLatencyManager != null) {
					options.batchLatencyManager.printResults(ps, "batch");
				}
				
				if (bufferOutput) {
					options.stream.print(baos.toString());
					try {
						baos.close();
					} catch (IOException e) {
					}
				}
			}
		}
	}

	private void setOptions(Options options) {
		this.options = options;
		if (options != null) {
			if (options.getBatchLogging() == null) {
				options.batchLogging = Granularity.NEVER;
			}
			if (options.getGetLogging() == null) {
				options.getLogging = Granularity.NEVER; 
			}
			if (options.getPutLogging() == null) {
				options.putLogging = Granularity.NEVER;
			}
			int maxFrequency = Math.max(options.batchLogging.frequency, Math.max(options.getLogging.frequency, options.putLogging.frequency));
			if (maxFrequency > 0) {
				if (options.batchLogging.frequency > 0 && options.batchLatencyManager == null) {
					options.batchLatencyManager = new ExponentialLatencyManager(options.numColumns, options.bitShift, options.useUs);
				}
				if (options.getLogging.frequency > 0 && options.getLatencyManager == null) {
					options.getLatencyManager = new ExponentialLatencyManager(options.numColumns, options.bitShift, options.useUs);
				}
				if (options.putLogging.frequency > 0 && options.putLatencyManager == null) {
					options.putLatencyManager = new ExponentialLatencyManager(options.numColumns, options.bitShift, options.useUs);
				}
				Logger logger = new Logger();
				statsPrinter = new Thread(logger);
				statsPrinter.setDaemon(true);
				statsPrinter.start();
				while (!logger.started) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
					}
				}
			}
		}
	}

	/**
	 * Get the current stack trace as a string, removing this method from the trace
	 * 
	 * @return the stack trace
	 */
	private String getStackTrace() {
		Exception e = new Exception();
		StackTraceElement[] elements = e.getStackTrace();
		e.setStackTrace(Arrays.copyOfRange(elements, 1, elements.length));
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}
	
	private long startBatchTime() {
		return (options != null && options.getBatchLogging() != Granularity.NEVER) ? System.nanoTime() : 0;
	}
	
	private void endBatchTime(long startTime, Key[] keys, Record[] records, AerospikeException ae) {
		if (startTime > 0) {
			long totalTimeUs = (System.nanoTime() - startTime)/1000;
			logBatchTimes(totalTimeUs, keys, records, ae);
		}
	}

	private void logBatchTimes(long timeInUs, Key[] keys, Record[] records, AerospikeException ae) {
		int total = keys.length;
		int successful = 0;
		for (int i = 0; records != null && i < total; i++) {
			successful += records[i] != null ? 1 : 0;
		}
		if (options.getBatchLogging() == Granularity.EVERY_CALL) {
			if (ae != null) {
				options.stream.printf("Batch: [%d keys] threw %s (%d:%s) in %,.3fms\n", keys.length, ae.getClass(), ae.getResultCode(), ae.getMessage(), timeInUs/1000.0);
			}
			else {
				options.stream.printf("Batch: [%d/%d keys] took %,.3fms\n", successful, keys.length, timeInUs/1000.0);
			}
		}
		else if (options.getBatchLogging()  != Granularity.NEVER) {
			options.batchLatencyManager.add(timeInUs, keys.length, successful);
		}
	}
	
	private long startPutTime() {
		return (options != null && options.getPutLogging() != Granularity.NEVER) ? System.nanoTime() : 0;
	}
	
	private void endPutTime(long startTime, Key key, PutOperation operation, AerospikeException ae) {
		if (startTime > 0) {
			long totalTimeUs = (System.nanoTime() - startTime)/1000;
			logPutTimes(totalTimeUs, key, operation, ae);
		}
	}

	private AtomicLong thresholdCount = new AtomicLong(0); 
	private void logPutTimes(long timeInUs, Key key, PutOperation operation, AerospikeException ae) {
		if (options.getPutLogging() == Granularity.EVERY_CALL) {
			if (ae != null) {
				options.stream.printf("%s: [%s] threw %s (%d:%s) in %,.3fms\n", operation.getName(), key.toString(), ae.getClass(), ae.getResultCode(), ae.getMessage(), timeInUs/1000.0);
			}
			else {
				options.stream.printf("%s: [%s] took %,.3fms\n", operation.getName(), key.toString(), timeInUs/1000.0);
			}
		}
		else if (options.getPutLogging()  != Granularity.NEVER) {
			options.putLatencyManager.add(timeInUs, 0, ae != null ? 0 : 1);
		}
//		if (timeInUs > 30000) {
//			if (thresholdCount.incrementAndGet() > 100) {
//				options.stream.printf("Time Violation: call took %,.3fms\n%s\n", timeInUs/1000.0, getStackTrace());
//			}
//		}
	}
	
	private long startGetTime() {
		return (options != null && options.getGetLogging() != Granularity.NEVER) ? System.nanoTime() : 0;
	}
	
	private void endGetTime(long startTime, Key key, Record result, AerospikeException ae) {
		if (startTime > 0) {
			long totalTimeUs = (System.nanoTime() - startTime)/1000;
			logGetTimes(totalTimeUs, key, result, ae);
		}
	}

	private void logGetTimes(long timeInUs, Key key, Record record, AerospikeException ae) {
		if (options.getGetLogging() == Granularity.EVERY_CALL) {
			if (ae != null) {
				options.stream.printf("Get: [%s] threw %s (%d:%s) in %,.3fms\n", key.toString(), ae.getClass(), ae.getResultCode(), ae.getMessage(), timeInUs/1000.0);
			}
			else {
				options.stream.printf("Get: [%s] took %,.3fms, record %sfound\n", key.toString(), timeInUs/1000.0, record == null ? "not " : "");
			}
		}
		else if (options.getGetLogging()  != Granularity.NEVER) {
			options.getLatencyManager.add(timeInUs, 0, ae != null ? 0 : record == null ? 0 : 1);
		}
	}
	

	
	@Override
	public void close() {
		if (this.closeStackTrace != null) {
			options.stream.printf("*** Close called multiple times. First:\n%s\nThis one:\n%s\n", this.closeStackTrace, getStackTrace());
		}
		this.closeStackTrace = getStackTrace();
		if (statsPrinter != null) {
			statsPrinter.interrupt();
		}
		delegate.close();
	}

	public Policy getReadPolicyDefault() {
		return delegate.getReadPolicyDefault();
	}

	public WritePolicy getWritePolicyDefault() {
		return delegate.getWritePolicyDefault();
	}

	public ScanPolicy getScanPolicyDefault() {
		return delegate.getScanPolicyDefault();
	}

	public QueryPolicy getQueryPolicyDefault() {
		return delegate.getQueryPolicyDefault();
	}

	public BatchPolicy getBatchPolicyDefault() {
		return delegate.getBatchPolicyDefault();
	}

	public InfoPolicy getInfoPolicyDefault() {
		return delegate.getInfoPolicyDefault();
	}

	public boolean isConnected() {
		return delegate.isConnected();
	}

	public Node[] getNodes() {
		return delegate.getNodes();
	}

	public List<String> getNodeNames() {
		return delegate.getNodeNames();
	}

	public Node getNode(String nodeName) throws InvalidNode {
		return delegate.getNode(nodeName);
	}

	public ClusterStats getClusterStats() {
		return delegate.getClusterStats();
	}

	public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		long now = startPutTime();
		try {
			delegate.put(policy, key, bins);
			endPutTime(now, key, PutOperation.PUT, null);
		}
		catch (AerospikeException ae) {
			endPutTime(now, key, PutOperation.PUT, ae);
			throw ae;
		}
	}


	public void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
			throws AerospikeException {
		delegate.put(eventLoop, listener, policy, key, bins);
	}

	public void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		long now = startPutTime(); 
		try {
			delegate.append(policy, key, bins);
			endPutTime(now, key, PutOperation.APPEND, null);
		}
		catch (AerospikeException ae) {
			endPutTime(now, key, PutOperation.APPEND, ae);
			throw ae;
		}
	}

	public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
			throws AerospikeException {
		delegate.append(eventLoop, listener, policy, key, bins);
	}

	public void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		long now = startPutTime(); 
		try {
			delegate.prepend(policy, key, bins);
			endPutTime(now, key, PutOperation.PREPEND, null);
		}
		catch (AerospikeException ae) {
			endPutTime(now, key, PutOperation.PREPEND, ae);
			throw ae;
		}
	}

	public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
			throws AerospikeException {
		delegate.prepend(eventLoop, listener, policy, key, bins);
	}

	public void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		long now = startPutTime(); 
		try {
			delegate.add(policy, key, bins);
			endPutTime(now, key, PutOperation.ADD, null);
		}
		catch (AerospikeException ae) {
			endPutTime(now, key, PutOperation.ADD, ae);
			throw ae;
		}
	}

	public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)
			throws AerospikeException {
		delegate.add(eventLoop, listener, policy, key, bins);
	}

	public boolean delete(WritePolicy policy, Key key) throws AerospikeException {
		long now = startPutTime(); 
		try {
			boolean result = delegate.delete(policy, key);
			endPutTime(now, key, PutOperation.DELETE, null);
			return result;
		}
		catch (AerospikeException ae) {
			endPutTime(now, key, PutOperation.DELETE, ae);
			throw ae;
		}
	}

	public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key)
			throws AerospikeException {
		delegate.delete(eventLoop, listener, policy, key);
	}

	public void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate)
			throws AerospikeException {
		delegate.truncate(policy, ns, set, beforeLastUpdate);
	}

	public void touch(WritePolicy policy, Key key) throws AerospikeException {
		long now = startPutTime(); 
		try {
			delegate.touch(policy, key);
			endPutTime(now, key, PutOperation.TOUCH, null);
		}
		catch (AerospikeException ae) {
			endPutTime(now, key, PutOperation.TOUCH, ae);
			throw ae;
		}
	}

	public void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key)
			throws AerospikeException {
		delegate.touch(eventLoop, listener, policy, key);
	}

	public boolean exists(Policy policy, Key key) throws AerospikeException {
		return delegate.exists(policy, key);
	}

	public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) throws AerospikeException {
		delegate.exists(eventLoop, listener, policy, key);
	}

	public boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return delegate.exists(policy, keys);
	}

	public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys)
			throws AerospikeException {
		delegate.exists(eventLoop, listener, policy, keys);
	}

	public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys)
			throws AerospikeException {
		delegate.exists(eventLoop, listener, policy, keys);
	}

	public Record get(Policy policy, Key key) throws AerospikeException {
		long now = startGetTime();
		try {
			Record result = delegate.get(policy, key);
			endGetTime(now, key, result, null);
			return result;
		}
		catch (AerospikeException ae) {
			endGetTime(now, key, null, ae);
			throw ae;
		}
	}

	public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
		delegate.get(eventLoop, listener, policy, key);
	}

	public Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
		long now = startGetTime();
		try {
			Record result = delegate.get(policy, key, binNames);
			endGetTime(now, key, result, null);
			return result;
		}
		catch (AerospikeException ae) {
			endGetTime(now, key, null, ae);
			throw ae;
		}
	}

	public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames)
			throws AerospikeException {
		delegate.get(eventLoop, listener, policy, key, binNames);
	}

	public Record getHeader(Policy policy, Key key) throws AerospikeException {
		return delegate.getHeader(policy, key);
	}

	public void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key)
			throws AerospikeException {
		delegate.getHeader(eventLoop, listener, policy, key);
	}

	public void get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
		delegate.get(policy, records);
	}

	public void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records)
			throws AerospikeException {
		delegate.get(eventLoop, listener, policy, records);
	}

	public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records)
			throws AerospikeException {
		delegate.get(eventLoop, listener, policy, records);
	}

	public Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException {
		long now = startBatchTime();
		try {
			Record[] results = delegate.get(policy, keys);
			endBatchTime(now, keys, results, null);
			return results;
		}
		catch (AerospikeException ae) {
			endBatchTime(now, keys, null, ae);
			throw ae;
		}
	}

	public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys)
			throws AerospikeException {
		delegate.get(eventLoop, listener, policy, keys);
	}

	public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys)
			throws AerospikeException {
		delegate.get(eventLoop, listener, policy, keys);
	}

	public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
		long now = startBatchTime();
		try {
			Record[] results = delegate.get(policy, keys, binNames);
			endBatchTime(now, keys, results, null);
			return results;
		}
		catch (AerospikeException ae) {
			endBatchTime(now, keys, null, ae);
			throw ae;
		}
	}

	public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys,
			String... binNames) throws AerospikeException {
		delegate.get(eventLoop, listener, policy, keys, binNames);
	}

	public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys,
			String... binNames) throws AerospikeException {
		delegate.get(eventLoop, listener, policy, keys, binNames);
	}

	public Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return delegate.getHeader(policy, keys);
	}

	public void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys)
			throws AerospikeException {
		delegate.getHeader(eventLoop, listener, policy, keys);
	}

	public void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys)
			throws AerospikeException {
		delegate.getHeader(eventLoop, listener, policy, keys);
	}

	public Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
		long now = startPutTime();
		try {
			Record result = delegate.operate(policy, key, operations);
			endPutTime(now, key, PutOperation.OPERATE, null);
			return result;
		}
		catch (AerospikeException ae) {
			endPutTime(now, key, PutOperation.OPERATE, ae);
			throw ae;
		}
	}

	public void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key,
			Operation... operations) throws AerospikeException {
		delegate.operate(eventLoop, listener, policy, key, operations);
	}

	public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames)
			throws AerospikeException {
		delegate.scanAll(policy, namespace, setName, callback, binNames);
	}

	public void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace,
			String setName, String... binNames) throws AerospikeException {
		delegate.scanAll(eventLoop, listener, policy, namespace, setName, binNames);
	}

	public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback,
			String... binNames) throws AerospikeException {
		delegate.scanNode(policy, nodeName, namespace, setName, callback, binNames);
	}

	public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback,
			String... binNames) throws AerospikeException {
		delegate.scanNode(policy, node, namespace, setName, callback, binNames);
	}

	public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language)
			throws AerospikeException {
		return delegate.register(policy, clientPath, serverPath, language);
	}

	public RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath,
			Language language) throws AerospikeException {
		return delegate.register(policy, resourceLoader, resourcePath, serverPath, language);
	}

	public RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language)
			throws AerospikeException {
		return delegate.registerUdfString(policy, code, serverPath, language);
	}

	public void removeUdf(InfoPolicy policy, String serverPath) throws AerospikeException {
		delegate.removeUdf(policy, serverPath);
	}

	public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args)
			throws AerospikeException {
		return delegate.execute(policy, key, packageName, functionName, args);
	}

	public void execute(EventLoop eventLoop, ExecuteListener listener, WritePolicy policy, Key key, String packageName,
			String functionName, Value... functionArgs) throws AerospikeException {
		delegate.execute(eventLoop, listener, policy, key, packageName, functionName, functionArgs);
	}

	public ExecuteTask execute(WritePolicy policy, Statement statement, String packageName, String functionName,
			Value... functionArgs) throws AerospikeException {
		return delegate.execute(policy, statement, packageName, functionName, functionArgs);
	}

	public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
		return delegate.query(policy, statement);
	}

	public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement)
			throws AerospikeException {
		delegate.query(eventLoop, listener, policy, statement);
	}

	public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
		return delegate.queryNode(policy, statement, node);
	}

	public ResultSet queryAggregate(QueryPolicy policy, Statement statement, String packageName, String functionName,
			Value... functionArgs) throws AerospikeException {
		return delegate.queryAggregate(policy, statement, packageName, functionName, functionArgs);
	}

	public ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException {
		return delegate.queryAggregate(policy, statement);
	}

	public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
		return delegate.queryAggregateNode(policy, statement, node);
	}

	public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName,
			IndexType indexType) throws AerospikeException {
		return delegate.createIndex(policy, namespace, setName, indexName, binName, indexType);
	}

	public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName,
			IndexType indexType, IndexCollectionType indexCollectionType) throws AerospikeException {
		return delegate.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType);
	}

	public IndexTask dropIndex(Policy policy, String namespace, String setName, String indexName)
			throws AerospikeException {
		return delegate.dropIndex(policy, namespace, setName, indexName);
	}

	public void createUser(AdminPolicy policy, String user, String password, List<String> roles)
			throws AerospikeException {
		delegate.createUser(policy, user, password, roles);
	}

	public void dropUser(AdminPolicy policy, String user) throws AerospikeException {
		delegate.dropUser(policy, user);
	}

	public void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException {
		delegate.changePassword(policy, user, password);
	}

	public void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
		delegate.grantRoles(policy, user, roles);
	}

	public void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
		delegate.revokeRoles(policy, user, roles);
	}

	public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
		delegate.createRole(policy, roleName, privileges);
	}

	public void dropRole(AdminPolicy policy, String roleName) throws AerospikeException {
		delegate.dropRole(policy, roleName);
	}

	public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges)
			throws AerospikeException {
		delegate.grantPrivileges(policy, roleName, privileges);
	}

	public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges)
			throws AerospikeException {
		delegate.revokePrivileges(policy, roleName, privileges);
	}

	public User queryUser(AdminPolicy policy, String user) throws AerospikeException {
		return delegate.queryUser(policy, user);
	}

	public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
		return delegate.queryUsers(policy);
	}

	public Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException {
		return delegate.queryRole(policy, roleName);
	}

	public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
		return delegate.queryRoles(policy);
	}

	@Override
	public ExecuteTask execute(WritePolicy policy, Statement statement, Operation... operations)
			throws AerospikeException {
		return delegate.execute(policy, statement, operations);
	}

	@Override
	public void scanPartitions(ScanPolicy policy, PartitionFilter partitionFilter, String namespace, String setName,
			ScanCallback callback, String... binNames) throws AerospikeException {

		delegate.scanPartitions(policy, partitionFilter, namespace, setName, callback, binNames);
	}

	@Override
	public void scanPartitions(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy,
			PartitionFilter partitionFilter, String namespace, String setName, String... binNames)
			throws AerospikeException {
		delegate.scanPartitions(eventLoop, listener, policy, partitionFilter, namespace, setName, binNames);
	}

	@Override
	public RecordSet queryPartitions(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter)
			throws AerospikeException {
		return queryPartitions(policy, statement, partitionFilter);
	}

	@Override
	public void queryPartitions(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy,
			Statement statement, PartitionFilter partitionFilter) throws AerospikeException {
		queryPartitions(eventLoop, listener, policy, statement, partitionFilter);
	}

	@Override
	public void createIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace,
			String setName, String indexName, String binName, IndexType indexType,
			IndexCollectionType indexCollectionType) throws AerospikeException {
		createIndex(eventLoop, listener, policy, namespace, setName, indexName, binName, indexType, indexCollectionType);
	}

	@Override
	public void dropIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace, String setName,
			String indexName) throws AerospikeException {
		dropIndex(eventLoop, listener, policy, namespace, setName, indexName);
	}

	@Override
	public void info(EventLoop eventLoop, InfoListener listener, InfoPolicy policy, Node node, String... commands)
			throws AerospikeException {
		info(eventLoop, listener, policy, node, commands);
	}

	@Override
	public Cluster getCluster() {
		return delegate.getCluster();
	}
}
