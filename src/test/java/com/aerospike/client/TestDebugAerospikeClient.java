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

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.DebugAerospikeClient.Granularity;
import com.aerospike.client.DebugAerospikeClient.Options;
import com.aerospike.client.cluster.ClusterUtilites;


public class TestDebugAerospikeClient {

	private static final String HOST = "172.28.128.5";
	private static final String NAMESPACE = "test";
	private static final String SET = "testSet";
	private static final long RUN_TIME_IN_S = 5;
	private static final int NUM_KEYS = 10000000;
	private static final int NUM_THREADS = 5;
	
	private static class Processor implements Runnable {
		private static volatile boolean terminate = false;
		private final IAerospikeClient client;
		private final Random rand;
		
		public Processor(IAerospikeClient client, long seed) {
			this.client = client;
			rand = new Random(seed);
		}
		
		private Key getKey(int id) {
			return new Key(NAMESPACE, SET, id);
		}

		@Override
		public void run() {
			while (!terminate) {
				switch (rand.nextInt(5)) {
				case 0:
				case 1:
					client.get(null, getKey(rand.nextInt(NUM_KEYS)));
					break;
				case 2:
				case 3:
					client.put(null, getKey(rand.nextInt(NUM_KEYS)), new Bin("value", "qwertyuiopasduiopzxcvbnm,.1234567890-"));
					break;
				default:
					int count = rand.nextInt(10)+2;
					Key[] keys = new Key[count];
					for (int i = 0; i < count; i++) {
						keys[i] = getKey(rand.nextInt(NUM_KEYS));
					}
					client.get(null, keys);
				}
			}
		}
	}
	

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testEverySecondLinearCuml() throws Exception {
		System.out.println("*** Test every second, linear, cumulative ***");
		Processor.terminate = false;
		ExecutorService executors = Executors.newFixedThreadPool(NUM_THREADS);
		Options options = new Options(Granularity.EVERY_SECOND);
		
		options.setLatencyManager(new LinearLatencyManager(20, 1, false, true));
		
		IAerospikeClient client = new DebugAerospikeClient(HOST, 3000, options);

		for (int i = 0; i < NUM_THREADS; i++) {
			executors.execute(new Processor(client, i));
		}
		try {
			Thread.sleep(RUN_TIME_IN_S*1000);
			Processor.terminate = true;
		}
		finally {
			executors.shutdown();
			executors.awaitTermination(10, TimeUnit.SECONDS);
			executors.shutdownNow();
			client.close();
			System.out.println("*** Test concluded ***");
		}
	}

	@Test
	public void testEverySecondLinear1DPCuml() throws Exception {
		System.out.println("*** Test every second, linear, cumulative, 1 DP ***");
		ExecutorService executors = Executors.newFixedThreadPool(NUM_THREADS);
		Processor.terminate = false;
		Options options = new Options(Granularity.EVERY_SECOND, Granularity.EVERY_SECOND, Granularity.EVERY_SECOND);
		
		options.setLatencyManager(new LinearLatencyManager(20, 1, false, true, 1));
		
		IAerospikeClient client = new DebugAerospikeClient(HOST, 3000, options);

		for (int i = 0; i < NUM_THREADS; i++) {
			executors.execute(new Processor(client, i));
		}
		try {
			Thread.sleep(RUN_TIME_IN_S*1000);
			Processor.terminate = true;
		}
		finally {
			executors.shutdown();
			executors.awaitTermination(10, TimeUnit.SECONDS);
			executors.shutdownNow();
			client.close();
			System.out.println("*** Test concluded ***");
		}
	}
	
	@Test
	public void testEverySecondLinear() throws Exception {
		System.out.println("*** Test every second, linear, non-cumulative ***");
		ExecutorService executors = Executors.newFixedThreadPool(NUM_THREADS);
		Processor.terminate = false;
		Options options = new Options(Granularity.EVERY_SECOND, Granularity.EVERY_SECOND, Granularity.EVERY_SECOND);
		
		options.setLatencyManager(new LinearLatencyManager(20, 1, false, false));
		
		IAerospikeClient client = new DebugAerospikeClient(HOST, 3000, options);

		for (int i = 0; i < NUM_THREADS; i++) {
			executors.execute(new Processor(client, i));
		}
		try {
			Thread.sleep(RUN_TIME_IN_S*1000);
			Processor.terminate = true;
		}
		finally {
			executors.shutdown();
			executors.awaitTermination(10, TimeUnit.SECONDS);
			executors.shutdownNow();
			client.close();
			System.out.println("*** Test concluded ***");
		}
	}

	@Test
	public void testEverySecondLinear1DP() throws Exception {
		System.out.println("*** Test every second, linear, non-cumulative, 1DP ***");
		ExecutorService executors = Executors.newFixedThreadPool(NUM_THREADS);
		Processor.terminate = false;
		Options options = new Options(Granularity.EVERY_SECOND, Granularity.EVERY_SECOND, Granularity.EVERY_SECOND);
		
		options.setLatencyManager(new LinearLatencyManager(20, 1, false, true, 1));
		
		IAerospikeClient client = new DebugAerospikeClient(HOST, 3000, options);

		for (int i = 0; i < NUM_THREADS; i++) {
			executors.execute(new Processor(client, i));
		}
		try {
			Thread.sleep(RUN_TIME_IN_S*1000);
			Processor.terminate = true;
		}
		finally {
			executors.shutdown();
			executors.awaitTermination(10, TimeUnit.SECONDS);
			executors.shutdownNow();
			client.close();
			System.out.println("*** Test concluded ***");
		}
	}
	

	@Test
	public void testEverySecondExponential() throws Exception {
		System.out.println("*** Test every second, exponential ***");
		ExecutorService executors = Executors.newFixedThreadPool(NUM_THREADS);
		Processor.terminate = false;
		Options options = new Options(Granularity.EVERY_SECOND, Granularity.EVERY_SECOND, Granularity.EVERY_SECOND);
		
		options.setLatencyManager(new ExponentialLatencyManager(7, 1, false));
		
		IAerospikeClient client = new DebugAerospikeClient(HOST, 3000, options);

		for (int i = 0; i < NUM_THREADS; i++) {
			executors.execute(new Processor(client, i));
		}
		try {
			Thread.sleep(RUN_TIME_IN_S*1000);
			Processor.terminate = true;
		}
		finally {
			executors.shutdown();
			executors.awaitTermination(10, TimeUnit.SECONDS);
			executors.shutdownNow();
			client.close();
			System.out.println("*** Test concluded ***");
		}
	}
	
	@Test
	public void testEveryCall() throws Exception {
		System.out.println("*** Test every call ***");
		ExecutorService executors = Executors.newFixedThreadPool(1);
		Processor.terminate = false;
		Options options = new Options(Granularity.EVERY_CALL, Granularity.EVERY_CALL, Granularity.EVERY_CALL);
		
		IAerospikeClient client = new DebugAerospikeClient(HOST, 3000, options);

		ClusterUtilites info = new ClusterUtilites(client);
		info.printInfo();

		executors.execute(new Processor(client, 0));
		try {
			Thread.sleep(1000);
			Processor.terminate = true;
		}
		finally {
			executors.shutdown();
			executors.awaitTermination(10, TimeUnit.SECONDS);
			executors.shutdownNow();
			client.close();
			System.out.println("*** Test concluded ***");
		}
	}

}