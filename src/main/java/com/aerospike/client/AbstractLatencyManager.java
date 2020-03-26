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

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public abstract class AbstractLatencyManager implements LatencyManager {
	protected static final long NS_TO_US = 1000;
	protected static final long US_TO_MS = 1000;
	protected static final long NS_TO_MS = NS_TO_US * US_TO_MS;
	private String header;
	// ycsb variables
	private AtomicInteger _buckets;
	private AtomicLongArray histogram;
	private AtomicLong histogramoverflow;
	private AtomicInteger operations;
	private AtomicLong totallatency;
	//keep a windowed version of these stats for printing status
	private AtomicInteger windowoperations;
	private AtomicLong windowtotallatency;
	private AtomicLong min;
	private AtomicLong max;
	private AtomicLong windowMin;
	private AtomicLong windowMax;
	private AtomicLong count;
	private AtomicLong success;
	private final String decimalFormatString;
	private final int decimalPlaces;

	public abstract void printLatencyResults(PrintStream stream, String prefix);

	protected AbstractLatencyManager() {
		this(0);
	}
	protected AbstractLatencyManager(int columnDecimalPoints) {
		// ycsb variables
		_buckets = new AtomicInteger(1000);
		histogram = new AtomicLongArray(_buckets.get());
		histogramoverflow = new AtomicLong(0);
		operations = new AtomicInteger(0);
		totallatency = new AtomicLong(0);
		windowoperations = new AtomicInteger(0);
		windowtotallatency = new AtomicLong(0);
		min = new AtomicLong(-1);
		max = new AtomicLong(-1);
		windowMin = new AtomicLong(-1);
		windowMax = new AtomicLong(-1);
		count = new AtomicLong(0);
		success = new AtomicLong(0);
		this.decimalPlaces = columnDecimalPoints;
		this.decimalFormatString = "%." + decimalPlaces + "f%%";
	}

	protected String formHeaderSuffix() {
		StringBuilder s = new StringBuilder(64);
		String[] headings = new String[] {"     avg", "     min", "      max", "  95th%", "  99th%", "  count", " success", "   recs"};
		for (String str : headings) {
			s.append(str);
		}
		return s.toString();
	}

	public void add(long elapsedUs, int count, int success) {
		/*
		 * ycsb calculations
		 */
		// Latency is specified in ns
		long latencyUs = elapsedUs;
		long latencyMs = latencyUs / 1000;
		if (latencyMs >= _buckets.get()) {
			histogramoverflow.incrementAndGet();
		} else {
			int latency = (int)latencyMs;
			histogram.incrementAndGet(latency);
		}
		operations.incrementAndGet();
		totallatency.addAndGet(latencyUs);
		windowoperations.incrementAndGet();
		windowtotallatency.addAndGet(latencyUs);

		if (count > 0 && this.count != null) {
			this.count.addAndGet(count);
		}
		if (success > 0) {
			this.success.addAndGet(success);
		}
		
		if ((min.get() < 0) || (latencyUs < min.get())) {
			min.set(latencyUs);
		}

		if ((max.get() < 0) || (latencyUs > max.get())) {
			max.set(latencyUs);
		}

		if ((windowMin.get() < 0) || (latencyUs < windowMin.get())) {
			windowMin.set(latencyUs);
		}

		if ((windowMax.get() < 0) || (latencyUs > windowMax.get())) {
			windowMax.set(latencyUs);
		}
		//System.out.printf("current = %d, windowMin = %d, windowMax = %d\n", latencyUs, windowMin.get(), windowMax.get());
	}

	public void printHeader(PrintStream stream) {	
		stream.println(header);
	}
	
	/**
	 * Print latency percents for specified cumulative ranges.
	 * This function is not absolutely accurate for a given time slice because this method 
	 * is not synchronized with the add() method.  Some values will slip into the next iteration.  
	 * It is not a good idea to add extra locks just to measure performance since that actually 
	 * affects performance.  Fortunately, the values will even out over time
	 * (ie. no double counting).
	 */
	public void printResults(PrintStream stream, String prefix) {
		if (this.count.get() == 0 && this.success.get() == 0) {
			return;
		}
		printLatencyResults(stream, prefix);
		/*
		 * ycsb print results
		 */
		double windowAvgLatency = (((double) windowtotallatency.get()) / ((double) windowoperations.get()));

		int opcounter = 0;
		boolean done95th = false;
		int ninetyFifth = 0;
		int ninetyNinth = 0;
		for (int i = 0; i < _buckets.get(); i++) {
			opcounter += histogram.get(i);
			double percentage = ((double) opcounter) / ((double) operations.get());
			if ((!done95th) && percentage >= 0.95) {
				ninetyFifth = i;
				done95th = true;
			}
			if (percentage >= 0.99) {
				ninetyNinth = i;
				break;
			}
		}
		printColumn(stream, 8, windowAvgLatency/1000.0, "ms");
		printColumn(stream, 8, windowMin.get()/1000.0, "ms");
		printColumn(stream, 9, windowMax.get()/1000.0, "ms");
		printColumn(stream, 7, ninetyFifth, "ms");
		printColumn(stream, 7, ninetyNinth, "ms");
		printColumn(stream, 7, windowoperations.get(), "");
		printColumn(stream, 8, (int)this.success.get(), "");
		printColumn(stream, 7, this.count == null ? -1 : (int)this.count.get(), "");

		stream.println();
		windowoperations.set(0);
		windowtotallatency.set(0);
		windowMin.set(-1);
		windowMax.set(-1);
		if (this.count != null) {
			this.count.set(0);
		}
		this.success.set(0);
	}

	protected void printColumn(PrintStream stream, int limit, double sum, int value) {
		this.printColumn(stream, limit, sum, value, 0);
	}

	protected void printColumn(PrintStream stream, int limit, double sum, int value, int decimalPlaces) {
        long percent = 0;

        String percentString;
        if (sum == 0) {
        	// No results
        	percentString = "---";
        }
        else if (decimalPlaces == 0) {
            if (value > 0) {
                percent = Math.round(value * 100.0 / sum);
            }
            percentString = Long.toString(percent) + "%";
        }
        else {
        		percentString = String.format(decimalFormatString, value * 100.0 / sum);
        }
        int spaces = Integer.toString(limit).length() + 4 - percentString.length() + (decimalPlaces > 0 ? 1+decimalPlaces : 0);

        for (int j = 0; j < spaces; j++) {
        		stream.print(' ');
        }
        stream.print(percentString);
	}

	protected void printColumn(PrintStream stream, int limit, int value) {
		String data = Integer.toString(value);      
		int spaces = Integer.toString(limit).length() + 4 - data.length();

		for (int j = 0; j < spaces; j++) {
			stream.print(' ');
		}
		stream.print(data);
	}

	protected void printColumn(PrintStream stream, int width, int value, String suffix) {
		String data = Integer.toString(value) + suffix;      
		if (value < 0) {
			data = "N/A";
		}
		int spaces = width - data.length();

		for (int j = 0; j < spaces; j++) {
			stream.print(' ');
		}
		stream.print(data);
	}

	protected void printColumn(PrintStream stream, int width, double value, String suffix) {
		String data = String.format("%.1f%s", value, suffix);
		if (value < 0) {
			data = "N/A";
		}
		int spaces = width - data.length();

		for (int j = 0; j < spaces; j++) {
			stream.print(' ');
		}
		stream.print(data);
	}

	public long getOperations() {
		return operations.get();
	}
}
