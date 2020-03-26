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

public class LinearLatencyManager extends AbstractLatencyManager implements LatencyManager {
    private final AtomicInteger[] buckets;
    private final int lastBucket;
    private final int multiplyer;
    private static final int COLUMN_WIDTH = 6;
    private final boolean showMicroSeconds;
    private final boolean cumulative;
    private String header;
    private final int decimalPlaces;
    
    public LinearLatencyManager(int columns, int bitShift, boolean showMicroSeconds, boolean cumulative, final int decimalPlaces) {
		super(decimalPlaces);
		this.decimalPlaces = decimalPlaces;
		this.cumulative = cumulative;
    	this.lastBucket = columns - 1;
    	this.multiplyer = bitShift;
    	this.showMicroSeconds = showMicroSeconds;
		buckets = new AtomicInteger[columns];
		for (int i = 0; i < columns; i++) {
			buckets[i] = new AtomicInteger();
		}
		formHeader();
    }

    public LinearLatencyManager(int columns, int bitShift, boolean showMicroSeconds, boolean cumulative) {
    	this(columns, bitShift, showMicroSeconds, cumulative, 0);
    }
    public LinearLatencyManager(int columns, int bitShift, boolean showMicroSeconds) {
		this(columns, bitShift, showMicroSeconds, true);
    }    
    @Override
    public LatencyManager duplicate() {
		return new LinearLatencyManager(buckets.length, this.multiplyer, this.showMicroSeconds, this.cumulative, this.decimalPlaces);
    }

    private void padString(StringBuilder builder, int count) {
		for (int i = 0; i < count; i++) {
			builder.append(' ');
		}
    }
    private void formHeader() {
		int limit = 1;
		String units = showMicroSeconds ? "us" : "ms";
		StringBuilder s = new StringBuilder(64);
		int width = COLUMN_WIDTH + (decimalPlaces > 0 ? 1+decimalPlaces : 0);
		s.append("\n       <=1").append(units);
		padString(s, width - 5);
		s.append(" >1").append(units);
		this.padString(s, width - 4 - units.length());
		
		for (int i = 2; i < buckets.length; i++) {			
			limit += multiplyer;
			String str = " >" + limit + units;
			s.append(str);
			padString(s, decimalPlaces > 0 ? 1+decimalPlaces : 0);
		}

		s.append(this.formHeaderSuffix());
		header = s.toString();
    }
    
	public void add(long elapsedUs, int count, int success) {
		int index = getIndex(elapsedUs);
		buckets[index].incrementAndGet();
		
		super.add(elapsedUs, count, success);
	}

	private int getIndex(long elapsedUs) {
		if (!showMicroSeconds) {
			elapsedUs /= US_TO_MS;
		}
		if (elapsedUs < lastBucket) {
			return (int)elapsedUs;
		}
		else {
			return lastBucket;
		}
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
	@Override
	public void printLatencyResults(PrintStream stream, String prefix) {
		//TimingResults results = getResults();
		// Capture snapshot and make buckets cumulative.
		int[] array = new int[buckets.length];
		int sum = 0;
		int count;
		for (int i = buckets.length - 1; i >= 1 ; i--) {
			 count = buckets[i].getAndSet(0);
			 array[i] = cumulative ? count + sum : count;
			 sum += count;
		}
		// The first bucket (<=1ms) does not need a cumulative adjustment.
		count = buckets[0].getAndSet(0);
		array[0] = count;
		sum += count;
		// Print cumulative results.
		stream.print(prefix);
        int spaces = 6 - prefix.length();

        for (int j = 0; j < spaces; j++) {
        	stream.print(' ');
        }

        double sumDouble = (double)sum;
        int limit = 0;

        printColumn(stream, limit-1, sumDouble, array[0], decimalPlaces);
        printColumn(stream, limit-1, sumDouble, array[1], decimalPlaces);

        for (int i = 2; i < array.length; i++) {
            limit += multiplyer;
            printColumn(stream, limit, sumDouble, array[i], decimalPlaces);
        }
	}
	
}
