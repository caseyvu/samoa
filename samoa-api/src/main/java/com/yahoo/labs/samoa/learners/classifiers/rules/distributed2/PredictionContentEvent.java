package com.yahoo.labs.samoa.learners.classifiers.rules.distributed2;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2014 Yahoo! Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.yahoo.labs.samoa.core.ContentEvent;

public class PredictionContentEvent implements ContentEvent {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4325291376364729737L;
	
	private final int ruleNumberID;
	private final double[] prediction;
	private final double error;
	private final long instanceIndex;

	public PredictionContentEvent(long instanceIndex, double[] prediction, double error, int ruleID) {
		this.ruleNumberID = ruleID;
		this.instanceIndex = instanceIndex;
		this.prediction = prediction;
		this.error = error;
	}
	
	@Override
	public String getKey() {
		return Integer.toString(this.ruleNumberID);
	}

	@Override
	public void setKey(String key) {
		// do nothing
	}

	@Override
	public boolean isLastEvent() {
		return false;
	}
	
	public int getRuleNumberID() {
		return this.ruleNumberID;
	}
	
	public long getInstanceIndex() {
		return this.instanceIndex;
	}
	
	public double[] getPrediction() {
		return this.prediction;
	}
	
	public double getError() {
		return this.error;
	}

}
