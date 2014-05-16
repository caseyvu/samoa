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
import com.yahoo.labs.samoa.instances.Instance;

public class AssignmentContentEvent implements ContentEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1031695762172836629L;

	private final int ruleNumberID;
	private final Instance instance;
	private final boolean isTesting;
	private final boolean isTraining;
	private final long instanceIndex;
	
	public AssignmentContentEvent() {
		this(0, 0, null, false, false);
	}
	public AssignmentContentEvent(int ruleID, long instanceIndex, 
			Instance instance, boolean isTesting, boolean isTraining) {
		this.ruleNumberID = ruleID;
		this.instance = instance;
		this.instanceIndex = instanceIndex;
		this.isTesting = isTesting;
		this.isTraining = isTraining;
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
	
	public Instance getInstance() {
		return this.instance;
	}
	
	public int getRuleNumberID() {
		return this.ruleNumberID;
	}
	
	public long getInstanceIndex() {
		return this.instanceIndex;
	}
	
	public boolean isTesting() {
		return this.isTesting;
	}
	
	public boolean isTraining() {
		return this.isTraining;
	}

}
