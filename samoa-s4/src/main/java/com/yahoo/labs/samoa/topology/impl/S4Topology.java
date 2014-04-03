package com.yahoo.labs.samoa.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 Yahoo! Inc.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.s4.core.util.AppConfig;
import org.apache.s4.core.util.ParsingUtils;
import org.apache.s4.deploy.DeploymentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.labs.samoa.topology.IProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.Topology;

public class S4Topology extends Topology {

	//private static Logger logger = LoggerFactory.getLogger(S4Topology.class);

	private String _evaluationTask;
	private String _topologyName;

	S4Topology(String topoName, String evalTask) {
		super(topoName);
		_evaluationTask = evalTask;
		// TODO include app
	}

	S4Topology(String topoName) {
		this(topoName, null);
	}

	@Override
	protected void addProcessingItem(IProcessingItem procItem) {
		// TODO add here the paralelism
		// the parallelism will be implemented by seting the amount of
		// processing items to be instantiated
		// If it is one use a singleton an instantiate in one of the partitions
		//
		super.addProcessingItem(procItem);
//		for (int i = 1; i < procItem.getParalellism(); i++) {
//			super.addProcessingItem(procItem.copy());
//			logger.debug("ADDED COPY {}", i);
//		}

	}
}
