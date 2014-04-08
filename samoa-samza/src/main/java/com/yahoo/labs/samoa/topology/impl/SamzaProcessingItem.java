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

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.ProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.impl.SamzaStream.SamzaSystemStream;
import com.yahoo.labs.samoa.utils.PartitioningScheme;
import com.yahoo.labs.samoa.utils.SamzaConfigFactory;
import com.yahoo.labs.samoa.utils.SystemsUtils;
import com.yahoo.labs.samoa.utils.StreamDestination;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

/**
 * ProcessingItem for Samza
 * which is also a Samza task (StreamTask and InitableTask)
 * 
 * @author Anh Thu Vu
 */
public class SamzaProcessingItem implements ISamzaProcessingItem, ProcessingItem, Serializable,
											StreamTask, InitableTask {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Processor processor;
	private int parallelismHint;
	private Set<SamzaSystemStream> inputStreams; // input streams: system.stream
	private List<SamzaStream> outputStreams;
	private String name; // PI name = Job name
	
	// Need this so Samza can initialize a StreamTask
	public SamzaProcessingItem() {}
	
	/* 
	 * Implement com.yahoo.labs.samoa.topology.ProcessingItem
	 */
	public SamzaProcessingItem(Processor processor, int parallelismHint) {
		this.processor = processor;
		this.parallelismHint = parallelismHint;
		this.inputStreams = new HashSet<SamzaSystemStream>();
		this.outputStreams = new LinkedList<SamzaStream>();
		this.name = null;
	}
	
	@Override
	public Processor getProcessor() {
		return processor;
	}
	
	private ProcessingItem addInputStream(Stream inputStream, PartitioningScheme scheme) {
		SamzaSystemStream stream = ((SamzaStream) inputStream).addDestination(new StreamDestination(this, parallelismHint,scheme));
		this.inputStreams.add(stream);
		return this;
	}
	
	@Override
	public ProcessingItem connectInputShuffleStream(Stream inputStream) {
		return this.addInputStream(inputStream, PartitioningScheme.SHUFFLE);
	}
	
	@Override
	public ProcessingItem connectInputKeyStream(Stream inputStream) {
		return this.addInputStream(inputStream, PartitioningScheme.GROUP_BY_KEY);
	}
	
	@Override
	public ProcessingItem connectInputAllStream(Stream inputStream) {
		return this.addInputStream(inputStream, PartitioningScheme.BROADCAST);
	}
	
	public Set<SamzaSystemStream> getInputStreams() {
		return this.inputStreams;
	}
	
	@Override
	public int getParalellism() {
		return this.parallelismHint;
	}
	
	@Override
	public String getName() {
		return this.name;
	}
	
	@Override
	public void setName(String name) {
		this.name = name;
	}
	
	/*
	 * Implement com.yahoo.samoa.topology.impl.ISamzaProcessingItem
	 */
	@Override
	public int addOutputStream(SamzaStream stream) {
		this.outputStreams.add(stream);
		return this.outputStreams.size();
	}
	
	public List<SamzaStream> getOutputStreams() {
		return this.outputStreams;
	}

	/*
	 * Implement Samza task
	 */
	@Override
	public void init(Config config, TaskContext context) throws Exception {
		String yarnConfHome = config.get(SamzaConfigFactory.YARN_CONF_HOME_KEY);
		if (yarnConfHome != null && yarnConfHome.length() > 0) // if the property is set , otherwise, assume we are running in
            													// local mode and ignore this
			SystemsUtils.setHadoopConfigHome(yarnConfHome);
		
		String filename = config.get(SamzaConfigFactory.FILE_KEY);
		String filesystem = config.get(SamzaConfigFactory.FILESYSTEM_KEY);
		this.name = config.get(SamzaConfigFactory.JOB_NAME_KEY);
		SerializationProxy wrapper = (SerializationProxy) SystemsUtils.deserializeObjectFromFileAndKey(filesystem, filename, name);
		this.processor = wrapper.processor;
		this.outputStreams = wrapper.outputStreams;
		
		// Init Processor and Streams
		this.processor.onCreate(0);
		for (SamzaStream stream:this.outputStreams) {
			stream.onCreate();
		}
		
	}

	@Override
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
		for (SamzaStream stream:this.outputStreams) {
			stream.setCollector(collector);
		}
		this.processor.process((ContentEvent) envelope.getMessage());
	}
	
	/*
	 * SerializationProxy
	 */
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private static class SerializationProxy implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1534643987559070336L;
		
		private Processor processor;
		private List<SamzaStream> outputStreams;
		
		public SerializationProxy(SamzaProcessingItem pi) {
			this.processor = pi.getProcessor();
			this.outputStreams = pi.getOutputStreams();
		}
	}

}