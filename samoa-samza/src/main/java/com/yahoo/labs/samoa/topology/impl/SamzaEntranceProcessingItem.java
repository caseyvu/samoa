package com.yahoo.labs.samoa.topology.impl;

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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.learners.InstanceContentEvent;
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.utils.SamzaConfigFactory;
import com.yahoo.labs.samoa.utils.SystemsUtils;

/**
 * EntranceProcessingItem for Samza
 * which is also a Samza task (StreamTask & InitableTask)
 * 
 * @author Anh Thu Vu
 *
 */
public class SamzaEntranceProcessingItem implements EntranceProcessingItem, ISamzaProcessingItem,
													Serializable, StreamTask, InitableTask {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7157734520046135039L;
	
	private EntranceProcessor processor;
	private String name;
	private SamzaStream outputStream;
	
	public SamzaEntranceProcessingItem(EntranceProcessor processor) {
		this.processor = processor;
	}
	
	// Need this so Samza can initialize a StreamTask
	public SamzaEntranceProcessingItem() {} 
	
	@Override
	public EntranceProcessor getProcessor() {
		return this.processor;
	}
	
	@Override
	public EntranceProcessingItem setOutputStream(Stream stream) {
		this.outputStream = (SamzaStream) stream;
		return this;
	}
	
	public SamzaStream getOutputStream() {
		return this.outputStream;
	}

	@Override
	public String getName() {
		return this.name;
	}
	
	@Override
	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public int addOutputStream(SamzaStream stream) {
		this.setOutputStream(stream);
		return 1; // entrance PI should have only 1 output stream
	}
	
	/*
	 * Serialization
	 */
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private static class SerializationProxy implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 313907132721414634L;
		
		private EntranceProcessor processor;
		private SamzaStream outputStream;
		private String name;
		
		public SerializationProxy(SamzaEntranceProcessingItem epi) {
			this.processor = epi.processor;
			this.outputStream = epi.outputStream;
			this.name = epi.name;
		}
	}
	
	/*
	 * Implement Samza Task
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
		this.outputStream = wrapper.outputStream;
		this.outputStream.onCreate();
	}

	@Override
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
		this.outputStream.setCollector(collector);
		InstanceContentEvent event = (InstanceContentEvent) envelope.getMessage();
		this.outputStream.put(event);
	}
	
	
	
	/*
	 * Implementation of Samza's SystemConsumer to get events from source
	 * and feed to SAMOA system
	 * 
	 */
	/* Current implementation: buffer the incoming events and send a batch 
	 * of them when poll() is called by Samza system.
	 * 
	 * Currently I impose a soft limit on the size of the buffer:
	 * when the buffer size reaches the limit, the reading thread will sleep
	 * for 100ms.
	 * A hard limit can be achieved by overriding the method
	 * protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue()
	 * of BlockingEnvelopeMap
	 * Then we have to stop reading (getting events from EntranceProcessor)
	 * when the queue/buffer is full.
	 * 
	 */
	public static class SamoaSystemConsumer extends BlockingEnvelopeMap {
		
		private EntranceProcessor entranceProcessor = null;
		private SystemStreamPartition systemStreamPartition;
		
		private static final Logger logger = LoggerFactory.getLogger(SamoaSystemConsumer.class);

		public SamoaSystemConsumer(String systemName, Config config) {
			String yarnConfHome = config.get(SamzaConfigFactory.YARN_CONF_HOME_KEY);
			if (yarnConfHome != null && yarnConfHome.length() > 0) // if the property is set , otherwise, assume we are running in
				                                            // local mode and ignore this
				SystemsUtils.setHadoopConfigHome(yarnConfHome);
			
			String filename = config.get(SamzaConfigFactory.FILE_KEY);
			String filesystem = config.get(SamzaConfigFactory.FILESYSTEM_KEY);
			String name = config.get(SamzaConfigFactory.JOB_NAME_KEY);
			SerializationProxy wrapper = (SerializationProxy) SystemsUtils.deserializeObjectFromFileAndKey(filesystem, filename, name);
			
			this.entranceProcessor = wrapper.processor;
			this.entranceProcessor.onCreate(0);
			
			// Internal stream from SystemConsumer to EntranceTask, so we
			// need only one partition
			this.systemStreamPartition = new SystemStreamPartition(systemName, wrapper.name, new Partition(0));
		}
		
		@Override
		public void start() {
			Thread processorPollingThread = new Thread(
	                new Runnable() {
	                    @Override
	                    public void run() {
	                        try {
	                            pollingEntranceProcessor();
	                            setIsAtHead(systemStreamPartition, true);
	                        } catch (InterruptedException e) {
	                            e.getStackTrace();
	                            stop();
	                        }
	                    }
	                }
	        );

	        processorPollingThread.start();
		}

		@Override
		public void stop() {

		}
		
		private void pollingEntranceProcessor() throws InterruptedException {
			int messageCnt = 0;
			while(!this.entranceProcessor.isFinished()) {
				messageCnt = this.getNumMessagesInQueue(systemStreamPartition);
				if (this.entranceProcessor.hasNext() && messageCnt < 10000) {
					this.put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition,null, null,this.entranceProcessor.nextEvent()));
				} else {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						break;
					}
				}
			}
			
			// Send last event
			this.put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition,null, null,this.entranceProcessor.nextEvent()));
		}
		
	}
}