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
package com.yahoo.labs.samoa.topology.impl;

import static org.junit.Assert.*;
import mockit.Mocked;
import mockit.Tested;
import mockit.Verifications;

import org.junit.Before;
import org.junit.Test;

import com.yahoo.labs.samoa.core.ContentEvent;

/**
 * @author Anh Thu Vu
 *
 */
public class ThreadsEventRunnableTest {

	@Tested private ThreadsEventRunnable task;
	
	@Mocked private ThreadsWorkerProcessingItem workerPi;
	@Mocked private ContentEvent event;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		task = new ThreadsEventRunnable(workerPi, event);
	}

	@Test
	public void testConstructor() {
		assertSame("ProcessingItem is not set correctly.",workerPi,task.getProcessingItem());
		assertSame("ContentEvent is not set correctly.",event,task.getContentEvent());
	}
	
	@Test
	public void testRun() {
		task.run();
		new Verifications () {
			{
				workerPi.processEvent(event); times=1;
			}
		};
	}

}