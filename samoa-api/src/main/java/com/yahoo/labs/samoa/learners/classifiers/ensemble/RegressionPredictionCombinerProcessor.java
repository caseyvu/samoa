package com.yahoo.labs.samoa.learners.classifiers.ensemble;

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

import java.util.HashMap;

import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.moa.core.DoubleVector;

/**
 * @author Anh Thu Vu
 *
 */
public class RegressionPredictionCombinerProcessor extends
		PredictionCombinerProcessor {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	protected void addStatisticsforInstanceReceived(int instanceIndex, int classifierIndex, double[] prediction, int add) {
        if (this.mapCountsforInstanceReceived == null) {
            this.mapCountsforInstanceReceived = new HashMap<Integer, Integer>();
            this.mapVotesforInstanceReceived = new HashMap<Integer, DoubleVector>();
        }
        DoubleVector vote = new DoubleVector(prediction);
        DoubleVector combinedVote = this.mapVotesforInstanceReceived.get(instanceIndex);
        if (combinedVote == null){
            combinedVote = new DoubleVector();
        }
        vote.scaleValues(getEnsembleMemberWeight(classifierIndex));
        combinedVote.addValues(vote);
                    
        this.mapVotesforInstanceReceived.put(instanceIndex, combinedVote);
        Integer count = this.mapCountsforInstanceReceived.get(instanceIndex);
        if (count == null) {
            count = 0;
        }
        this.mapCountsforInstanceReceived.put(instanceIndex, count + add);
    }
	
	@Override
	protected double getEnsembleMemberWeight(int i) {
        return 1.0/this.ensembleSize; // equal weight among all classifier. Any better option?
    }
	
	@Override
    public Processor newProcessor(Processor sourceProcessor) {
        RegressionPredictionCombinerProcessor newProcessor = new RegressionPredictionCombinerProcessor();
        RegressionPredictionCombinerProcessor originProcessor = (RegressionPredictionCombinerProcessor) sourceProcessor;
        if (originProcessor.getOutputStream() != null) {
            newProcessor.setOutputStream(originProcessor.getOutputStream());
        }
        newProcessor.setSizeEnsemble(originProcessor.getSizeEnsemble());
        return newProcessor;
    }
}
