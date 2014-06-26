package com.yahoo.labs.samoa.learners.classifiers.rules;

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

import java.util.ArrayList;
import java.util.List;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.MultiChoiceOption;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.learners.RegressionLearner;
import com.yahoo.labs.samoa.learners.classifiers.rules.distributed.AMRDefaultRuleProcessor;
import com.yahoo.labs.samoa.learners.classifiers.rules.distributed.AMRLearnerProcessor;
import com.yahoo.labs.samoa.learners.classifiers.rules.distributed.AMRRuleSetProcessor;
import com.yahoo.labs.samoa.moa.classifiers.rules.core.attributeclassobservers.FIMTDDNumericAttributeClassLimitObserver;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.TopologyBuilder;

/**
 * @author Anh Thu Vu
 *
 */
public class HorizontalAMRulesRegressor implements RegressionLearner, Configurable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2785944439173586051L;

	// Options
	public FloatOption splitConfidenceOption = new FloatOption(
			"splitConfidence",
			'c',
			"Hoeffding Bound Parameter. The allowable error in split decision, values closer to 0 will take longer to decide.",
			0.0000001, 0.0, 1.0);

	public FloatOption tieThresholdOption = new FloatOption("tieThreshold",
			't', "Hoeffding Bound Parameter. Threshold below which a split will be forced to break ties.",
			0.05, 0.0, 1.0);

	public IntOption gracePeriodOption = new IntOption("gracePeriod",
			'g', "Hoeffding Bound Parameter. The number of instances a leaf should observe between split attempts.",
			200, 1, Integer.MAX_VALUE);

	public FlagOption DriftDetectionOption = new FlagOption("DoNotDetectChanges", 'H',
			"Drift Detection. Page-Hinkley.");

	public FloatOption pageHinckleyAlphaOption = new FloatOption(
			"pageHinckleyAlpha",
			'a',
			"The alpha value to use in the Page Hinckley change detection tests.",
			0.005, 0.0, 1.0);

	public IntOption pageHinckleyThresholdOption = new IntOption(
			"pageHinckleyThreshold",
			'l',
			"The threshold value (Lambda) to be used in the Page Hinckley change detection tests.",
			35, 0, Integer.MAX_VALUE);

	public FlagOption noAnomalyDetectionOption = new FlagOption("noAnomalyDetection", 'A',
			"Disable anomaly Detection.");

	public FloatOption multivariateAnomalyProbabilityThresholdOption = new FloatOption(
			"multivariateAnomalyProbabilityThresholdd",
			'm',
			"Multivariate anomaly threshold value.",
			0.99, 0.0, 1.0);

	public FloatOption univariateAnomalyProbabilityThresholdOption = new FloatOption(
			"univariateAnomalyprobabilityThreshold",
			'u',
			"Univariate anomaly threshold value.",
			0.10, 0.0, 1.0);

	public IntOption anomalyNumInstThresholdOption = new IntOption(
			"anomalyThreshold",
			'n',
			"The threshold value of anomalies to be used in the anomaly detection.",
			30, 0, Integer.MAX_VALUE); // num minimum of instances to detect anomalies. 15.

	public FlagOption unorderedRulesOption = new FlagOption("setUnorderedRulesOn", 'U',
			"unorderedRules.");

	public ClassOption numericObserverOption = new ClassOption("numericObserver",
			'z', "Numeric observer.", 
			FIMTDDNumericAttributeClassLimitObserver.class,
			"FIMTDDNumericAttributeClassLimitObserver");

	public MultiChoiceOption predictionFunctionOption = new MultiChoiceOption(
			"predictionFunctionOption", 'P', "The prediction function to use.", new String[]{
					"Adaptative","Perceptron", "Target Mean"}, new String[]{
					"Adaptative","Perceptron", "Target Mean"}, 0);

	public FlagOption constantLearningRatioDecayOption = new FlagOption(
			"learningRatio_Decay_set_constant", 'd',
			"Learning Ratio Decay in Perceptron set to be constant. (The next parameter).");

	public FloatOption learningRatioOption = new FloatOption(
			"learningRatio", 's', 
			"Constante Learning Ratio to use for training the Perceptrons in the leaves.", 0.025);

	public MultiChoiceOption votingTypeOption = new MultiChoiceOption(
			"votingType", 'V', "Voting Type.", new String[]{
					"InverseErrorWeightedVote","UniformWeightedVote"}, new String[]{
					"InverseErrorWeightedVote","UniformWeightedVote"}, 0);
	
	public IntOption learnerParallelismOption = new IntOption(
            "leanerParallelism",
            'p',
            "The number of local statistics PI to do distributed computation",
            1, 1, Integer.MAX_VALUE);
	public IntOption ruleSetParallelismOption = new IntOption(
            "modelParallelism",
            'r',
            "The number of replicated model (rule set) PIs",
            1, 1, Integer.MAX_VALUE);

	// Processor
	private AMRRuleSetProcessor model;
	private AMRDefaultRuleProcessor root;
	private AMRLearnerProcessor learner;
	// private AMRCombineResultProcessor combiner;

	// Stream
	private Stream forwardToRootStream;
	private Stream forwardToLearnerStream;
	private Stream modelResultStream;
	
	private Stream newRuleStream;
	private Stream rootResultStream;
	
	private Stream predicateStream;
	
	// private Stream resultStream;

	@Override
	public void init(TopologyBuilder topologyBuilder, Instances dataset, int parallelism) {
		
		// Create MODEL PIs
		this.model = new AMRRuleSetProcessor.Builder(dataset)
//		.threshold(pageHinckleyThresholdOption.getValue())
//		.alpha(pageHinckleyAlphaOption.getValue())
//		.changeDetection(this.DriftDetectionOption.isSet())
//		.predictionFunction(predictionFunctionOption.getChosenIndex())
//		.constantLearningRatioDecay(constantLearningRatioDecayOption.isSet())
//		.learningRatio(learningRatioOption.getValue())
//		.splitConfidence(splitConfidenceOption.getValue())
//		.tieThreshold(tieThresholdOption.getValue())
//		.gracePeriod(gracePeriodOption.getValue())
		.noAnomalyDetection(noAnomalyDetectionOption.isSet())
		.multivariateAnomalyProbabilityThreshold(multivariateAnomalyProbabilityThresholdOption.getValue())
		.univariateAnomalyProbabilityThreshold(univariateAnomalyProbabilityThresholdOption.getValue())
		.anomalyNumberOfInstancesThreshold(anomalyNumInstThresholdOption.getValue())
		.unorderedRules(unorderedRulesOption.isSet())
//		.numericObserver((FIMTDDNumericAttributeClassLimitObserver)numericObserverOption.getValue())
		.voteType(votingTypeOption.getChosenIndex())
		.build();

		topologyBuilder.addProcessor(model, this.ruleSetParallelismOption.getValue());
		
		// MODEL PIs streams
		this.forwardToRootStream = topologyBuilder.createStream(this.model);
		this.forwardToRootStream.setBatchSize(50);
		this.forwardToLearnerStream = topologyBuilder.createStream(this.model);
		this.forwardToLearnerStream.setBatchSize(50);
		this.modelResultStream = topologyBuilder.createStream(this.model);
		this.modelResultStream.setBatchSize(50);
		
		this.model.setDefaultRuleStream(this.forwardToRootStream);
		this.model.setStatisticsStream(this.forwardToLearnerStream);
		this.model.setResultStream(this.modelResultStream);
		
		// Create DefaultRule PI
		this.root = new AMRDefaultRuleProcessor.Builder(dataset)
		.threshold(pageHinckleyThresholdOption.getValue())
		.alpha(pageHinckleyAlphaOption.getValue())
		.changeDetection(this.DriftDetectionOption.isSet())
		.predictionFunction(predictionFunctionOption.getChosenIndex())
		.constantLearningRatioDecay(constantLearningRatioDecayOption.isSet())
		.learningRatio(learningRatioOption.getValue())
		.splitConfidence(splitConfidenceOption.getValue())
		.tieThreshold(tieThresholdOption.getValue())
		.gracePeriod(gracePeriodOption.getValue())
//		.noAnomalyDetection(noAnomalyDetectionOption.isSet())
//		.multivariateAnomalyProbabilityThreshold(multivariateAnomalyProbabilityThresholdOption.getValue())
//		.univariateAnomalyProbabilityThreshold(univariateAnomalyProbabilityThresholdOption.getValue())
//		.anomalyNumberOfInstancesThreshold(anomalyNumInstThresholdOption.getValue())
//		.unorderedRules(unorderedRulesOption.isSet())
		.numericObserver((FIMTDDNumericAttributeClassLimitObserver)numericObserverOption.getValue())
//		.voteType(votingTypeOption.getChosenIndex())
		.build();
		
		topologyBuilder.addProcessor(this.root);
		
		// Default Rule PI streams
		this.newRuleStream = topologyBuilder.createStream(this.root);
		this.newRuleStream.setBatchSize(1);
		this.rootResultStream = topologyBuilder.createStream(this.root);
		this.rootResultStream.setBatchSize(50);
		
		this.root.setRuleStream(this.newRuleStream);
		this.root.setResultStream(this.rootResultStream);
		
		// Create Learner PIs
		this.learner = new AMRLearnerProcessor.Builder(dataset)
    	.splitConfidence(splitConfidenceOption.getValue())
    	.tieThreshold(tieThresholdOption.getValue())
    	.gracePeriod(gracePeriodOption.getValue())
    	.noAnomalyDetection(noAnomalyDetectionOption.isSet())
		.multivariateAnomalyProbabilityThreshold(multivariateAnomalyProbabilityThresholdOption.getValue())
		.univariateAnomalyProbabilityThreshold(univariateAnomalyProbabilityThresholdOption.getValue())
		.anomalyNumberOfInstancesThreshold(anomalyNumInstThresholdOption.getValue())
    	.build();
    
		topologyBuilder.addProcessor(learner, this.learnerParallelismOption.getValue());
    
		this.predicateStream = topologyBuilder.createStream(learner);
		this.predicateStream.setBatchSize(1);
		this.learner.setOutputStream(predicateStream);
		
		// Create result combiner
//		this.combiner = new AMRCombineResultProcessor();
//		topologyBuilder.addProcessor(combiner, 1);
//		this.resultStream = topologyBuilder.createStream(combiner);
//		this.resultStream.setBatchSize(200);
//		this.combiner.setResultStream(this.resultStream);
		
		// Connect streams
		// to MODEL
		topologyBuilder.connectInputAllStream(this.newRuleStream, this.model);
		topologyBuilder.connectInputAllStream(this.predicateStream, this.model);
		// to ROOT
		topologyBuilder.connectInputShuffleStream(this.forwardToRootStream, this.root);
		// to LEARNER
		topologyBuilder.connectInputKeyStream(this.forwardToLearnerStream, this.learner);
		topologyBuilder.connectInputAllStream(this.newRuleStream, this.learner);
		// to COMBINER
//		topologyBuilder.connectInputShuffleStream(this.modelResultStream, this.combiner);
//		topologyBuilder.connectInputShuffleStream(this.rootResultStream, this.combiner);
	}

	@Override
	public Processor getInputProcessor() {
		return model;
	}

	@Override
	public Stream getResultStream() {
		return this.modelResultStream;
	}
	
	@Override
	public List<Stream> getResultStreams() {
		List<Stream> list = new ArrayList<Stream>();
		list.add(this.modelResultStream);
		list.add(this.rootResultStream);
		return list;
	}

}
