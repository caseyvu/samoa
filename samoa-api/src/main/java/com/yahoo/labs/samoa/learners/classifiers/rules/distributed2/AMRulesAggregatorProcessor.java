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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.learners.InstanceContentEvent;
import com.yahoo.labs.samoa.learners.ResultContentEvent;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.ActiveRule;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.NonLearningRule;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.Perceptron;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.RuleActiveRegressionNode;
import com.yahoo.labs.samoa.moa.classifiers.rules.core.attributeclassobservers.FIMTDDNumericAttributeClassLimitObserver;
import com.yahoo.labs.samoa.moa.classifiers.rules.core.voting.ErrorWeightedVote;
import com.yahoo.labs.samoa.topology.Stream;

public class AMRulesAggregatorProcessor implements Processor {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6866829484252994433L;
	
	private int processorId;

	// Rules & default rule
	protected List<NonLearningRule> ruleSet;
	protected ActiveRule defaultRule;
	protected int ruleNumberID;
	protected double[] statistics;
	
	private Map<Long, InstanceRecord> pendingPredictions;
	private boolean lastEventReceived;

	// SAMOA Stream
	private Stream statisticsStream;
	private Stream resultStream;

	// Options
	protected int pageHinckleyThreshold;
	protected double pageHinckleyAlpha;
	protected boolean driftDetection;
	protected int predictionFunction; // Adaptive=0 Perceptron=1 TargetMean=2
	protected boolean constantLearningRatioDecay;
	protected double learningRatio;

	protected double splitConfidence;
	protected double tieThreshold;
	protected int gracePeriod;

	protected FIMTDDNumericAttributeClassLimitObserver numericObserver;
	protected ErrorWeightedVote voteType;

	/*
	 * Constructor
	 */
	public AMRulesAggregatorProcessor (Builder builder) {
		this.pageHinckleyThreshold = builder.pageHinckleyThreshold;
		this.pageHinckleyAlpha = builder.pageHinckleyAlpha;
		this.driftDetection = builder.driftDetection;
		this.predictionFunction = builder.predictionFunction;
		this.constantLearningRatioDecay = builder.constantLearningRatioDecay;
		this.learningRatio = builder.learningRatio;
		this.splitConfidence = builder.splitConfidence;
		this.tieThreshold = builder.tieThreshold;
		this.gracePeriod = builder.gracePeriod;
		
		this.numericObserver = builder.numericObserver;
		this.voteType = builder.voteType;
	}
	@Override
	public boolean process(ContentEvent event) {
		if (event instanceof RuleContentEvent) { // remove rule
			RuleContentEvent rce = (RuleContentEvent) event;
			if (rce.isRemoving()) { // Adding is not applicable
				this.removeRule(rce.getRuleNumberID());
			}
		} 
		else if (event instanceof PredicateContentEvent) { // add predicate to rule
			this.updateRuleSplitNode((PredicateContentEvent) event);
		}
		else if (event instanceof PredictionContentEvent) { // prediction
			this.processPredictionEvent((PredictionContentEvent) event);
		}
		else if (event instanceof InstanceContentEvent) {
			this.processInstanceEvent((InstanceContentEvent)event);
		}
		
		return false;
	}
	
	/*
	 * Process InstanceContentEvent
	 */
	private void processInstanceEvent(InstanceContentEvent ice) {
		Instance instance = ice.getInstance();
		long instanceIndex = ice.getInstanceIndex();
		
		if (ice.isLastEvent()) this.lastEventReceived = true;
		
		boolean coveredBySomeRule = false;
		List<Integer> ruleIDs = new ArrayList<Integer>();
		InstanceRecord record = new InstanceRecord(instance, ice.getClassId(), ice.getEvaluationIndex());
		for (NonLearningRule rule:ruleSet) {
			if (rule.isCovering(instance)) {
				coveredBySomeRule = true;
				// Prepare the record 
				record.addPredictionRecord(rule.getRuleNumberID());
				ruleIDs.add(rule.getRuleNumberID());
			}
		}
		
		if (!coveredBySomeRule) {
			// predict with default rule
			if (ice.isTesting()) {
				double[] prediction = getVotesForInstanceWithDefaultRule(instance);
				ResultContentEvent rce = newResultContentEvent(prediction, ice);
				resultStream.put(rce);
			}
			
			// train with default rule
			if (ice.isTraining()) {
				this.traingOnInstanceForDefaultRule(instance);
			}
		}
		else {
			this.pendingPredictions.put(instanceIndex, record);
			// Send instance to the rule
			for (Integer ruleID:ruleIDs) {
				this.sendInstanceToRule(ice, ruleID);
			}
		}
	}
	
	/*
	 * Prediction with Default Rule
	 */
	/**
	 * Helper method to generate new ResultContentEvent based on an instance and
	 * its prediction result.
	 * @param prediction The predicted class label from the decision tree model.
	 * @param inEvent The associated instance content event
	 * @return ResultContentEvent to be sent into Evaluator PI or other destination PI.
	 */
	private ResultContentEvent newResultContentEvent(double[] prediction, InstanceContentEvent inEvent){
		ResultContentEvent rce = new ResultContentEvent(inEvent.getInstanceIndex(), inEvent.getInstance(), inEvent.getClassId(), prediction, inEvent.isLastEvent());
		rce.setClassifierIndex(this.processorId);
		rce.setEvaluationIndex(inEvent.getEvaluationIndex());
		return rce;
	}
	
	private ResultContentEvent newResultContentEvent(double[] prediction, long instanceIndex, 
			Instance instance, int classId, int evalIndex, boolean isLast) {
		ResultContentEvent rce = new ResultContentEvent(instanceIndex, instance, classId, prediction, isLast);
		rce.setClassifierIndex(this.processorId);
		rce.setEvaluationIndex(evalIndex);
		return rce;
	}
	
	private double[] getVotesForInstanceWithDefaultRule(Instance instance) {
		ErrorWeightedVote errorWeightedVote=newErrorWeightedVote();
		double [] vote=defaultRule.getPrediction(instance);
		double error= defaultRule.getCurrentError();
		errorWeightedVote.addVote(vote,error);
		double[] weightedVote=errorWeightedVote.computeWeightedVote();
		return weightedVote;
	}

	public ErrorWeightedVote newErrorWeightedVote() {
		return voteType.getACopy();
	}
	
	/*
	 * Training with default rule
	 */
	private void traingOnInstanceForDefaultRule(Instance instance) {
		defaultRule.updateStatistics(instance);
		if (defaultRule.getInstancesSeen() % this.gracePeriod == 0.0) {
			if (defaultRule.tryToExpand(this.splitConfidence, this.tieThreshold) == true) {
				ActiveRule newDefaultRule=newRule(defaultRule.getRuleNumberID(),(RuleActiveRegressionNode)defaultRule.getLearningNode(),
						((RuleActiveRegressionNode)defaultRule.getLearningNode()).getStatisticsOtherBranchSplit()); //other branch
				defaultRule.split();
				defaultRule.setRuleNumberID(++ruleNumberID);
				this.ruleSet.add(new NonLearningRule(this.defaultRule));
				// send to statistics PI
				sendAddRuleEvent(defaultRule.getRuleNumberID(), this.defaultRule);
				defaultRule=newDefaultRule;

			}
		}
	}
	
	/*
	 * Create new rules
	 */
	private ActiveRule newRule(int ID, RuleActiveRegressionNode node, double[] statistics) {
		ActiveRule r=newRule(ID);

		if (node!=null)
		{
			if(node.getPerceptron()!=null)
			{
				r.getLearningNode().setPerceptron(new Perceptron(node.getPerceptron()));
				r.getLearningNode().getPerceptron().setLearningRatio(this.learningRatio);
			}
			if (statistics==null)
			{
				double mean;
				if(node.getNodeStatistics().getValue(0)>0){
					mean=node.getNodeStatistics().getValue(1)/node.getNodeStatistics().getValue(0);
					r.getLearningNode().getTargetMean().reset(mean, 1); 
				}
			}  
		}
		if (statistics!=null && ((RuleActiveRegressionNode)r.getLearningNode()).getTargetMean()!=null)
		{
			double mean;
			if(statistics[0]>0){
				mean=statistics[1]/statistics[0];
				((RuleActiveRegressionNode)r.getLearningNode()).getTargetMean().reset(mean, (long)statistics[0]); 
			}
		}
		return r;
	}

	private ActiveRule newRule(int ID) {
		ActiveRule r=new ActiveRule.Builder().
				threshold(this.pageHinckleyThreshold).
				alpha(this.pageHinckleyAlpha).
				changeDetection(this.driftDetection).
				predictionFunction(this.predictionFunction).
				statistics(new double[3]).
				learningRatio(this.learningRatio).
				numericObserver(numericObserver).
				id(ID).build();
		return r;
	}
	
	/*
	 * Process PredicateContentEvent
	 */
	private void updateRuleSplitNode(PredicateContentEvent pce) {
		int ruleID = pce.getRuleNumberID();
		for (NonLearningRule rule:ruleSet) {
			if (rule.getRuleNumberID() == ruleID) {
				rule.nodeListAdd(pce.getRuleSplitNode());
			}
		}
	}
	
	/*
	 * Process PredictionContentEvent
	 */
	private void processPredictionEvent(PredictionContentEvent pce) {
		long instanceIndex = pce.getInstanceIndex();
		InstanceRecord record = this.pendingPredictions.get(instanceIndex);
		if (record.setPrediction(pce.getRuleNumberID(),pce.getPrediction(), pce.getError())) {
			if (record.hasReceivedAllPredictions()) {
				// Get prediction
				double[] votes = this.getCummulativePrediction(record);
				
				// Remove pending record
				this.pendingPredictions.remove(instanceIndex);
				
				// Send prediction
				boolean isLast = this.lastEventReceived && this.pendingPredictions.isEmpty();
				ResultContentEvent rce = this.newResultContentEvent(votes, instanceIndex, record.getInstance(), 
						record.getClassId(), record.getEvaluationIndex(), isLast);
				resultStream.put(rce);
			}
		}
	}
	
	private double[] getCummulativePrediction(InstanceRecord record) {
		ErrorWeightedVote errorWeightedVote=newErrorWeightedVote();
		for (PredictionRecord prediction:record.getPredictions()) {
			errorWeightedVote.addVote(prediction.getPrediction(), prediction.getError());
		}
		return errorWeightedVote.computeWeightedVote();
	}
	
	
	/*
	 * Process RuleContentEvent
	 */
	private boolean removeRule(int ruleID) {
		Iterator<NonLearningRule> ruleIterator= this.ruleSet.iterator();
		while (ruleIterator.hasNext()) { 
			NonLearningRule rule = ruleIterator.next();
			if (rule.getRuleNumberID() == ruleID) {
				ruleIterator.remove();
				return true;
			}
		}
		return false;
	}

	@Override
	public void onCreate(int id) {
		this.processorId = id;
		this.statistics= new double[]{0.0,0,0};	
		this.ruleNumberID=0;
		this.defaultRule = newRule(++this.ruleNumberID);
		
		this.ruleSet = new LinkedList<NonLearningRule>();
		
		this.pendingPredictions = new HashMap<Long,InstanceRecord>();
		this.lastEventReceived = false;
	}

	/* 
	 * Clone processor
	 */
	@Override
	public Processor newProcessor(Processor p) {
		AMRulesAggregatorProcessor oldProcessor = (AMRulesAggregatorProcessor) p;
		Builder builder = new Builder(oldProcessor);
		AMRulesAggregatorProcessor newProcessor = builder.build();
		newProcessor.resultStream = oldProcessor.resultStream;
		newProcessor.statisticsStream = oldProcessor.statisticsStream;
		return newProcessor;
	}
	
	/*
	 * Send events
	 */
	private void sendInstanceToRule(InstanceContentEvent ice, int ruleID) {
		AssignmentContentEvent ace = new AssignmentContentEvent(ruleID, ice.getInstanceIndex(), ice.getInstance(), ice.isTesting(), ice.isTraining());
		this.statisticsStream.put(ace);
	}
	
	private void sendAddRuleEvent(int ruleID, ActiveRule rule) {
		RuleContentEvent rce = new RuleContentEvent(ruleID, rule, false);
		this.statisticsStream.put(rce);
	}
	
	/*
	 * Output streams
	 */
	public void setStatisticsStream(Stream statisticsStream) {
		this.statisticsStream = statisticsStream;
	}
	
	public Stream getStatisticsStream() {
		return this.statisticsStream;
	}
	
	public void setResultStream(Stream resultStream) {
		this.resultStream = resultStream;
	}
	
	public Stream getResultStream() {
		return this.resultStream;
	}
	
	/*
	 * Others
	 */
    public boolean isRandomizable() {
    	return true;
    }

	/*
	 * Builder
	 */
	public static class Builder {
		private int pageHinckleyThreshold;
		private double pageHinckleyAlpha;
		private boolean driftDetection;
		private int predictionFunction; // Adaptive=0 Perceptron=1 TargetMean=2
		private boolean constantLearningRatioDecay;
		private double learningRatio;
		private double splitConfidence;
		private double tieThreshold;
		private int gracePeriod;
		
		private FIMTDDNumericAttributeClassLimitObserver numericObserver;
		private ErrorWeightedVote voteType;
		
		private Instances dataset;
		
		public Builder(Instances dataset){
			this.dataset = dataset;
		}
		
		public Builder(AMRulesAggregatorProcessor processor) {
			this.pageHinckleyThreshold = processor.pageHinckleyThreshold;
			this.pageHinckleyAlpha = processor.pageHinckleyAlpha;
			this.driftDetection = processor.driftDetection;
			this.predictionFunction = processor.predictionFunction;
			this.constantLearningRatioDecay = processor.constantLearningRatioDecay;
			this.learningRatio = processor.learningRatio;
			this.splitConfidence = processor.splitConfidence;
			this.tieThreshold = processor.tieThreshold;
			this.gracePeriod = processor.gracePeriod;
			
			
			
			this.numericObserver = processor.numericObserver;
			this.voteType = processor.voteType;
		}
		
		public Builder threshold(int threshold) {
			this.pageHinckleyThreshold = threshold;
			return this;
		}
		
		public Builder alpha(double alpha) {
			this.pageHinckleyAlpha = alpha;
			return this;
		}
		
		public Builder changeDetection(boolean changeDetection) {
			this.driftDetection = changeDetection;
			return this;
		}
		
		public Builder predictionFunction(int predictionFunction) {
			this.predictionFunction = predictionFunction;
			return this;
		}
		
		public Builder constantLearningRatioDecay(boolean constantDecay) {
			this.constantLearningRatioDecay = constantDecay;
			return this;
		}
		
		public Builder learningRatio(double learningRatio) {
			this.learningRatio = learningRatio;
			return this;
		}
		
		public Builder splitConfidence(double splitConfidence) {
			this.splitConfidence = splitConfidence;
			return this;
		}
		
		public Builder tieThreshold(double tieThreshold) {
			this.tieThreshold = tieThreshold;
			return this;
		}
		
		public Builder gracePeriod(int gracePeriod) {
			this.gracePeriod = gracePeriod;
			return this;
		}
		
		public Builder numericObserver(FIMTDDNumericAttributeClassLimitObserver numericObserver) {
			this.numericObserver = numericObserver;
			return this;
		}
		
		public Builder voteType(ErrorWeightedVote voteType) {
			this.voteType = voteType;
			return this;
		}
		
		public AMRulesAggregatorProcessor build() {
			return new AMRulesAggregatorProcessor(this);
		}
	}
	
	static class InstanceRecord {
		private final Instance instance;
		private final int classId;
		private final int evaluationIndex;
		private List<PredictionRecord> predictions;
		private int receivedPredictionsCount;
		
		InstanceRecord(Instance instance, int classId, int evalIndex) {
			this.instance = instance;
			this.classId = classId;
			this.evaluationIndex = evalIndex;
			this.predictions = new ArrayList<PredictionRecord>();
			this.receivedPredictionsCount = 0;
		}
		
		Instance getInstance() {
			return this.instance;
		}
		
		int getClassId() {
			return this.classId;
		}
		
		int getEvaluationIndex() {
			return this.evaluationIndex;
		}
		
		void addPredictionRecord(int ruleID) {
			this.predictions.add(new PredictionRecord(ruleID));
		}
		
		boolean setPrediction(int ruleID, double[] prediction, double error) {
			for (PredictionRecord record:this.predictions) {
				if (record.getRuleNumberID() == ruleID) {
					if (!record.isSet()) receivedPredictionsCount++;
					record.setPrediction(prediction, error);
					return true;
				}
			}
			return false;
		}
		
		boolean hasReceivedAllPredictions() {
			return (receivedPredictionsCount == predictions.size());
		}
		
		List<PredictionRecord> getPredictions() {
			return this.predictions;
		}
	}
	
	static class PredictionRecord {
		private final int ruleNumberID;
		private double[] prediction;
		private double error;
		private boolean isSet;
		
		PredictionRecord(int ruleID) {
			this.ruleNumberID = ruleID;
			this.isSet = false;
			this.error = 0;
			this.prediction = null;
		}
		
		int getRuleNumberID() {
			return this.ruleNumberID;
		}
		
		void setPrediction(double[] prediction, double error) {
			this.isSet = true;
			this.prediction = prediction;
			this.error = error;
		}
		
		double[] getPrediction() {
			return this.prediction;
		}
		
		double getError() {
			return this.error;
		}
		
		boolean isSet() {
			return this.isSet;
		}
	}
}
