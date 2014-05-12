package com.yahoo.labs.samoa.learners.classifiers.rules.distributed1;

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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.learners.InstanceContentEvent;
import com.yahoo.labs.samoa.learners.ResultContentEvent;
import com.yahoo.labs.samoa.learners.classifiers.rules.centralized.AMRulesRegressorProcessor;
import com.yahoo.labs.samoa.learners.classifiers.rules.centralized.AMRulesRegressorProcessor.Builder;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.ActiveRule;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.LearningRule;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.PassiveRule;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.Perceptron;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.RuleActiveRegressionNode;
import com.yahoo.labs.samoa.moa.classifiers.rules.core.attributeclassobservers.FIMTDDNumericAttributeClassLimitObserver;
import com.yahoo.labs.samoa.moa.classifiers.rules.core.voting.ErrorWeightedVote;
import com.yahoo.labs.samoa.topology.Stream;

public class AMRulesAggregatorProcessor implements Processor {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6303385725332704251L;

	private int processorId;

	// Rules & default rule
	protected List<PassiveRule> ruleSet;
	protected ActiveRule defaultRule;
	protected int ruleNumberID;
	protected double[] statistics;

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

	protected boolean noAnomalyDetection;
	protected double multivariateAnomalyProbabilityThreshold;
	protected double univariateAnomalyprobabilityThreshold;
	protected int anomalyNumInstThreshold;

	protected boolean unorderedRules;

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
		
		this.noAnomalyDetection = builder.noAnomalyDetection;
		this.multivariateAnomalyProbabilityThreshold = builder.multivariateAnomalyProbabilityThreshold;
		this.univariateAnomalyprobabilityThreshold = builder.univariateAnomalyprobabilityThreshold;
		this.anomalyNumInstThreshold = builder.anomalyNumInstThreshold;
		this.unorderedRules = builder.unorderedRules;
		
		this.numericObserver = builder.numericObserver;
		this.voteType = builder.voteType;
	}
	
	/*
	 * Process
	 */
	@Override
	public boolean process(ContentEvent event) {
		if (event instanceof InstanceContentEvent) {
			InstanceContentEvent instanceEvent = (InstanceContentEvent) event;
			// predict
			if (instanceEvent.isTesting()) {
				this.predictOnInstance(instanceEvent);
			}
		
			// train
			if (instanceEvent.isTraining()) {
				this.trainOnInstance(instanceEvent);
			}
		}
		else if (event instanceof PredicateContentEvent) {
			this.updateRuleSplitNode((PredicateContentEvent) event);
		}
		
		return true;
	}
	
	/*
	 * Prediction
	 */
	private void predictOnInstance (InstanceContentEvent instanceEvent) {
		double[] prediction = getVotesForInstance(instanceEvent.getInstance());
		ResultContentEvent rce = newResultContentEvent(prediction, instanceEvent);
		resultStream.put(rce);
	}
	
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
	
	/**
	 * getVotesForInstance extension of the instance method getVotesForInstance
	 * in moa.classifier.java
	 * returns the prediction of the instance.
	 * Called in EvaluateModelRegression
	 */            
	private double[] getVotesForInstance(Instance instance) {
		ErrorWeightedVote errorWeightedVote=newErrorWeightedVote();
		//DoubleVector combinedVote = new DoubleVector();   
		int numberOfRulesCovering = 0;
		
		for (PassiveRule rule: ruleSet) {
			if (rule.isCovering(instance) == true){
				numberOfRulesCovering++;
				//DoubleVector vote = new DoubleVector(rule.getPrediction(instance));
				double [] vote=rule.getPrediction(instance);
				double error= rule.getCurrentError();
				errorWeightedVote.addVote(vote,error);
				//combinedVote.addValues(vote);
				if (!this.unorderedRules) { // Ordered Rules Option.
					break; // Only one rule cover the instance.
				}
			}
		}

		if (numberOfRulesCovering == 0) {
			//combinedVote = new DoubleVector(defaultRule.getPrediction(instance));
			double [] vote=defaultRule.getPrediction(instance);
			double error= defaultRule.getCurrentError();
			errorWeightedVote.addVote(vote,error);	
		} 	
		double[] weightedVote=errorWeightedVote.computeWeightedVote();
		//double weightedError=errorWeightedVote.getWeightedError();
		
		return weightedVote;
	}

	public ErrorWeightedVote newErrorWeightedVote() {
		return voteType.getACopy();
	}
	
	/*
	 * Training
	 */
	private void trainOnInstance (InstanceContentEvent instanceEvent) {
		this.trainOnInstanceImpl(instanceEvent.getInstance());
	}
	public void trainOnInstanceImpl(Instance instance) {
		/**
		 * AMRules Algorithm
		 * 
		 //For each rule in the rule set
			//If rule covers the instance
				//if the instance is not an anomaly	
					//Update Change Detection Tests
				    	//Compute prediction error
				    	//Call PHTest
						//If change is detected then
							//Remove rule
						//Else
							//Update sufficient statistics of rule
							//If number of examples in rule  > Nmin
								//Expand rule
						//If ordered set then
							//break
			//If none of the rule covers the instance
				//Update sufficient statistics of default rule
				//If number of examples in default rule is multiple of Nmin
					//Expand default rule and add it to the set of rules
					//Reset the default rule
		 */
		boolean rulesCoveringInstance = false;
		Iterator<PassiveRule> ruleIterator= this.ruleSet.iterator();
		while (ruleIterator.hasNext()) { 
			PassiveRule rule = ruleIterator.next();
			if (rule.isCovering(instance) == true) {
				rulesCoveringInstance = true;
				if (isAnomaly(instance, rule) == false) {
					//Update Change Detection Tests
					double error = rule.computeError(instance); //Use adaptive mode error
					boolean changeDetected = rule.getLearningNode().updateChangeDetection(error);
					if (changeDetected == true) {
						ruleIterator.remove();
						// Send to statistics PIs
						sendRemoveRuleEvent(rule.getRuleNumberID());
					} else {
						rule.updateStatistics(instance);
						// Send instance to statistics PIs
						sendInstanceToRule(instance, rule.getRuleNumberID());
					}
					if (!this.unorderedRules) 
						break;
				}
			}
		}	

		if (rulesCoveringInstance == false){ 
			defaultRule.updateStatistics(instance);
			if (defaultRule.getInstancesSeen() % this.gracePeriod == 0.0) {
				if (defaultRule.tryToExpand(this.splitConfidence, this.tieThreshold) == true) {
					ActiveRule newDefaultRule=newRule(defaultRule.getRuleNumberID(),(RuleActiveRegressionNode)defaultRule.getLearningNode(),
							((RuleActiveRegressionNode)defaultRule.getLearningNode()).getStatisticsOtherBranchSplit()); //other branch
					defaultRule.split();
					defaultRule.setRuleNumberID(++ruleNumberID);
					this.ruleSet.add(new PassiveRule(this.defaultRule));
					// send to statistics PI
					sendAddRuleEvent(defaultRule.getRuleNumberID(), this.defaultRule);
					defaultRule=newDefaultRule;
				}
			}
		}
	}

	/**
	 * Method to verify if the instance is an anomaly.
	 * @param instance
	 * @param rule
	 * @return
	 */
	private boolean isAnomaly(Instance instance, LearningRule rule) {
		//AMRUles is equipped with anomaly detection. If on, compute the anomaly value. 			
		boolean isAnomaly = false;	
		if (this.noAnomalyDetection == false){
			if (rule.getInstancesSeen() >= this.anomalyNumInstThreshold) {
				isAnomaly = rule.isAnomaly(instance, 
						this.univariateAnomalyprobabilityThreshold,
						this.multivariateAnomalyProbabilityThreshold,
						this.anomalyNumInstThreshold);
			}
		}
		return isAnomaly;
	}
	
	/*
	 * Create new rules
	 */
	// TODO check this after finish rule, LN
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
	 * Add predicate/RuleSplitNode for a rule
	 */
	private void updateRuleSplitNode(PredicateContentEvent pce) {
		int ruleID = pce.getRuleNumberID();
		for (PassiveRule rule:ruleSet) {
			if (rule.getRuleNumberID() == ruleID) {
				if (rule.nodeListAdd(pce.getRuleSplitNode())) {
					rule.setLearningNode(pce.getLearningNode());
				}
			}
		}
	}

	@Override
	public void onCreate(int id) {
		this.processorId = id;
		this.statistics= new double[]{0.0,0,0};	
		this.ruleNumberID=0;
		this.defaultRule = newRule(++this.ruleNumberID);
		
		this.ruleSet = new LinkedList<PassiveRule>();
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
	private void sendInstanceToRule(Instance instance, int ruleID) {
		AssignmentContentEvent ace = new AssignmentContentEvent(ruleID, instance);
		this.statisticsStream.put(ace);
	}
	
	private void sendRemoveRuleEvent(int ruleID) {
		RuleContentEvent rce = new RuleContentEvent(ruleID, null, true);
		this.statisticsStream.put(rce);
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
		
		private boolean noAnomalyDetection;
		private double multivariateAnomalyProbabilityThreshold;
		private double univariateAnomalyprobabilityThreshold;
		private int anomalyNumInstThreshold;
		
		private boolean unorderedRules;
		
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
			
			this.noAnomalyDetection = processor.noAnomalyDetection;
			this.multivariateAnomalyProbabilityThreshold = processor.multivariateAnomalyProbabilityThreshold;
			this.univariateAnomalyprobabilityThreshold = processor.univariateAnomalyprobabilityThreshold;
			this.anomalyNumInstThreshold = processor.anomalyNumInstThreshold;
			this.unorderedRules = processor.unorderedRules;
			
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
		
		public Builder noAnomalyDetection(boolean noAnomalyDetection) {
			this.noAnomalyDetection = noAnomalyDetection;
			return this;
		}
		
		public Builder multivariateAnomalyProbabilityThreshold(double mAnomalyThreshold) {
			this.multivariateAnomalyProbabilityThreshold = mAnomalyThreshold;
			return this;
		}
		
		public Builder univariateAnomalyProbabilityThreshold(double uAnomalyThreshold) {
			this.univariateAnomalyprobabilityThreshold = uAnomalyThreshold;
			return this;
		}
		
		public Builder anomalyNumberOfInstancesThreshold(int anomalyNumInstThreshold) {
			this.anomalyNumInstThreshold = anomalyNumInstThreshold;
			return this;
		}
		
		public Builder unorderedRules(boolean unorderedRules) {
			this.unorderedRules = unorderedRules;
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

}
