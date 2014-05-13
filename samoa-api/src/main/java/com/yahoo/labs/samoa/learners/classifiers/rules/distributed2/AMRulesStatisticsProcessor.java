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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.ActiveRule;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.RuleActiveRegressionNode;
import com.yahoo.labs.samoa.learners.classifiers.rules.common.RuleSplitNode;
import com.yahoo.labs.samoa.topology.Stream;

public class AMRulesStatisticsProcessor implements Processor {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4342286729992707581L;

	private int processorId;
	
	private List<ActiveRule> ruleSet;
	
	private Stream outputStream;
	
	private double splitConfidence;
	private double tieThreshold;
	private int gracePeriod;
	
	protected boolean noAnomalyDetection;
	protected double multivariateAnomalyProbabilityThreshold;
	protected double univariateAnomalyprobabilityThreshold;
	protected int anomalyNumInstThreshold;
	
	public AMRulesStatisticsProcessor(Builder builder) {
		this.splitConfidence = builder.splitConfidence;
		this.tieThreshold = builder.tieThreshold;
		this.gracePeriod = builder.gracePeriod;
		this.noAnomalyDetection = builder.noAnomalyDetection;
		this.multivariateAnomalyProbabilityThreshold = builder.multivariateAnomalyProbabilityThreshold;
		this.univariateAnomalyprobabilityThreshold = builder.univariateAnomalyprobabilityThreshold;
		this.anomalyNumInstThreshold = builder.anomalyNumInstThreshold;
	}
	
	
	@Override
	public boolean process(ContentEvent event) {
		if (event instanceof AssignmentContentEvent) {
			AssignmentContentEvent ace = (AssignmentContentEvent) event;
			if (ace.isTesting()) {
				// Prediction
				this.predictOnInstance(ace);
			}
			if (ace.isTraining()) {
				// Training
				this.trainRuleWithInstance(ace.getRuleNumberID(), ace.getInstance());
			}
		}
		else if (event instanceof RuleContentEvent) {
			RuleContentEvent rce = (RuleContentEvent) event;
			if (!rce.isRemoving()) { // Remove rule is not applicable
				this.addRule(rce.getRule());
			}
		}
		return false;
	}
	
	/*
	 * Prediction
	 */
	private void predictOnInstance(AssignmentContentEvent ace) {
		int ruleID = ace.getRuleNumberID();
		Instance instance = ace.getInstance();
		for (ActiveRule rule: ruleSet) {
			if (rule.getRuleNumberID() == ruleID) {
				double [] vote=rule.getPrediction(instance);
				double error= rule.getCurrentError();
				
				// Send prediction
				this.sendPrediction(ace.getInstanceIndex(), vote, error, ruleID);
				return;
			}
		}
	}
	
	/*
	 * Training
	 */
	private void trainRuleWithInstance(int ruleID, Instance instance) {
		Iterator<ActiveRule> ruleIterator= this.ruleSet.iterator();
		while (ruleIterator.hasNext()) { 
			ActiveRule rule = ruleIterator.next();
			if (rule.getRuleNumberID() == ruleID) {
				// check for coverage again
				if (rule.isCovering(instance) && isAnomaly(instance, rule) == false) {
					//Update Change Detection Tests
					double error = rule.computeError(instance); //Use adaptive mode error
					boolean changeDetected = rule.getLearningNode().updateChangeDetection(error);
					if (changeDetected == true) {
						ruleIterator.remove();
						// send event
						this.sendRemoveRuleEvent(ruleID);
					} else {
						rule.updateStatistics(instance);
						if (rule.getInstancesSeen()  % this.gracePeriod == 0.0) {
							if (rule.tryToExpand(this.splitConfidence, this.tieThreshold) ) {
								rule.split();
								// expanded: update Aggregator with new/updated predicate
								this.sendPredicate(rule.getRuleNumberID(), rule.getLastUpdatedRuleSplitNode(), 
										(RuleActiveRegressionNode)rule.getLearningNode(), rule.lastUpdatedRuleSplitNodeIsNew());
							}	
						}
					}
				}
				return;
			}
		}	
	}
	
	/**
	 * Method to verify if the instance is an anomaly.
	 * @param instance
	 * @param rule
	 * @return
	 */
	private boolean isAnomaly(Instance instance, ActiveRule rule) {
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
	 * Add rule
	 */
	private boolean addRule(ActiveRule rule) {
		this.ruleSet.add(rule);
		return true;
	}
	
	/*
	 * Send events
	 */
	private void sendPrediction(long instanceIndex, double[] prediction, double error, int ruleID) {
		PredictionContentEvent pce = new PredictionContentEvent(instanceIndex, prediction, error, ruleID);
		this.outputStream.put(pce);
	}
	
	private void sendRemoveRuleEvent(int ruleID) {
		RuleContentEvent rce = new RuleContentEvent(ruleID, null, true);
		this.outputStream.put(rce);
	}
	
	private void sendPredicate(int ruleID, RuleSplitNode splitNode, RuleActiveRegressionNode learningNode, boolean isNew) {
		this.outputStream.put(new PredicateContentEvent(ruleID, splitNode, isNew));
	}

	@Override
	public void onCreate(int id) {
		this.processorId = id;
		this.ruleSet = new LinkedList<ActiveRule>();
	}

	@Override
	public Processor newProcessor(Processor p) {
		AMRulesStatisticsProcessor oldProcessor = (AMRulesStatisticsProcessor)p;
		AMRulesStatisticsProcessor newProcessor = 
					new AMRulesStatisticsProcessor.Builder(oldProcessor).build();
			
		newProcessor.setOutputStream(oldProcessor.outputStream);
		return newProcessor;
	}
	
	/*
	 * Builder
	 */
	public static class Builder {
		private double splitConfidence;
		private double tieThreshold;
		private int gracePeriod;
		
		private boolean noAnomalyDetection;
		private double multivariateAnomalyProbabilityThreshold;
		private double univariateAnomalyprobabilityThreshold;
		private int anomalyNumInstThreshold;

		private Instances dataset;
		
		public Builder(Instances dataset){
			this.dataset = dataset;
		}
		
		public Builder(AMRulesStatisticsProcessor processor) {
			this.splitConfidence = processor.splitConfidence;
			this.tieThreshold = processor.tieThreshold;
			this.gracePeriod = processor.gracePeriod;
			
			this.noAnomalyDetection = processor.noAnomalyDetection;
			this.multivariateAnomalyProbabilityThreshold = processor.multivariateAnomalyProbabilityThreshold;
			this.univariateAnomalyprobabilityThreshold = processor.univariateAnomalyprobabilityThreshold;
			this.anomalyNumInstThreshold = processor.anomalyNumInstThreshold;
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
		
		public AMRulesStatisticsProcessor build() {
			return new AMRulesStatisticsProcessor(this);
		}
	}
	
	/*
	 * Output stream
	 */
	public void setOutputStream(Stream stream) {
		this.outputStream = stream;
	}
	
	public Stream getOutputStream() {
		return this.outputStream;
	}

}
