/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;

/**
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ClassHierarchyBasedScoreCorrection {

	private KnowledgeBase kb;
	private Map<String, LinkedList<String>> indexedTree;
	private Set<String> rootNodes;
	private Map<String, Double> scores;
	private Map<String, Double> correctionFactors;
	private double defaultCorrectionFactor = 0.0;
	private boolean verbose = false;
	
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	public ClassHierarchyBasedScoreCorrection(KnowledgeBase kb, double defaultCorrectionFactor) {
		this.kb = kb;
		this.defaultCorrectionFactor = defaultCorrectionFactor;
		
		// build the complete class hierarchy tree
		indexedTree = new HashMap<>();
		// keep track of the root nodes, as the class hierarchy does not contain owl:Thing
		rootNodes = new HashSet<>(KnowledgeBase.getClassHierarchy().values());
		for(String subClass : KnowledgeBase.getClassHierarchy().keySet()) {
			String superClass = KnowledgeBase.getClassHierarchy().get(subClass);
			
			LinkedList<String> children = MapUtils.get(indexedTree, superClass, new LinkedList<String>());
			children.add(subClass);
			
			rootNodes.remove(subClass);
		}
		
		// index the correction factors - currently not available in kb
		correctionFactors = new HashMap<>();
	}
	
	public Processable<Pair<Integer, Double>> applyClassHierarchyScoreCorrection(Processable<Pair<Integer, Double>> classScores) {
		
		scores = new HashMap<>();
		
		// add the scores to the score map
		for(Pair<Integer, Double> p : classScores.get()) {
			String clsName = kb.getClassIndices().get(p.getFirst());
			scores.put(clsName, p.getSecond());
		}
		
		// start at the root nodes, descend breadth-first 
		for(String root : rootNodes) {
			findCandidates(root, null, 0.0);
		}
		
		// create output
		Collection<Pair<Integer, Double>> result = new LinkedList<>();
		for(String c : scores.keySet()) {
			int clsId = kb.getClassIds().get(c);
			result.add(new Pair<>(clsId, scores.get(c)));
		}
		return classScores.createProcessableFromCollection(result);
	}
	
	protected void findCandidates(String node, String superClass, double correctionSum) {
		
		double score = 0.0;
		
		// if a candidate is encountered
		if(scores.containsKey(node)) {
			// - and super is set, correct the score using super's correction sum
			if(superClass!=null) {
				score = correctScore(node, superClass, correctionSum);
				
				// reset the correction factor (do not cumulate because we just changed the score!)
				correctionSum = 0.0;
			} else {
				score = scores.get(node);
			}
			
			// set super = candidate
			superClass = node;
		}
				
		if(indexedTree.containsKey(node)) {
			for(String n : indexedTree.get(node)) {
				// add node's correction factor and pass it to the next level
				Double sum = cumulateCorrectionSum(n, node, score, correctionSum);
				findCandidates(n, superClass, sum);
			}
		}
	}
	
	protected Double cumulateCorrectionSum(String node, String superClass, double superClassScore, double currentSum) {
		Double superClassFactor = correctionFactors.get(superClass);
		
		if(superClassFactor==null) {
			superClassFactor = defaultCorrectionFactor;
		}
		
		double score = 0.0;
		
		if(currentSum==0.0) {
			score = superClassScore * superClassFactor;
		} else {
			score = currentSum + (superClassScore * superClassFactor);
		}
		
//		System.out.println(String.format("[ClassHierarchyBasedScoreCorrection] cumulating score for %s with super class %s (%f): %f + %f x %f = %f", 
//				node,
//				superClass,
//				superClassScore,
//				currentSum,
//				superClassScore,
//				superClassFactor,
//				score
//				));
		
		return score;
	}
	
	protected double correctScore(String node, String superClass, double correctionSum) {
		Double score = scores.get(node);
		
//		if(correctionSum==null) {
//			correctionSum = defaultCorrectionFactor;
//		}
		
		double newScore = score + correctionSum;
		
		if(verbose) {
			System.out.println(String.format("[ClassHiearchyBasedScoreCorrection] correcting score for class %s based on super class %s: %f + %f = %f", 
				node,
				superClass,
				score,
				correctionSum,
				newScore
				));
		}
		
		scores.put(node, newScore);
		
		return newScore;
	}
}
