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
package de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableEntity;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.MatchableEntityToKBComparator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.MatchableTableRowToKBComparator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.KeyContainmentRule;
import de.uni_mannheim.informatik.dws.tnt.match.similarity.TFIDF;
import de.uni_mannheim.informatik.dws.tnt.match.similarity.VectorSimilarity;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.MaximumBipartiteMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.rules.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.WekaMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.similarity.EqualsSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.DeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.UnadjustedDeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.MaximumOfTokenContainment;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceCosineSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchingRuleGenerator {

	private KnowledgeBase kb;
	private double valueSimilarityThreshold;
	private boolean verbose = false;
	private TFIDF<String> weights;

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	/**
	 * @param weights the weights to set
	 */
	public void setWeights(TFIDF<String> weights) {
		this.weights = weights;
	}

	public MatchingRuleGenerator(KnowledgeBase kb, double valueSimilarityThreshold) {
		this.kb = kb;
		this.valueSimilarityThreshold = valueSimilarityThreshold;
	}
	
	public Processable<LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn>> generateMatchingRules(Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBCorrespondences) {

		// key containment filter
		KeyContainmentRule containmentRule = new KeyContainmentRule(kb);
		containmentRule.setVerbose(verbose);
		Processable<Group<Integer, MatchableTableDeterminant>> excludedKeys = containmentRule.getExludedKeys(tableToKBCorrespondences);
		
		// get candidate keys for each class
		Processable<Group<Integer, MatchableTableDeterminant>> candidateKeysByClass = kb.getCandidateKeys()
			.group(
				(MatchableTableDeterminant record, DataIterator<Pair<Integer, MatchableTableDeterminant>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(record.getTableId(), record));
				});
		
		// create the rules
		Processable<Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, Group<Integer, MatchableTableDeterminant>>> groupedWithKeys = tableToKBCorrespondences
			// group table-to-kb correspondences by table/class combination
			.group(
				(Correspondence<MatchableTableColumn, Matchable> record, DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				})
			// join the candidate keys via the class id
			.join(candidateKeysByClass, (p)->p.getKey().getSecond(), (g)->g.getKey());

		Processable<LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn>> rules = groupedWithKeys 
			// join the excluded candidate keys via the table id
			.leftJoin(excludedKeys, (p)->p.getFirst().getKey().getFirst(), (g)->g.getKey())
			.map(
				(Pair<Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, Group<Integer, MatchableTableDeterminant>>, Group<Integer, MatchableTableDeterminant>> record, DataIterator<LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn>> resultCollector) 
				-> {
					Group<Integer, MatchableTableDeterminant> candidateKeys = record.getFirst().getSecond();
					Group<Integer, MatchableTableDeterminant> excludedCandidateKeys = record.getSecond();
					
					Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences = record.getFirst().getFirst().getRecords();
					
					// create a 1:1 mapping between the columns, s.t. only one version of each matching rule can exist!
					MaximumBipartiteMatchingAlgorithm<MatchableTableColumn, Matchable> max = new MaximumBipartiteMatchingAlgorithm<>(correspondences);
					max.run();
					correspondences = max.getResult();
					
					// get all candidate keys that have not been excluded
					Collection<MatchableTableDeterminant> keys;
					
					if(excludedCandidateKeys!=null) {						
						keys = Q.without(candidateKeys.getRecords().get(), excludedCandidateKeys.getRecords().get());
					} else {
						keys = candidateKeys.getRecords().get();
					}
					
					// check for each key if it is supported by correspondences
					for(MatchableTableDeterminant key : keys) {
					
						Processable<Correspondence<MatchableTableColumn, Matchable>> cors = correspondences.where((c)->key.getColumns().contains(c.getSecondRecord()));
						
						if(cors.size()==key.getColumns().size()) {
							LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn> rule = createLinearCombinationMatchingRule(cors.get());
							resultCollector.next(rule);
						}
					}
				});
			
		return rules;
	}
	
	public LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn> createLinearCombinationMatchingRule(Collection<Correspondence<MatchableTableColumn, Matchable>> correspondences) {

		// create the matching rule
		LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(valueSimilarityThreshold);
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : correspondences) {
			MatchableTableColumn webColumn = cor.getFirstRecord();
			MatchableTableColumn kbColumn = cor.getSecondRecord();
			
			SimilarityMeasure<?> measure = null;
			
			if(kbColumn.getType()==webColumn.getType()) {
				switch (kbColumn.getType()) {
				case numeric:
//					measure = new DeviationSimilarity();
					measure = new UnadjustedDeviationSimilarity();
					break;
				case date:
					WeightedDateSimilarity dateSim = new WeightedDateSimilarity(1, 3, 5);
					dateSim.setYearRange(10);
					measure = dateSim;
					break;
				default:
//					measure = new MaximumOfTokenContainment();
					measure = new TokenizingJaccardSimilarity();
					break;
				}
				
				MatchableEntityToKBComparator<?> comparator = new MatchableEntityToKBComparator<>(webColumn, kbColumn, measure, 0.0);
				try {
					rule.addComparator(comparator, 1.0);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				System.out.println(String.format("Incompatible types: %s (%s) <-> %s (%s)", webColumn, webColumn.getType(), kbColumn, kbColumn.getType()));
			}
		}

		rule.normalizeWeights();
		
		if(verbose) {
			System.out.println(String.format("[MatchingRuleGenerator] %f <= %s", valueSimilarityThreshold, rule.toString()));
		}
		
		return rule;
	}
	
	public WekaMatchingRule<MatchableTableRow, MatchableTableColumn> createWekaMatchingRule(
			Processable<MatchableTableColumn> targetSchema,
			Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences, 
			String classifierName, 
			String[] parameters) {

		// create the matching rule
		WekaMatchingRule<MatchableTableRow, MatchableTableColumn> rule = new WekaMatchingRule<>(valueSimilarityThreshold, classifierName, parameters);
		
		for(MatchableTableColumn kbColumn : targetSchema.sort((c)->c.getColumnIndex()).get()) {
			switch (kbColumn.getType()) {
			case numeric:
				rule.addComparator(new MatchableTableRowToKBComparator<>(kbColumn, new DeviationSimilarity(),0.0));
				break;
			case date:
				rule.addComparator(new MatchableTableRowToKBComparator<>(kbColumn, new WeightedDateSimilarity(1, 3, 5),0.0));
				break;
			case link:
				rule.addComparator(new MatchableTableRowToKBComparator<>(kbColumn, new EqualsSimilarity<>(), 0.0));
				break;
			default:
				rule.addComparator(new MatchableTableRowToKBComparator<>(kbColumn, new TokenizingJaccardSimilarity(),0.0));
				rule.addComparator(new MatchableTableRowToKBComparator<>(kbColumn, new MaximumOfTokenContainment(),0.0));
	//					rule.addComparator(new MatchableTableRowToKBComparator<>(kbColumn, new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5), 0.0));
				if(weights!=null) {
					rule.addComparator(new MatchableTableRowToKBComparator<>(kbColumn, new VectorSimilarity(weights, new VectorSpaceCosineSimilarity()), 0.0));
				}
				break;
			}
			
		}

		if(verbose) {
			System.out.println(String.format("[MatchingRuleGenerator] %f <= %s", valueSimilarityThreshold, rule.toString()));
		}
		
		return rule;
	}
}
