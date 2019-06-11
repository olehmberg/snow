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
package de.uni_mannheim.informatik.dws.tnt.match.entitystitching;

import java.util.*;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableDataSetLoader;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers.BlockingKeyCreatorByUniqueness;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.ClusteredMatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.MatchableTableRowBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.GreedyOneToOneMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardRecordBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.rules.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.MatchingRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.MaxScoreMatchingRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.WekaMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.UnadjustedDeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EntityTableDuplicateDetection {

	public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> detectDuplicates(Table entityTable) throws Exception {

		WebTableDataSetLoader loader = new WebTableDataSetLoader();
		
		BlockingKeyCreatorByUniqueness blockingKeyCreator = new BlockingKeyCreatorByUniqueness();
		Map<TableColumn, Double> uniqueness = blockingKeyCreator.calculateColumnUniqueness(entityTable);
		
		// create a dataset of records
		DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadDataSet(entityTable);
		
		MaxScoreMatchingRule<MatchableTableRow, MatchableTableColumn> matchingRule = new MaxScoreMatchingRule<>(0.0);
		Collection<TableColumn> blockingKeys = new HashSet<>();
		
		// match once for each candidate key
		// if records have missing values, not all records can be matched by every candidate key
		for(Collection<TableColumn> candidateKey : entityTable.getSchema().getCandidateKeys()) {
			blockingKeys.addAll(blockingKeyCreator.createBlockingKey(candidateKey, uniqueness));
			
			// create the matching rule
			LinearCombinationMatchingRule<MatchableTableRow, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(1.0);
				
			for(TableColumn c : candidateKey) {
				
				MatchableTableColumn columnToCompare = new MatchableTableColumn(entityTable.getTableId(), c);
				MatchableTableRowComparator comparator = new MatchableTableRowComparator(columnToCompare);
				rule.addComparator(comparator, 1.0);
				
			}
			
			rule.normalizeWeights();
			
			System.out.println(String.format("Matching rule uses {%s}", StringUtils.join(Q.project(candidateKey, new TableColumn.ColumnHeaderProjection()), ",")));
			
			matchingRule.addMatchingRule(rule);
		}
		
		// create the blocker
		MatchableTableRowBlockingKeyGenerator bkg  = new MatchableTableRowBlockingKeyGenerator(blockingKeys);
		StandardRecordBlocker<MatchableTableRow, MatchableTableColumn> blocker = new StandardRecordBlocker<>(bkg);
		System.out.println(String.format("Blocking via {%s}", StringUtils.join(Q.project(blockingKeys, new TableColumn.ColumnHeaderProjection()), ",")));
		
		// run the matching
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences = engine.runDuplicateDetection(records, matchingRule, blocker);
		
		System.out.println(String.format("Found %d duplicates", correspondences.size()));

		return correspondences;
	}
	
	public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> detectDuplicates(Collection<Table> entityTables) throws Exception {

		WebTableDataSetLoader loader = new WebTableDataSetLoader();
				
		// create a dataset of records
		DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(entityTables);
		
		// create the blocking keys (only block by label)
		Collection<TableColumn> blockingKeys = new HashSet<>();
		Map<String, Collection<MatchableTableColumn>> schemaClusters = new HashMap<>();
		for(Table t : entityTables) {
			TableColumn lbl = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"rdf-schema#label".equals(c.getHeader())));
			blockingKeys.add(lbl);

			for(TableColumn c : t.getColumns()) {
				if(!Q.toSet("uri","pk").contains(c.getHeader())) {
					Collection<MatchableTableColumn> cluster = MapUtils.get(schemaClusters, c.getHeader(), new LinkedList<>());
					// cluster.add(c);
					cluster.add(records.getSchema().getRecord(c.getIdentifier()));
				}
			}
		}

		// create the matching rule
		MatchingRule<MatchableTableRow, MatchableTableColumn> matchingRule = null;
		WekaMatchingRule<MatchableTableRow, MatchableTableColumn> wekaRule = new WekaMatchingRule<>(0.6, "SimpleLogistic", new String[] {});
		LinearCombinationMatchingRule<MatchableTableRow, MatchableTableColumn> linearRule = new LinearCombinationMatchingRule<>(0.6);

		for(String s : schemaClusters.keySet()) {
			Collection<MatchableTableColumn> cluster = schemaClusters.get(s);

			// get the data type 
			MatchableTableColumn first = Q.firstOrDefault(cluster);

			SimilarityMeasure<?> measure = null;
			switch(first.getType()) {
				case numeric:
					measure = new UnadjustedDeviationSimilarity();
					break;
				case date:
					WeightedDateSimilarity dateSim = new WeightedDateSimilarity(1, 3, 5);
					dateSim.setYearRange(10);
					measure = dateSim;
					break;
				default:
					measure = new TokenizingJaccardSimilarity();
			}

			ClusteredMatchableTableRowComparator<?> comparator = new ClusteredMatchableTableRowComparator<>(cluster, measure);

			wekaRule.addComparator(comparator);
			linearRule.addComparator(comparator, 1.0);
		}
		linearRule.normalizeWeights();
		
		// create the blocker
		//TODO the blocker currently uses exact cell value matching, which means that the rule has not much left to decide ...
		MatchableTableRowBlockingKeyGenerator bkg  = new MatchableTableRowBlockingKeyGenerator(blockingKeys);
		StandardRecordBlocker<MatchableTableRow, MatchableTableColumn> blocker = new StandardRecordBlocker<>(bkg);
		System.out.println(String.format("Blocking via {%s}", StringUtils.join(Q.project(blockingKeys, new TableColumn.ColumnHeaderProjection()), ",")));

		// run the matching
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();
		matchingRule = linearRule;
		System.out.println(String.format("Matching rule: %s", matchingRule.toString()));

		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences = engine.runIdentityResolution(
			records, 
			records, 
			null, 
			matchingRule, 
			blocker);
		
		System.out.println(String.format("Found %d duplicates", correspondences.size()));

		GreedyOneToOneMatchingAlgorithm<MatchableTableRow, MatchableTableColumn> oneToOne = new GreedyOneToOneMatchingAlgorithm<>(correspondences);
		oneToOne.run();
		correspondences = oneToOne.getResult();

		System.out.println(String.format("%d duplicates after global matching", correspondences.size()));

		return correspondences;

	}

}
