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

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.MaximumBipartiteMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SumDoubleAggregator;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class FullClusterBasedMaximumMatching {

	private boolean log = false;
	private KnowledgeBase kb;
	private File logDirectory;
	
	public void setLogDirectory(File logDirectory) {
		this.logDirectory = logDirectory;
	}
	
	public void setLog(boolean log) {
		this.log = log;
	}
	
	/**
	 * 
	 */
	public FullClusterBasedMaximumMatching(KnowledgeBase kb) {
		this.kb = kb;
	}
	
	public Processable<Correspondence<MatchableTableColumn, Matchable>> run(
			Processable<MatchableTableColumn> tableSchema,
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToTableCorrespondences,
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBCorrespondences) {

		// run a maximum mapping per table
		return clusteredMaximumMatchingPerTable(tableSchema, tableToTableCorrespondences, tableToKBCorrespondences);
	}
		
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> clusteredMaximumMatchingPerTable(
		Processable<MatchableTableColumn> tableSchema,
		Processable<Correspondence<MatchableTableColumn, Matchable>> tableToTableCorrespondences, 
		Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBCorrespondences) {
		// cluster all columns from the left schema using the correspondences
		// - a cluster contains all columns with correspondences among them (column-to-column)
		// - does not connect different clusters which are mapped to the same KB property!
		ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
		for(Correspondence<MatchableTableColumn, Matchable> cor : tableToTableCorrespondences.get()) {
			clusterer.addEdge(new Triple<>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		for(Correspondence<MatchableTableColumn, Matchable> cor : tableToKBCorrespondences.get()) {
			clusterer.addEdge(new Triple<>(cor.getFirstRecord(), cor.getFirstRecord(), 1.0));
		}
		// create cluster representatives
		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clustering = clusterer.createResult();
		Map<MatchableTableColumn, Collection<MatchableTableColumn>> representativeToCluster = new HashMap<>();
		Map<MatchableTableColumn, MatchableTableColumn> columnToClusterRepresentative = new HashMap<>();
		int clusterId = 0;
		for(Collection<MatchableTableColumn> cluster : clustering.keySet()) {
			MatchableTableColumn representative = Q.firstOrDefault(cluster);
			
			// create an artificial column to represent the cluster
			// - each clusters needs his own table id for the maximum matching step
			// - actually, overlapping clusters need the same data source id ...
			Table clusterTable = new Table();
			clusterTable.setTableId(clusterId++);
			clusterTable.setPath(Integer.toString(clusterTable.getTableId()));
			TableColumn clusterColumn = new TableColumn(0, clusterTable);
			clusterColumn.setHeader(representative.getHeader());
			representative = new MatchableTableColumn(clusterTable.getTableId(), clusterColumn);
			
			clustering.put(cluster, representative);
			representativeToCluster.put(representative, cluster);
			for(MatchableTableColumn col : cluster) {
				columnToClusterRepresentative.put(col, representative);
			}
		}
		
		System.out.println(String.format("[FullClusterBasedMaximumMatching] found %d web table column clusters", clustering.size()));
		if(log) {
			for(Collection<MatchableTableColumn> cluster : clustering.keySet()) {
				System.out.println(String.format("\t(%d)\t{%s}", cluster.size(), StringUtils.join(Q.project(cluster, (c)->c.toString()), ",")));
			}
		}
		
		
		// aggregate correspondences from clusters to KB (i.e. cluster-to-property correspondences)
		// this would prefer small clusters: the aggregated score is the average similarity score w.r.t. the cluster size (i.e., sum of scores / cluster size)
		// instead: use the sum of similarities for each cluster
		Processable<Correspondence<MatchableTableColumn, Matchable>> clusterLevelCorrespondences = tableToKBCorrespondences
			.aggregate((Correspondence<MatchableTableColumn, Matchable> record,DataIterator<Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Double>> resultCollector) 
					-> {
						MatchableTableColumn cluster = columnToClusterRepresentative.get(record.getFirstRecord());
						MatchableTableColumn cls = record.getSecondRecord();
						
						double score = record.getSimilarityScore(); // / (double)clusterColumns.size();
						
						resultCollector.next(new Pair<>(new Pair<>(cluster, cls), score));
					}
					, new SumDoubleAggregator<>())
			.map((Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Double> record,DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) 
					-> {
						resultCollector.next(new Correspondence<MatchableTableColumn, Matchable>(record.getFirst().getFirst(), record.getFirst().getSecond(), record.getSecond()));
					}
			);
		
		System.out.println(String.format("[FullClusterBasedMaximumMatching] aggregation resulted in %d column-cluster-to-KB-property correspondences", clusterLevelCorrespondences.size()));
		
		if(log) {
			for(Correspondence<MatchableTableColumn, Matchable> cor : clusterLevelCorrespondences.sort((c)->c.getSecondRecord().getIdentifier()).get()) {
				Collection<MatchableTableColumn> cluster = representativeToCluster.get(cor.getFirstRecord());
				System.out.println(String.format("\t%f %s: {%s}",
						cor.getSimilarityScore(),
						cor.getSecondRecord(),
						StringUtils.join(Q.project(cluster, new MatchableTableColumn.ColumnHeaderProjection()), ",")));
			}
		}

		// run a maximum matching on each table using the cluster scores

		// first create new table-to-kb correspondences with the cluster scores
		Processable<Correspondence<MatchableTableColumn, Matchable>> rescoredTableToKBCorrespondences = tableToKBCorrespondences
			.join(
				clusterLevelCorrespondences, 
				(c)->new Pair<>(columnToClusterRepresentative.get(c.getFirstRecord()), c.getSecondRecord()),
				(c)->new Pair<>(c.getFirstRecord(), c.getSecondRecord())
			)
			.map((p)->new Correspondence<>(p.getFirst().getFirstRecord(), p.getSecond().getSecondRecord(), p.getSecond().getSimilarityScore()));

		if(log) {
			System.out.println(String.format("[FullClusterBasedMaximumMatching] %d re-scored column-to-KB-property correspondences", rescoredTableToKBCorrespondences.size()));
			// print individual scores per table
			for(Correspondence<MatchableTableColumn, Matchable> cor : rescoredTableToKBCorrespondences
				.sort((c)->c.getSimilarityScore())
				.sort((c)->c.getFirstRecord().getColumnIndex())
				.sort((c)->c.getFirstRecord().getTableId())
				.get()
			) {
				System.out.println(String.format("\t%f %s -> %s",
						cor.getSimilarityScore(),
						cor.getFirstRecord(),
						cor.getSecondRecord()
				));
			}
		}	

		// create a global matching on the column correspondences per table / KB class
		MaximumBipartiteMatchingAlgorithm<MatchableTableColumn, Matchable> maximumMatching = new MaximumBipartiteMatchingAlgorithm<>(rescoredTableToKBCorrespondences);
		maximumMatching.setGroupByRightDataSource(true);
		maximumMatching.setGroupByLeftDataSource(true);
		maximumMatching.run();
		rescoredTableToKBCorrespondences = maximumMatching.getResult();
		
		System.out.println(String.format("[FullClusterBasedMaximumMatching] maximal matching resulted in %d column-to-KB-property correspondences", rescoredTableToKBCorrespondences.size()));
		
		// only consider a mapping to a class if rdfs:label is mapped
		LabelMappedConstraint labelMapped = new LabelMappedConstraint(log);
		rescoredTableToKBCorrespondences = labelMapped.applyLabelMappedConstraint(rescoredTableToKBCorrespondences);

		System.out.println(String.format("[FullClusterBasedMaximumMatching] label-mapped filter resulted in %d column-to-KB-property correspondences", rescoredTableToKBCorrespondences.size()));

		// choose the correspondences for the class with the highest total score for each table
		// the total score is the sum of all correspondences to a certain class
		Processable<Pair<Integer, Pair<Integer,Double>>> classScores = rescoredTableToKBCorrespondences
			.aggregate
			((Correspondence<MatchableTableColumn, Matchable> record, DataIterator<Pair<Pair<Integer, Integer>, Double>> resultCollector)
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record.getSimilarityScore()));
				}
			, new SumDoubleAggregator<>())
			.map((p)->new Pair<>(p.getFirst().getFirst(), new Pair<>(p.getFirst().getSecond(), p.getSecond())));
			// .sort((p)->p.getSecond(), false);
		
		Processable<Group<Integer, Pair<Integer, Double>>> classScoresByTable = classScores.group((p,col)->col.next(new Pair<>(p.getFirst(), p.getSecond())));

		if(log) {
			System.out.println("[FullClusterBasedMaximumMatching] Class scores after maximal matching: ");
			// Map<Integer, Pair<Integer, Double>> classScoresByTable = Pair.toMap(classScores.get());
			for(Group<Integer, Pair<Integer, Double>> tableScores : classScoresByTable.sort((g)->g.getKey()).get()) {
				System.out.println(String.format("[FullClusterBasedMaximumMatching]\t#%d\t%s", 
					tableScores.getKey(),
					StringUtils.join(
						Q.<String,Pair<Integer, Double>>project(tableScores.getRecords().sort((p)->p.getSecond(),false).get(), (p)->String.format("%s (%f)",
							kb.getClassIndices().get(p.getFirst()),
							p.getSecond()
						))
						, " / ")
				));
			}
		}
		
		// choose the mapping for the class with the highest score for each table
		Map<Integer, Integer> maxClassPerTable = new HashMap<>();
		for(Group<Integer, Pair<Integer, Double>> tableScores : classScoresByTable.get()) {
			Pair<Integer, Double> maxClass = tableScores.getRecords().sort((p)->p.getSecond(), false).firstOrNull();
			maxClassPerTable.put(tableScores.getKey(), maxClass.getFirst());
		}
		
		rescoredTableToKBCorrespondences = rescoredTableToKBCorrespondences.where((c)->{
			Integer selectedClass = maxClassPerTable.get(c.getFirstRecord().getTableId());
			if(selectedClass!=null) {
				return c.getSecondRecord().getTableId()==selectedClass.intValue();
			} else {
				return false;
			}
		});
		
		System.out.println(String.format("[FullClusterBasedMaximumMatching] maximum column-to-KB-property matching resulted in %d correspondences", rescoredTableToKBCorrespondences.size()));
		
		return rescoredTableToKBCorrespondences;
	}

}
