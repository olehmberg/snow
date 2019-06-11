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
package de.uni_mannheim.informatik.dws.tnt.match;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.dws.winter.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.CountAggregator;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CorrespondenceFormatter {

	public static void printAttributeClusters(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), 1.0));
		}
		
		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clustering = clusterer.createResult();
		
		System.out.println("Attribute Clusters:");
		for(Collection<MatchableTableColumn> cluster : clustering.keySet()) {
			Set<String> headers = new HashSet<>(Q.project(cluster, new MatchableTableColumn.ColumnHeaderProjection()));
			Set<String> ids = new HashSet<>(Q.project(cluster, new MatchableTableColumn.ColumnIdProjection()));
			System.out.println(String.format("\t[%d]%s (%s)", 
				cluster.size(),
				StringUtils.join(headers, ","),
				StringUtils.join(Q.sort(ids), ",")
			));
		}
	}
	
	public static void printTableLinkageFrequency(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		Processable<Pair<Pair<Integer, Integer>, Integer>> freq = schemaCorrespondences.aggregate(new RecordKeyValueMapper<Pair<Integer, Integer>, Correspondence<MatchableTableColumn,Matchable>, Correspondence<MatchableTableColumn,Matchable>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(Correspondence<MatchableTableColumn, Matchable> record,
					DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) {
				resultCollector.next(
						new Pair<Pair<Integer,Integer>, Correspondence<MatchableTableColumn,Matchable>>(
								new Pair<Integer,Integer>(record.getFirstRecord().getDataSourceIdentifier(), record.getSecondRecord().getDataSourceIdentifier()),
								record));
			}
		}, new CountAggregator<Pair<Integer, Integer>, Correspondence<MatchableTableColumn,Matchable>>());
		
		SimilarityMatrix<Integer> m = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
		for(Pair<Pair<Integer, Integer>, Integer> f : freq.get()) {
			m.add(f.getFirst().getFirst(), f.getFirst().getSecond(), (double)f.getSecond());
//			System.out.println(String.format("\t%d <-> %d\t%d", f.getFirst().getFirst(), f.getFirst().getSecond(), f.getSecond()));
		}
		
		System.out.println("Table Linkage Frequencies:");
		System.out.println(m.getOutput(5));
	}
	
}
