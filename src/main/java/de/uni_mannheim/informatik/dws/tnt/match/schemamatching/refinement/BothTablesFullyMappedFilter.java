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
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.DistinctTableProvenanceFilter;
import de.uni_mannheim.informatik.dws.winter.clustering.PartitioningWithPositiveAndNegativeEdges;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Edge;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Graph;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Node;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Partitioning;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * implementation is overkill for the task, but it's just a copy of OneTableFullyMappedFilter with a small change
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class BothTablesFullyMappedFilter {
	
	private File logDirectory;
	private boolean log = false;
	private boolean enforceDistinctProvenance = true;
	public void setLogDirectory(File logDirectory) {
		this.logDirectory = logDirectory;
	}
	public void setLog(boolean log) {
		this.log = log;
	}
	
	/**
	 * Enforces that no two tables with shared provenance (i.e., were created from the same table at any point) can be merged by filtering out all their correspondences
	 * @param enforceDistinctProvenance the enforceDistinctProvenance to set
	 */
	public void setEnforceDistinctProvenance(boolean enforceDistinctProvenance) {
		this.enforceDistinctProvenance = enforceDistinctProvenance;
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> run(
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			Collection<Table> tables) {
		
		// create table-level edges from correspondences and negative edges from violations
		// create a clustering that does not include any violation edges inside the clusters
		// algorithm (basically the same as in table synthesis for partitioning):
		// - start with graph of tables with positive edges (similarity) and negative edges (violations)
		// - choose the strongest positive edge to merge two partitions, such that there is no negative edge between them
		// - merge the partitions
		// - update all edges
		// - repeat until no more partitions can be merged
		
		Map<Integer, Table> tablesById = Q.map(tables, (t)->t.getTableId());
		
		// create table-level correspondences if criterion is fulfilled
		Processable<Triple<Integer, Integer, Double>> tableCorrespondences = schemaCorrespondences
			// group correspondences by table combination
			.group((Correspondence<MatchableTableColumn, Matchable> record, DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				})
			.map((Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>> record, DataIterator<Triple<Integer, Integer, Double>> resultCollector) 
				-> {
					Set<String> leftMapped = new HashSet<>();
					Set<String> rightMapped = new HashSet<>();
					
					Table leftTable = tablesById.get(record.getKey().getFirst());
					Table rightTable = tablesById.get(record.getKey().getSecond());
					
					for(Correspondence<MatchableTableColumn, Matchable> cor : record.getRecords().get()) {

						if(!ContextColumns.isDisambiguationColumn(cor.getFirstRecord().getHeader()) && !ContextColumns.isDisambiguationColumn(cor.getSecondRecord().getHeader())) {
							leftMapped.add(cor.getFirstRecord().getIdentifier());
							rightMapped.add(cor.getSecondRecord().getIdentifier());
						}
					}
					
					int leftColumns = Q.where(leftTable.getColumns(), (c)->!ContextColumns.isDisambiguationColumn(c.getHeader())).size();
					int rightColumns = Q.where(rightTable.getColumns(), (c)->!ContextColumns.isDisambiguationColumn(c.getHeader())).size();

					if(leftMapped.size()==leftColumns && rightMapped.size()==rightColumns) {
						resultCollector.next(new Triple<>(leftTable.getTableId(), rightTable.getTableId(), 1.0));
						
						if(log) {
							System.out.println(String.format("[BothTablesFullyMappedFilter] Keeping correspondences between #%d and #%d:\n\t{%s}/{%s} mapped: {%s}\n\t{%s}/{%s} mapped: {%s}", 
								leftTable.getTableId(),
								rightTable.getTableId(),
								StringUtils.join(Q.project(Q.where(leftTable.getColumns(), new ContextColumns.IsNoContextColumnPredicate()), new TableColumn.ColumnHeaderProjection()), ","),
								StringUtils.join(Q.project(Q.where(leftTable.getColumns(), new ContextColumns.IsNoContextColumnPredicate()), new TableColumn.ColumnIdentifierProjection()), ","),
								StringUtils.join(Q.sort(leftMapped), ","),
								StringUtils.join(Q.project(Q.where(rightTable.getColumns(), new ContextColumns.IsNoContextColumnPredicate()), new TableColumn.ColumnHeaderProjection()), ","),
								StringUtils.join(Q.project(Q.where(rightTable.getColumns(), new ContextColumns.IsNoContextColumnPredicate()), new TableColumn.ColumnIdentifierProjection()), ","),
								StringUtils.join(Q.sort(rightMapped), ",")
								));
						}
					} else {
						resultCollector.next(new Triple<>(leftTable.getTableId(), rightTable.getTableId(), -1.0));
						
						if(log) {
							System.out.println(String.format("[BothTablesFullyMappedFilter] Filtering out correspondences between #%d and #%d:\n\t{%s}/{%s} mapped: {%s}\n\t{%s}/{%s} mapped: {%s}", 
								leftTable.getTableId(),
								rightTable.getTableId(),
								StringUtils.join(Q.project(Q.where(leftTable.getColumns(), new ContextColumns.IsNoContextColumnPredicate()), new TableColumn.ColumnHeaderProjection()), ","),
								StringUtils.join(Q.project(Q.where(leftTable.getColumns(), new ContextColumns.IsNoContextColumnPredicate()), new TableColumn.ColumnIdentifierProjection()), ","),
								StringUtils.join(Q.sort(leftMapped), ","),
								StringUtils.join(Q.project(Q.where(rightTable.getColumns(), new ContextColumns.IsNoContextColumnPredicate()), new TableColumn.ColumnHeaderProjection()), ","),
								StringUtils.join(Q.project(Q.where(rightTable.getColumns(), new ContextColumns.IsNoContextColumnPredicate()), new TableColumn.ColumnIdentifierProjection()), ","),
								StringUtils.join(Q.sort(rightMapped), ",")
								));
						}
						
//						StringBuilder sb = new StringBuilder();
//						sb.append(String.format("Correspondences between tables #%d and #%d filtered out\n", record.getKey().getFirst(), record.getKey().getSecond()));
//						for(TableColumn c : Q.where(leftTable.getColumns(), (c)->!leftMapped.contains(c.getIdentifier()))) {
//							sb.append(String.format("\t{#%d} unmapped: %s\n", record.getKey().getFirst(), c));
//						}
//						for(TableColumn c : Q.where(rightTable.getColumns(), (c)->!rightMapped.contains(c.getIdentifier()))) {
//							sb.append(String.format("\t{#%d} unmapped: %s\n", record.getKey().getSecond(), c));
//						}
//						
//						System.out.println(sb.toString());
					}
				});
		
		if(enforceDistinctProvenance) {
			// run provenance filter to generate negative edges
			DistinctTableProvenanceFilter provenanceFilter = new DistinctTableProvenanceFilter();
			provenanceFilter.setReturnRemovedCorrespondences(true);
			provenanceFilter.setLog(log);
			Processable<Triple<Integer, Integer, Double>> negativeByProvenance = provenanceFilter.run(schemaCorrespondences, tables)
				.map((Correspondence<MatchableTableColumn, Matchable> record, DataIterator<Triple<Integer, Integer, Double>> resultCollector) 
					-> {
						resultCollector.next(new Triple<Integer, Integer, Double>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId(), -1.0));
					});
			
			tableCorrespondences = tableCorrespondences.append(negativeByProvenance);
		}
		
		// create table-level clusters
		PartitioningWithPositiveAndNegativeEdges<Integer> partitioning = new PartitioningWithPositiveAndNegativeEdges<>(0.0);
//		partitioning.setLog(log);
		Map<Collection<Integer>, Integer> clustering = partitioning.cluster(tableCorrespondences.get());
		 
		 Map<Integer, Integer> tableIdToCluster = new HashMap<>();
		 int clusterId = 0;
		 for(Collection<Integer> cluster : clustering.keySet()) {
			 for(Integer tId : cluster) {
				 tableIdToCluster.put(tId, clusterId);
			 }
			 clusterId++;
		 }
		 
		 // assign tables without schema correspondences to singleton clusters
		 for(Integer tableId : tablesById.keySet()) {
			 if(!tableIdToCluster.containsKey(tableId)) {
				 tableIdToCluster.put(tableId, clusterId++);
			 }
		 }

		 if(logDirectory!=null) {
			 logTableLevelCorrespondences(tableCorrespondences, tables, new File(logDirectory, "BothTablesFullyMappedFilter_TableCorrespondences.net"), clustering);
		 }

		 // keep only correspondences which are in the same table cluster
		 return schemaCorrespondences
		 // very important: compare intValue() here, as the result of == is undefined for Integers > 127
		 	.where((c)->tableIdToCluster.get(c.getFirstRecord().getTableId()).intValue()==tableIdToCluster.get(c.getSecondRecord().getTableId()).intValue());
		 	
	}

	protected void logTableLevelCorrespondences(Processable<Triple<Integer, Integer, Double>> tableCorrespondences, Collection<Table> tables, File f, Map<Collection<Integer>,Integer> clustering)  {
		Map<Integer, Table> tableMap = Q.map(tables, (t)->t.getTableId());
		Map<Integer, Node<String>> tableNodes = new HashMap<>();
		
		Graph<Node<String>, Edge<String, String>> graph = new Graph<>();
		Partitioning<Node<String>> pClustering = new Partitioning<>(graph);
		Graph<Node<String>, Edge<String, String>> positiveGraph = new Graph<>();
		Graph<Node<String>, Edge<String, String>> negativeGraph = new Graph<>();
		
		for(Triple<Integer, Integer, Double> cor : tableCorrespondences.get()) {
			
			Node<String> n1 = tableNodes.get(cor.getFirst());
			if(n1==null) {
				Table t = tableMap.get(cor.getFirst());
				String cols = StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",");
				n1 = new Node<String>(String.format("#%d: %s", cor.getFirst(), cols), cor.getFirst());
				tableNodes.put(cor.getFirst(), n1);
			}
			
			Node<String> n2 = tableNodes.get(cor.getSecond());
			if(n2==null) {
				Table t = tableMap.get(cor.getSecond());
				String cols = StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",");
				n2 = new Node<String>(String.format("#%d: %s", cor.getSecond(), cols), cor.getSecond());
				tableNodes.put(cor.getSecond(), n2);
			}
		
			graph.addEdge(n1, n2, new Edge<String, String>(n1, n2, "", 1.0), 1.0);
			if(cor.getThird()>=0) {
				positiveGraph.addEdge(n1, n2, new Edge<String, String>(n1, n2, "", 1.0), 1.0);
			} else {
				negativeGraph.addEdge(n1, n2, new Edge<String, String>(n1, n2, "", 1.0), 1.0);
			}
		}
		
		int clusterId = 1;
		for(Collection<Integer> cluster : clustering.keySet()) {
			if(log) {
				System.out.println(String.format("[BothTablesFullyMapped] Table cluster %d: {%s}", 
					clusterId,
					StringUtils.join(
						Q.project(Q.where(tables, (t)->cluster.contains(t.getTableId())), 
							(t)->StringUtils.join(Q.project(t.getColumns(), (c)->c.toString()), ",")
							)
						, " / ")
				));
			}

			for(Integer node : cluster) {
				Node<String> n = tableNodes.get(node);
				pClustering.setPartition(n,clusterId);
			}
			clusterId++;
		}

		try {
//			System.out.println("[OneTableFullyMappedFilter] writing table graph");
			graph.writePajekFormat(f);
			pClustering.writePajekFormat(new File(f.getAbsolutePath() + "_cluster_assignment.clu"));
//			System.out.println("[OneTableFullyMappedFilter] writing positive table graph");
			positiveGraph.writePajekFormat(new File(f.getAbsolutePath() + "_positive_edges.net"));
//			System.out.println("[OneTableFullyMappedFilter] writing negative graph");
			negativeGraph.writePajekFormat(new File(f.getAbsolutePath() + "_negative_edges.net"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}