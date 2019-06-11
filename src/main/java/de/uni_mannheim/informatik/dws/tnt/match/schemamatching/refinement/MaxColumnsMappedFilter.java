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
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.DistinctTableProvenanceFilter;
import de.uni_mannheim.informatik.dws.winter.clustering.PartitioningWithPositiveAndNegativeEdges;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
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
 * 
 * Filters schema correspondences s.t. only correspondences between tables remain where the key of one table is contained in the key of the other table.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MaxColumnsMappedFilter {
	
	private DataSet<MatchableTableDeterminant, MatchableTableColumn> clusteredCandidateKeys = null;
	private boolean enforceDistinctProvenance = true;
	private boolean propagateKeys = false;
	private boolean propagateDependencies = false;
	private File logDirectory = null;
	
	/**
	 * @return the clusteredCandidateKeys
	 */
	public DataSet<MatchableTableDeterminant, MatchableTableColumn> getClusteredCandidateKeys() {
		return clusteredCandidateKeys;
	}

	/**
	 * Enforces that no two tables with shared provenance (i.e., were created from the same table at any point) can be merged by filtering out all their correspondences
	 * @param enforceDistinctProvenance the enforceDistinctProvenance to set
	 */
	public void setEnforceDistinctProvenance(boolean enforceDistinctProvenance) {
		this.enforceDistinctProvenance = enforceDistinctProvenance;
	}

	/**
	 * @param propagateKeys the propagateKeys to set
	 */
	public void setPropagateKeys(boolean propagateKeys) {
		this.propagateKeys = propagateKeys;
	}

	/**
	 * @param propagateDependencies the propagateDependencies to set
	 */
	public void setPropagateDependencies(boolean propagateDependencies) {
		this.propagateDependencies = propagateDependencies;
	}

	/**
	 * @param logDirectory the logDirectory to set
	 */
	public void setLogDirectory(File logDirectory) {
		this.logDirectory = logDirectory;
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> run(
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			// Processable<MatchableTableColumn> attributes,
			Collection<Table> tables) {
		
		if(enforceDistinctProvenance) {
			// run provenance filter to filter out pairs with the same provenance
			DistinctTableProvenanceFilter provenanceFilter = new DistinctTableProvenanceFilter();
			schemaCorrespondences = provenanceFilter.run(schemaCorrespondences, tables);
		}

		// find the matching determinants
		Processable<Triple<Integer, Integer, Double>> tableCorrespondences = schemaCorrespondences
			// group correspondences by table combination
			.group((Correspondence<MatchableTableColumn, Matchable> record, DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				})
			// check if all columns are mapped and create correspondences
			.map(
				(Group<Pair<Integer,Integer>,Correspondence<MatchableTableColumn,Matchable>> p)
				-> {
					Set<MatchableTableColumn> leftMapped = new HashSet<>();
					Set<MatchableTableColumn> rightMapped = new HashSet<>();

					boolean fkMapped = false;

					for(Correspondence<MatchableTableColumn, Matchable> cor : p.getRecords().get()) {

						if(!ContextColumns.isUnionContextColumn(cor.getFirstRecord().getHeader())) {

							leftMapped.add(cor.getFirstRecord());
							rightMapped.add(cor.getSecondRecord());

						}

						if("FK".equals(cor.getFirstRecord().getHeader()) && "FK".equals(cor.getSecondRecord().getHeader())) {
							fkMapped = true;
						}
					}
					

					if(
						fkMapped &&		// make sure the tables belong to the same entity table
						leftMapped.size()>1
						) {
							// use the number of correspondences between the tables as score
							// - so tables with more overlap will be preferred in the table-level clustering
							double score = leftMapped.size();
							return new Triple<>(p.getKey().getFirst(), p.getKey().getSecond(), score);
					} else {
						return new Triple<>(p.getKey().getFirst(), p.getKey().getSecond(), -1.0);
					}
				});

		Map<Integer, Integer> tableIdToCluster = new HashMap<>();

		// create table-level clusters
		PartitioningWithPositiveAndNegativeEdges<Integer> tableClusterer = new PartitioningWithPositiveAndNegativeEdges<>(0.0);
		Map<Collection<Integer>, Integer> tableClustering = tableClusterer.cluster(tableCorrespondences.get());
		
		if(logDirectory!=null) {
			logTableLevelCorrespondences(tableCorrespondences, tables, new File(logDirectory, "MaxColumnsMapped_tablecorrespondences.net"), tableClustering);
		}

		Map<Integer, Table> tableMap = Q.map(tables, (t)->t.getTableId());
		int clusterId = 0;
		for(Collection<Integer> cluster : tableClustering.keySet()) {
			System.out.println(String.format("[MaxColumnsMappedFilter] merged cluster %d: %s", clusterId, StringUtils.join(cluster, ",")));
			for(Integer key : cluster) {
				Table t = tableMap.get(key);
				tableIdToCluster.put(key, clusterId);
				System.out.println(String.format("[MaxColumnsMappedFilter]\t\t#%d: {%s}", key, 
						StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
				));
			}
			clusterId++;
		}

		 // keep only correspondences which are in the same table cluster
		 return schemaCorrespondences
		 // very important: compare intValue() here, as the result of == is undefined for Integers > 127
			 .where((c)-> {
				Integer cluster1 = tableIdToCluster.get(c.getFirstRecord().getTableId());
				Integer cluster2 = tableIdToCluster.get(c.getSecondRecord().getTableId());
				return cluster1!=null && cluster2!=null && cluster1.intValue()==cluster2.intValue();
			 });
		 	
	}

	protected Pair<Set<TableColumn>,Set<TableColumn>> translateFD(Pair<Set<TableColumn>,Set<TableColumn>> fd, Table from, Table to, Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		Set<TableColumn> det = new HashSet<>();
		det.addAll(translateColumns(fd.getFirst(), from, to, correspondences));
	
		if(det.size()!=fd.getFirst().size()) {
			 return null;
		} else {
			Set<TableColumn> dep = new HashSet<>();
			dep.addAll(translateColumns(fd.getSecond(), from, to, correspondences));
			return new Pair<>(det, dep);
		}
	}

	protected Collection<TableColumn> translateColumns(Collection<TableColumn> columns, Table from, Table to, Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		Set<TableColumn> result = new HashSet<>();
		for(TableColumn c : columns) {
			Correspondence<MatchableTableColumn, Matchable> correspondence = correspondences
				.where(
					(cor)->
						cor.getFirstRecord().getIdentifier().equals(c.getIdentifier()) && cor.getSecondRecord().getTableId()==to.getTableId() 
						|| 
						cor.getSecondRecord().getIdentifier().equals(c.getIdentifier()) && cor.getFirstRecord().getTableId()==to.getTableId())
				.firstOrNull();
			if(correspondence!=null) {
				if(correspondence.getFirstRecord().getIdentifier().equals(c.getIdentifier())) {
					result.addAll(Q.where(to.getColumns(), (col)->col.getIdentifier().equals(correspondence.getSecondRecord().getIdentifier())));
				} else if (correspondence.getSecondRecord().getIdentifier().equals(c.getIdentifier())) {
					result.addAll(Q.where(to.getColumns(), (col)->col.getIdentifier().equals(correspondence.getFirstRecord().getIdentifier())));
				}
			}
		}
		return result;
	}

	protected void logCorrespondences(Processable<Triple<MatchableTableDeterminant, MatchableTableDeterminant, Double>> correspondences, File f, Map<Collection<MatchableTableDeterminant>,MatchableTableDeterminant> clustering)  {
		Map<MatchableTableDeterminant, Node<String>> tableNodes = new HashMap<>();
		
		Graph<Node<String>, Edge<String, String>> graph = new Graph<>();
		Partitioning<Node<String>> pClustering = new Partitioning<>(graph);
		Graph<Node<String>, Edge<String, String>> positiveGraph = new Graph<>();
		Graph<Node<String>, Edge<String, String>> negativeGraph = new Graph<>();
		
		int nodeId = 0;
		for(MatchableTableDeterminant det : correspondences
			.map(
				(Triple<MatchableTableDeterminant, MatchableTableDeterminant, Double> cor, DataIterator<MatchableTableDeterminant> col)
				->{ col.next(cor.getFirst()); col.next(cor.getSecond()); })
			.distinct()
			.get()) {
				Node<String> n = new Node<String>(String.format("#%d: %s", det.getTableId(), MatchableTableColumn.formatCollection(det.getColumns())), nodeId++);
				tableNodes.put(det, n);
		}

		for(Triple<MatchableTableDeterminant, MatchableTableDeterminant, Double> cor : correspondences.get()) {
			
			Node<String> n1 = tableNodes.get(cor.getFirst());
			Node<String> n2 = tableNodes.get(cor.getSecond());
		
			graph.addEdge(n1, n2, new Edge<String, String>(n1, n2, "", 1.0), 1.0);
			if(cor.getThird()>=0) {
				positiveGraph.addEdge(n1, n2, new Edge<String, String>(n1, n2, "", 1.0), 1.0);
			} else {
				negativeGraph.addEdge(n1, n2, new Edge<String, String>(n1, n2, "", 1.0), 1.0);
			}
		}
		
		int clusterId = 1;
		for(Collection<MatchableTableDeterminant> cluster : clustering.keySet()) {
			// if(log) {
			// 	System.out.println(String.format("[BothTablesFullyMapped] Table cluster %d: {%s}", 
			// 		clusterId,
			// 		StringUtils.join(
			// 			Q.project(Q.where(tables, (t)->cluster.contains(t.getTableId())), 
			// 				(t)->StringUtils.join(Q.project(t.getColumns(), (c)->c.toString()), ",")
			// 				)
			// 			, " / ")
			// 	));
			// }

			for(MatchableTableDeterminant node : cluster) {
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
		
			graph.addEdge(n1, n2, new Edge<String, String>(n1, n2, "", cor.getThird()), cor.getThird());
			if(cor.getThird()>=0) {
				positiveGraph.addEdge(n1, n2, new Edge<String, String>(n1, n2, "", cor.getThird()), cor.getThird());
			} else {
				negativeGraph.addEdge(n1, n2, new Edge<String, String>(n1, n2, "", cor.getThird()), cor.getThird());
			}
		}
		
		int clusterId = 1;
		for(Collection<Integer> cluster : clustering.keySet()) {
			// if(log) {
			// 	System.out.println(String.format("[BothTablesFullyMapped] Table cluster %d: {%s}", 
			// 		clusterId,
			// 		StringUtils.join(
			// 			Q.project(Q.where(tables, (t)->cluster.contains(t.getTableId())), 
			// 				(t)->StringUtils.join(Q.project(t.getColumns(), (c)->c.toString()), ",")
			// 				)
			// 			, " / ")
			// 	));
			// }

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