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
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.clustering.PartitioningWithPositiveAndNegativeEdges;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Edge;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Graph;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Node;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Partitioning;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * 
 * Filters schema correspondences s.t. only correspondences between tables with fully mapped primary keys remain.
 * Expects the candidate key collection (Table.getSchema().getCandidateKeys()) to contain only a single key.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class BothKeysFullyMappedFilter {
	
	private DataSet<MatchableTableDeterminant, MatchableTableColumn> clusteredCandidateKeys = null;
	private boolean enforceDistinctProvenance = true;
	private boolean propagateKeys = false;
	
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

	public Processable<Correspondence<MatchableTableColumn, Matchable>> run(
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			Processable<MatchableTableColumn> attributes,
			Collection<Table> tables) {
		
		Processable<MatchableTableDeterminant> determinants = new ParallelProcessableCollection<>();

		// create the keys that we want to match
		for(Table t : tables) {
			TableColumn fk =Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
			for(Collection<TableColumn> key : t.getSchema().getCandidateKeys()) {
				if(key.contains(fk)) {
					Collection<String> detIds = Q.project(key, (c)->c.getIdentifier());

					Collection<TableColumn> closure = t.getColumns();
					Collection<String> depIds = Q.project(closure, (c)->c.getIdentifier());

					MatchableTableDeterminant mdet = new MatchableTableDeterminant(
						t.getTableId(), 
						new HashSet<>(attributes.where((c)->detIds.contains(c.getIdentifier())).get()),
						new HashSet<>(attributes.where((c)->depIds.contains(c.getIdentifier())).get()),
						new HashSet<>(t.getColumns()).equals(new HashSet<>(closure))
						);

					if(mdet.getColumns().size()>0) {
						determinants.add(mdet);

						// System.out.println(String.format("[DeterminantSelector] Functional dependency for table #%d %s: {%s} -> {%s}", 
						//     t.getTableId(),
						//     t.getPath(),
						//     StringUtils.join(Q.project(det, new TableColumn.ColumnHeaderProjection()), ","),
						//     StringUtils.join(Q.project(dep, new TableColumn.ColumnHeaderProjection()), ",")
						// ));
					} else {
						System.out.println(String.format("[BothKeysFullyMappedFilter] unmapped determinant: %s",
							StringUtils.join(Q.project(key, new TableColumn.ColumnHeaderProjection()), ",")
							));
					}
				}
			}
		}

		if(enforceDistinctProvenance) {
			// run provenance filter to filter out pairs with the same provenance
			DistinctTableProvenanceFilter provenanceFilter = new DistinctTableProvenanceFilter();
			schemaCorrespondences = provenanceFilter.run(schemaCorrespondences, tables);
		}

		// find the matching determinants
		Processable<Triple<MatchableTableDeterminant, MatchableTableDeterminant, Double>> determinantCorrespondences = schemaCorrespondences
			// group correspondences by table combination
			.group((Correspondence<MatchableTableColumn, Matchable> record, DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				})
			// join the keys to the left table
			.join(determinants, (g)->g.getKey().getFirst(), (d)->d.getTableId())
			// join the keys to the right table
			.join(determinants, (p)->p.getFirst().getKey().getSecond(), (d)->d.getTableId())
			// check if all columns are mapped and create correspondences
			.map(
				(Pair<Pair<Group<Pair<Integer,Integer>,Correspondence<MatchableTableColumn,Matchable>>,MatchableTableDeterminant>,MatchableTableDeterminant> p)
				-> {
					MatchableTableDeterminant d1 = p.getFirst().getSecond();
					MatchableTableDeterminant d2 = p.getSecond();

					if(d1.getTableId()!=d2.getTableId()) {

						Set<MatchableTableColumn> leftMapped = new HashSet<>();
						Set<MatchableTableColumn> rightMapped = new HashSet<>();
						
						for(Correspondence<MatchableTableColumn, Matchable> cor : p.getFirst().getFirst().getRecords().get()) {
							if(d1.getColumns().contains(cor.getFirstRecord())
								&& d2.getColumns().contains(cor.getSecondRecord())) {
									leftMapped.add(cor.getFirstRecord());
									rightMapped.add(cor.getSecondRecord());
							}
						}
						
						int leftColumns = d1.getColumns().size();
						int rightColumns = d2.getColumns().size();

						if(
							d1.getColumns().equals(leftMapped)      // d1 matches d2
							&& leftMapped.size()==leftColumns && rightMapped.size()==rightColumns     // all columns are mapped
							) {
								return new Triple<>(d1, d2, 1.0);
							}
					} 

					return null;
				});
		
		// keep a list of all candidate keys that were used in the clustering for each table
		// if a table is not part of any cluster, add all existing canddiate keys
		// -> use these lists as candidate keys for the stitched tables!
		Processable<MatchableTableDeterminant> candidateKeys = new ParallelProcessableCollection<>();

		Map<Integer, Integer> tableIdToCluster = new HashMap<>();


		// create key-level clusters
		ConnectedComponentClusterer<MatchableTableDeterminant> clusterer = new ConnectedComponentClusterer<>();
		Map<Collection<MatchableTableDeterminant>, MatchableTableDeterminant> clustering = clusterer.cluster(determinantCorrespondences.get());
		
		int clusterId = 0;
		for(Collection<MatchableTableDeterminant> cluster : clustering.keySet()) {
			System.out.println(String.format("[BothKeysFullyMappedFilter] cluster %d: %s", clusterId, StringUtils.join(Q.project(cluster, (d)->d.getTableId()), ",")));
			for(MatchableTableDeterminant key : cluster) {
				tableIdToCluster.put(key.getTableId(), clusterId);
				System.out.println(String.format("[BothKeysFullyMappedFilter]\t\t#%d: {%s}", key.getTableId(), MatchableTableColumn.formatCollection(key.getColumns())));
			}
			clusterId++;

			// all keys in the cluster were used, so keep them
			for(MatchableTableDeterminant d : cluster) {
				candidateKeys.add(d);
			}
		}
		if(propagateKeys) {
			// if we want to propagate keys, create the clusters based on table correspondences
			// if a table has determinants in multiple clusters, these clusters will be merged, effectively propagating the key to the merged clusters

			// create table-level clusters
			ConnectedComponentClusterer<Integer> tableClusterer = new ConnectedComponentClusterer<>();
			Map<Collection<Integer>, Integer> tableClustering = tableClusterer.cluster(Q.project(determinantCorrespondences.get(), (c)->new Triple<>(c.getFirst().getTableId(), c.getSecond().getTableId(), c.getThird())));
			
			clusterId = 0;
			for(Collection<Integer> cluster : tableClustering.keySet()) {
				System.out.println(String.format("[BothKeysFullyMappedFilter] merged cluster %d: %s", clusterId, StringUtils.join(cluster, ",")));
				for(Integer key : cluster) {

					tableIdToCluster.put(key, clusterId);
					System.out.println(String.format("[BothKeysFullyMappedFilter]\t\t#%d: {%s}", key, 
						StringUtils.join(
							determinants.where((d)->d.getTableId()==key.intValue()).map((d)->MatchableTableColumn.formatCollection(d.getColumns())).get()
							, " / ")
					));
				}
				clusterId++;
			}
		}

		 // add keys for all un-clustered tables
		 for(Table t : tables) {
			 if(!tableIdToCluster.containsKey(t.getTableId())) {
				 candidateKeys = candidateKeys.append(determinants.where((d)->d.getTableId()==t.getTableId()));
			 }
		 }

		clusteredCandidateKeys = new ParallelHashedDataSet<>(candidateKeys.get());

		 // keep only correspondences which are in the same table cluster
		 return schemaCorrespondences
		 // very important: compare intValue() here, as the result of == is undefined for Integers > 127
			 .where((c)-> {
				Integer cluster1 = tableIdToCluster.get(c.getFirstRecord().getTableId());
				Integer cluster2 = tableIdToCluster.get(c.getSecondRecord().getTableId());
				return cluster1!=null && cluster2!=null && cluster1.intValue()==cluster2.intValue();
			 });
		 	
	}

}