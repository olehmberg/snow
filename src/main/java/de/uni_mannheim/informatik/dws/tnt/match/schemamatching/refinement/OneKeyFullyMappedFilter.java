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
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencySet;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
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
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SumDoubleAggregator;
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
 * Filters schema correspondences s.t. only correspondences between tables remain where the key of one table is contained in the key of the other table.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class OneKeyFullyMappedFilter {
	
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
						System.out.println(String.format("[OneKeyFullyMappedFilter] unmapped determinant: %s",
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
						Set<MatchableTableColumn> bothKeysMappedLeft = new HashSet<>();
						boolean isAllContextColumns = true;
						boolean d1AllContextColumns = true;
						boolean d2AllContextColumns = true;

						for(Correspondence<MatchableTableColumn, Matchable> cor : p.getFirst().getFirst().getRecords().get()) {
							leftMapped.add(cor.getFirstRecord());
							rightMapped.add(cor.getSecondRecord());

							if(d1.getColumns().contains(cor.getFirstRecord())
								&& d2.getColumns().contains(cor.getSecondRecord())) {
									bothKeysMappedLeft.add(cor.getFirstRecord());

									if(!ContextColumns.isContextColumn(cor.getFirstRecord()) && !"FK".equals(cor.getFirstRecord().getHeader())) {
										isAllContextColumns = false;
									}
							}

							if(d1.getColumns().contains(cor.getFirstRecord()) && !ContextColumns.isUnionContextColumn(cor.getFirstRecord().getHeader()) && !"FK".equals(cor.getFirstRecord().getHeader())) {
								d1AllContextColumns = false;
							}
							if(d2.getColumns().contains(cor.getSecondRecord()) && !ContextColumns.isUnionContextColumn(cor.getSecondRecord().getHeader()) && !"FK".equals(cor.getSecondRecord().getHeader())) {
								d2AllContextColumns = false;
							}
						}
						
						int leftColumns = d1.getColumns().size();
						int rightColumns = d2.getColumns().size();

						if(
							leftMapped.containsAll(d1.getColumns())		// d1 is completely mapped
							&& rightMapped.containsAll(d2.getColumns())	// d2 is completely mapped
							&& (
								 (
									bothKeysMappedLeft.size()>=d1.getColumns().size() && !d2AllContextColumns 	// d1 is contained in d2
									|| 										// either d1 or d2 is contained in the other key
									bothKeysMappedLeft.size()>=d2.getColumns().size() && !d1AllContextColumns	// d2 is contained in d1
								)
								||
								(	// alternative: the keys are equal
									// Problem: this will merge incompatible tables via transitivity
									// Solution: add negative edges on table level
									bothKeysMappedLeft.size()==d1.getColumns().size()
									&&
									bothKeysMappedLeft.size()==d2.getColumns().size()

								)
							)
							) {
								// use the number of correspondences between the tables as score
								// - so tables with more overlap will be preferred in the table-level clustering
								double score = leftMapped.size();
								return new Triple<>(d1, d2, score);
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
		PartitioningWithPositiveAndNegativeEdges<MatchableTableDeterminant> clusterer = new PartitioningWithPositiveAndNegativeEdges<>(0.0);
		Map<Collection<MatchableTableDeterminant>, MatchableTableDeterminant> clustering = clusterer.cluster(determinantCorrespondences.get());
		
		if(logDirectory!=null) {
			logCorrespondences(determinantCorrespondences, new File(logDirectory, "OneKeyFullyMapped_determinantcorrespondences.net"), clustering);
		}

		int clusterId = 0;
		for(Collection<MatchableTableDeterminant> cluster : clustering.keySet()) {
			System.out.println(String.format("[OneKeyFullyMappedFilter] cluster %d: %s", clusterId, StringUtils.join(Q.project(cluster, (d)->d.getTableId()), ",")));
			for(MatchableTableDeterminant key : cluster) {
				tableIdToCluster.put(key.getTableId(), clusterId);
				System.out.println(String.format("[OneKeyFullyMappedFilter]\t\t#%d: {%s}", key.getTableId(), MatchableTableColumn.formatCollection(key.getColumns())));
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

			// the determinantCorrespondences only contain positive edges
			// to avoid incompatible tables from being merged, we must add negative edges
			// two tables are incompatible if they have no compatible keys, i.e., no positive edge between them
			Processable<Triple<Integer, Integer, Double>> negativeEdges = determinants
				// create all possible combinations of determinants
				.join(determinants, (d)->1)
				// join the positive edges (undirected!)
				.leftJoin(
					determinantCorrespondences, 
					(p)->Q.toSet(p.getFirst().getTableId(), p.getSecond().getTableId()),
					(cor)->Q.toSet(cor.getFirst().getTableId(), cor.getSecond().getTableId()))
				// create a negative edge if no positive edge was joined
				.map(
					(joined) -> {
						if(joined.getSecond()==null) 
							return new Triple<>(joined.getFirst().getFirst().getTableId(), joined.getFirst().getSecond().getTableId(), -1.0);
						else
							return null;
					}
				);

			// transform determinant-level correspondences (positive edges) to table-level correspondences
			Processable<Triple<Integer, Integer, Double>> positiveEdges = determinantCorrespondences
					.aggregate(
						(t,col)->col.next(new Pair<>(new Pair<>(t.getFirst().getTableId(), t.getSecond().getTableId()), t.getThird())), 
						new SumDoubleAggregator<Pair<Integer, Integer>>())
					.map((g)->new Triple<>(g.getFirst().getFirst(), g.getFirst().getSecond(), g.getSecond()));

					// .map((t)->new Triple<>(t.getFirst().getTableId(), t.getSecond().getTableId(), t.getThird()));

			Processable<Triple<Integer, Integer, Double>> tableLevelEdges = positiveEdges.append(negativeEdges);

			// create table-level clusters
			// ConnectedComponentClusterer<Integer> tableClusterer = new ConnectedComponentClusterer<>();
			PartitioningWithPositiveAndNegativeEdges<Integer> tableClusterer = new PartitioningWithPositiveAndNegativeEdges<>(0.0);
			// Map<Collection<Integer>, Integer> tableClustering = tableClusterer.cluster(Q.project(determinantCorrespondences.get(), (c)->new Triple<>(c.getFirst().getTableId(), c.getSecond().getTableId(), c.getThird())));
			Map<Collection<Integer>, Integer> tableClustering = tableClusterer.cluster(tableLevelEdges.get());
			
			if(logDirectory!=null) {
				logTableLevelCorrespondences(tableLevelEdges, tables, new File(logDirectory, "OneKeyFullyMapped_tablecorrespondences.net"), tableClustering);
			}

			// group schema correspondences to speed up lookup for table pairs
			Processable<Group<Set<Integer>, Correspondence<MatchableTableColumn, Matchable>>> schemaCorrespondencesGroupedByTableCombination = schemaCorrespondences
				.group((cor,col)->col.next(new Pair<>(Q.toSet(cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId()), cor)));
			// Map<Set<Integer>, Group<Set<Integer>, Correspondence<MatchableTableColumn, Matchable>>> tableCombinationToCorrespondences = Q.map(schemaCorrespondencesGroupedByTableCombination.get(), (g)->g.getKey());
			Map<Set<Integer>, Map<String, MatchableTableColumn>> columnMappingByTables = new HashMap<>();
			for(Group<Set<Integer>, Correspondence<MatchableTableColumn, Matchable>> g : schemaCorrespondencesGroupedByTableCombination.get()) {
				Set<Integer> tableIds = g.getKey();
				Map<String, MatchableTableColumn> mapping = new HashMap<>();
				for(Correspondence<MatchableTableColumn, Matchable> cor : g.getRecords().get()) {
					mapping.put(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord());
					mapping.put(cor.getSecondRecord().getIdentifier(), cor.getFirstRecord());
				}
				columnMappingByTables.put(tableIds, mapping);
			}


			Map<Integer, Table> tableMap = Q.map(tables, (t)->t.getTableId());
			clusterId = 0;
			for(Collection<Integer> cluster : tableClustering.keySet()) {
				System.out.println(String.format("[OneKeyFullyMappedFilter] merged cluster %d: %s", clusterId, StringUtils.join(cluster, ",")));
				for(Integer key : cluster) {

					tableIdToCluster.put(key, clusterId);
					Collection<MatchableTableDeterminant> keyCluster = determinants.where((d)->d.getTableId()==key.intValue()).get();
					System.out.println(String.format("[OneKeyFullyMappedFilter]\t\t#%d: {%s}", key, 
						StringUtils.join(
							determinants.where((d)->d.getTableId()==key.intValue()).map((d)->MatchableTableColumn.formatCollection(d.getColumns())).get()
							, " / ")
					));

					// propagateKeys(tables, candidateKeys, keyCluster, schemaCorrespondences);
					propagateKeys(tables, candidateKeys, keyCluster, columnMappingByTables);

					if(propagateDependencies) {
						// propagate FDs
						Table t = tableMap.get(key);
						printFunctionalDependencies(t);
						FunctionalDependencySet fdSet = new FunctionalDependencySet(t.getSchema().getFunctionalDependencies());
						fdSet.setVerbose(true);
						for(Integer otherTableId : cluster) {
							if(otherTableId.intValue()!=key.intValue()) {
								Table otherTable = tableMap.get(otherTableId);

								// Collection<TableColumn> otherInT = translateColumns(otherTable.getColumns(), otherTable, t, schemaCorrespondences);
								Collection<TableColumn> otherInT = translateColumns(otherTable.getColumns(), otherTable, t, columnMappingByTables);
								Collection<TableColumn> otherIndependentInT;
								if(!otherTable.getSchema().getFunctionalDependencies().containsKey(new HashSet<>())) {
									otherIndependentInT = new HashSet<>();
								} else {
									// otherIndependentInT = translateColumns(otherTable.getSchema().getFunctionalDependencies().get(new HashSet<>()), otherTable, t, schemaCorrespondences);
									otherIndependentInT = translateColumns(otherTable.getSchema().getFunctionalDependencies().get(new HashSet<>()), otherTable, t, columnMappingByTables);
								}

								fdSet.setPropagationCondition((fd)->true);
								// propagate invalidation to all columns which are not in the other table
								fdSet.setPropagateInvalidationToDependants((fd)->Q.without(t.getColumns(), otherInT));
								// specialise with all columns which are not independent in the other table
								fdSet.setSpecialiseWith((fd)->Q.without(t.getColumns(), otherIndependentInT));

								// for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(otherTable.getSchema().getFunctionalDependencies())) {	
								for(Pair<Set<TableColumn>,Set<TableColumn>> fd : FunctionalDependencyUtils.split(Pair.fromMap(otherTable.getSchema().getFunctionalDependencies()))) {
									// Pair<Set<TableColumn>,Set<TableColumn>> translatedFD = translateFD(fd, otherTable, t, schemaCorrespondences);
									Pair<Set<TableColumn>,Set<TableColumn>> translatedFD = translateFD(fd, otherTable, t, columnMappingByTables);
									//Problem: we want to invalidate X -> Y
									// - but the other table only contains X,Z -> Y and Z is not in this table
									// - so we cannot translate the FD
									if(translatedFD!=null) {
										fdSet.addVerifiedMinimalFunctionalDependency(translatedFD);
									}
								}
							}
						}
						t.getSchema().setFunctionalDependencies(FunctionalDependencyUtils.canonicalCover(fdSet.getFunctionalDependencies()));
						printFunctionalDependencies(t);
					}
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

	protected void propagateKeys(Collection<Table> tables, Processable<MatchableTableDeterminant> candidateKeys, Collection<MatchableTableDeterminant> keyCluster, Map<Set<Integer>, Map<String, MatchableTableColumn>> columnMappingByTables) {
		Map<Integer, Table> tableMap = Q.map(tables, (t)->t.getTableId());
		// for each key
		for(MatchableTableDeterminant d : keyCluster) {

			MatchableTableDeterminant keyToCheck = d;
			// check if there is a larger key containing it
			for(MatchableTableDeterminant d2 : keyCluster) {
				if(d.getTableId()!=d2.getTableId()) {
					Collection<MatchableTableColumn> d2Translated = translateMatchableColumns(d2.getColumns(), tableMap.get(d2.getTableId()), tableMap.get(d.getTableId()), columnMappingByTables);
					if(d2Translated!=null) {
						if(d2Translated.containsAll(keyToCheck.getColumns())) {
							System.out.println(String.format("[OneKeyFullyMappedFilter] propagating key #%d: {%s} via #%d: {%s} to #%d: {%s}",
								keyToCheck.getTableId(),
								MatchableTableColumn.formatCollection(keyToCheck.getColumns()),
								d2.getTableId(),
								MatchableTableColumn.formatCollection(d2.getColumns()),
								keyToCheck.getTableId(),
								MatchableTableColumn.formatCollection(d2Translated)
							));
							// if it is contained, replace the current key
							keyToCheck = new MatchableTableDeterminant(d.getTableId(), new HashSet<>(d2Translated));
						} else {
							// System.out.println(String.format("[OneKeyFullyMappedFilter] key #%d: {%s} does not contain #%d: {%s}",
							// 	d2.getTableId(),
							// 	MatchableTableColumn.formatCollection(d2.getColumns()),
							// 	keyToCheck.getTableId(),
							// 	MatchableTableColumn.formatCollection(keyToCheck.getColumns())
							// ));
						}
					} else {
						System.out.println(String.format("[OneKeyFullyMappedFilter] cannot translate key #%d: {%s} to #%d!",
							d2.getTableId(),
							MatchableTableColumn.formatCollection(d2.getColumns()),
							keyToCheck.getTableId()
						));
					}
				}
			}

			// after checking all other keys in the cluster, we have found a key for the table which is not invalidated by the other keys in the cluster
			// (there might be other valid keys for the table, but as we stitch the tables afterwards, they will be added anyways)
			candidateKeys.add(keyToCheck);
		}
	}

	protected Pair<Set<TableColumn>,Set<TableColumn>> translateFD(Pair<Set<TableColumn>,Set<TableColumn>> fd, Table from, Table to, Map<Set<Integer>, Map<String, MatchableTableColumn>> columnMappingByTables) {
		Set<TableColumn> det = new HashSet<>();
		det.addAll(translateColumns(fd.getFirst(), from, to, columnMappingByTables));
	
		if(det.size()!=fd.getFirst().size()) {
			 return null;
		} else {
			Set<TableColumn> dep = new HashSet<>();
			dep.addAll(translateColumns(fd.getSecond(), from, to, columnMappingByTables));
		
			return new Pair<>(det, dep);
		}
	}

	protected Collection<MatchableTableColumn> translateMatchableColumns(Collection<MatchableTableColumn> columns, Table from, Table to, Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		Set<MatchableTableColumn> result = new HashSet<>();
		for(MatchableTableColumn c : columns) {
			Correspondence<MatchableTableColumn, Matchable> correspondence = correspondences
				.where(
					(cor)->
						cor.getFirstRecord().getIdentifier().equals(c.getIdentifier()) && cor.getSecondRecord().getTableId()==to.getTableId() 
						|| 
						cor.getSecondRecord().getIdentifier().equals(c.getIdentifier()) && cor.getFirstRecord().getTableId()==to.getTableId())
				.firstOrNull();
			if(correspondence!=null) {
				if(correspondence.getFirstRecord().getIdentifier().equals(c.getIdentifier())) {
					result.add(correspondence.getSecondRecord());
				} else if (correspondence.getSecondRecord().getIdentifier().equals(c.getIdentifier())) {
					result.add(correspondence.getFirstRecord());
				}
			}
		}
		return result;
	}

	protected Collection<MatchableTableColumn> translateMatchableColumns(Collection<MatchableTableColumn> columns, Table from, Table to, Map<Set<Integer>, Map<String, MatchableTableColumn>> columnMappingByTables) {
		Set<MatchableTableColumn> result = new HashSet<>();
		Map<String, MatchableTableColumn> mapping = columnMappingByTables.get(Q.toSet(from.getTableId(), to.getTableId()));
		for(MatchableTableColumn c : columns) {

			MatchableTableColumn otherColumn = mapping.get(c.getIdentifier());
			if(otherColumn!=null) {
				result.add(otherColumn);
			}
		}
		return result;
	}


	protected Collection<TableColumn> translateColumns(Collection<TableColumn> columns, Table from, Table to, Map<Set<Integer>, Map<String, MatchableTableColumn>> columnMappingByTables) {
		Set<TableColumn> result = new HashSet<>();
		Map<String, MatchableTableColumn> mapping = columnMappingByTables.get(Q.toSet(from.getTableId(), to.getTableId()));
		for(TableColumn c : columns) {
			MatchableTableColumn otherColumn = mapping.get(c.getIdentifier());
			if(otherColumn!=null) {
				result.addAll(Q.where(to.getColumns(), (col)->col.getIdentifier().equals(otherColumn.getIdentifier())));
			}
		}
		return result;
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

	private void printFunctionalDependencies(Table t) {
		System.out.println("*** Functional Dependencies");
		for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
			Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
			System.out.println(String.format("\t{%s} -> {%s}", 
					StringUtils.join(Q.project(det, new TableColumn.ColumnHeaderProjection()), ","),
					StringUtils.join(Q.project(dep, new TableColumn.ColumnHeaderProjection()), ",")
					));
		}
		System.out.println("*** Candidate Keys");
		for(Collection<TableColumn> key : t.getSchema().getCandidateKeys()) {
			System.out.println(String.format("\t{%s}", 
					StringUtils.join(Q.project(key, new TableColumn.ColumnHeaderProjection()), ",")
					));
		}
	}
}