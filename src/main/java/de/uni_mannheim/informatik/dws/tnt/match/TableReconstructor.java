/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.tnt.match;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencySet;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.dws.winter.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.Table.ConflictHandling;

/**
 * 
 * Merges tables based on the schema correspondences that are provided. First, the tables are transformed into a global schema (by merging all attributes). Then, the union of all transformed tables is created.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableReconstructor {

	private boolean addProvenance = true;
	private Map<Integer, Table> web = null;
	private boolean log = false;
	private File logDirectory = null;
	private boolean keepUnchangedTables = true;
	private boolean overlappingAttributesOnly = false;
	private boolean clusterByOverlappingAttributes = false;
	private boolean createNonOverlappingClusters = false;
	private boolean mergeToSet = true;
	private boolean verifyDependencies = false;
	private boolean propagateDependencies = true;
	private Map<Table, Collection<Table>> stitchedProvenance;

	/**
	 * @param keepUnchangedTables the keepUnchangedTables to set
	 */
	public void setKeepUnchangedTables(boolean keepUnchangedTables) {
		this.keepUnchangedTables = keepUnchangedTables;
	}

	/**
	 * @param overlappingAttributesOnly the overlappingAttributesOnly to set
	 */
	public void setOverlappingAttributesOnly(boolean overlappingAttributesOnly) {
		this.overlappingAttributesOnly = overlappingAttributesOnly;
	}

	/**
	 * @param clusterByOverlappingAttributes the clusterByOverlappingAttributes to set
	 */
	public void setClusterByOverlappingAttributes(boolean clusterByOverlappingAttributes) {
		this.clusterByOverlappingAttributes = clusterByOverlappingAttributes;
	}

	/**
	 * @param createNonOverlappingClusters the createNonOverlappingClusters to set
	 */
	public void setCreateNonOverlappingClusters(boolean createNonOverlappingClusters) {
		this.createNonOverlappingClusters = createNonOverlappingClusters;
	}

	/**
	 * @param verifyDependencies the verifyDependencies to set
	 */
	public void setVerifyDependencies(boolean verifyDependencies) {
		this.verifyDependencies = verifyDependencies;
	}

	/**
	 * @param propagateDependencies the propagateDependencies to set
	 */
	public void setPropagateDependencies(boolean propagateDependencies) {
		this.propagateDependencies = propagateDependencies;
	}

	/**
	 * @param mergeToSet the mergeToSet to set
	 */
	public void setMergeToSet(boolean mergeToSet) {
		this.mergeToSet = mergeToSet;
	}

	public void setLog(boolean log) {
		this.log = log;
	}
	
	public void setLogDirectory(File logDirectory) {
		this.logDirectory = logDirectory;
	}
	
	/**
	 * @return the stitchedProvenance
	 */
	public Map<Table, Collection<Table>> getStitchedProvenance() {
		return stitchedProvenance;
	}

	/**
	 * Creates a TableReconstructor that does *not* add provenance
	 */
	public TableReconstructor() {
		addProvenance = false;
	}
	
	/**
	 * Creates a TableReconstructor that *does* add provenance
	 * @param web
	 */
	public TableReconstructor(Map<Integer, Table> web) {
		this.addProvenance = true;
		this.web = web;
	}
	
	protected Map<Collection<Integer>, Integer> getTableClusters(
			Processable<MatchableTableColumn> attributes, 
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		ConnectedComponentClusterer<Integer> clusterer = new ConnectedComponentClusterer<>();
		
		for(MatchableTableColumn c : attributes.get()) {
			if(web.containsKey(c.getTableId())) {
				clusterer.addEdge(new Triple<Integer, Integer, Double>(c.getTableId(), c.getTableId(), 1.0));
			}
		}
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
			if(web.containsKey(cor.getFirstRecord().getTableId()) && web.containsKey(cor.getSecondRecord().getTableId())) {
				clusterer.addEdge(new Triple<Integer, Integer, Double>(cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId(), cor.getSimilarityScore()));
			}
		}
		
		Map<Collection<Integer>, Integer> clustering = clusterer.createResult();
		
		if(clusterByOverlappingAttributes) {
			// we only want to create clusters from tables where all attributes are mapped
			// so we split the table clusters into combinations of tables with overlapping attributes (tables can appear multiple times in the resulting clusters)

			Map<Collection<Integer>, Integer> newClustering = new HashMap<>();

			for(Collection<Integer> cluster : clustering.keySet()) {
				if(cluster.size()>1) {
					// get all attribute clusters
					Map<Collection<MatchableTableColumn>, MatchableTableColumn> attributeClusters = getAttributeClusters(attributes, schemaCorrespondences);

					List<Collection<MatchableTableColumn>> attributeClustersForTableCluster = new LinkedList<>(Q.where(attributeClusters.keySet(), (clu)->Q.any(clu,(c)->cluster.contains(c.getTableId()))));
					
					Map<Set<Collection<MatchableTableColumn>>, Set<Integer>> attributeCombinationsToTables = new HashMap<>();

					// add clusters for single attributes
					for(int i = 0; i < attributeClustersForTableCluster.size(); i++) {
							Set<Integer> tableSet = new HashSet<>(Q.project(attributeClustersForTableCluster.get(i), (c)->c.getTableId()));

							// only add if there is more than one table in the cluster
							if(tableSet.size()>1) {
								attributeCombinationsToTables.put(
									Q.toSet(attributeClustersForTableCluster.get(i)),
									tableSet
								);
							}
					}

					// for every combination of two attribute clusters, get the set of tables containing them
					for(int i = 0; i < attributeClustersForTableCluster.size(); i++) {
						for(int j = i+1; j < attributeClustersForTableCluster.size(); j++) {
							Set<Integer> tableSet = Q.intersection(
								Q.project(attributeClustersForTableCluster.get(i), (c)->c.getTableId()),
								Q.project(attributeClustersForTableCluster.get(j), (c)->c.getTableId())
							);

							// if(keepUnchangedTables || tableSet.size()>1) {
							if(tableSet.size()>0) {
								attributeCombinationsToTables.put(
									Q.toSet(attributeClustersForTableCluster.get(i), attributeClustersForTableCluster.get(j)),
									tableSet
								);
							}
							// }
						}
					}

					for(Map.Entry<Set<Collection<MatchableTableColumn>>, Set<Integer>> e : attributeCombinationsToTables.entrySet()) {
						Set<String> attributeNames = new HashSet<>();
						for(Collection<MatchableTableColumn> attributeCluster : e.getKey()) {
							attributeNames.add(Distribution.fromCollection(Q.project(attributeCluster, (c)->c.getHeader())).getMode());
						}
						// System.out.println(String.format("[pair] %s: %s",
						// 	StringUtils.join(attributeNames, ","),
						// 	StringUtils.join(e.getValue(), ",")
						// ));
					}

					// merge two attribute pairs if their sets of tables are equal
					Map<Set<Integer>, Collection<Entry<Set<Collection<MatchableTableColumn>>, Set<Integer>>>> groupedByTableSet = Q.group(attributeCombinationsToTables.entrySet(), (e)->e.getValue());

					System.out.println(String.format("[TableReconstructor] splitting cluster %s into sub clusters based on attributes:",
						StringUtils.join(cluster, ",")));

					// change the map into {attributes}->{tables}
					Map<Set<Collection<MatchableTableColumn>>, Set<Integer>> attributesToTables = new HashMap<>();
					for(Set<Integer> clu : groupedByTableSet.keySet()) {
						Set<Collection<MatchableTableColumn>> att = new HashSet<>();
						Set<String> attributeNames = new HashSet<>();

						for(Entry<Set<Collection<MatchableTableColumn>>, Set<Integer>> e : groupedByTableSet.get(clu)) {
							att.addAll(e.getKey());
							for(Collection<MatchableTableColumn> a : e.getKey()) {
								attributeNames.add(Distribution.fromCollection(Q.project(a, (c)->c.getHeader())).getMode());
							}
						}

						Set<Integer> tables = attributesToTables.get(att);
						if(tables==null) {
							tables = new HashSet<>(clu);
						} else {
							tables.addAll(clu);
						}

						System.out.println(String.format("[TableReconstructor]\tcluster %s: {%s}",
							StringUtils.join(tables, ","),
							StringUtils.join(Q.sort(attributeNames), ",")
							));

						attributesToTables.put(att, tables);
					}

					if(createNonOverlappingClusters && attributesToTables.size()>1) {
						// remove sub-clusters which are contained in another sub-cluster with more attributes

						Map<Set<Collection<MatchableTableColumn>>, Set<Integer>> nonOverlappingClusters = new HashMap<>();
						Set<Integer> assignedTables = new HashSet<>();


						// start with the cluster with the largest number of attributes
						for(Set<Collection<MatchableTableColumn>> att : Q.sort(attributesToTables.keySet(), (a1,a2)->-Integer.compare(a1.size(),a2.size()))) {
							Set<Integer> tbls = new HashSet<>(attributesToTables.get(att));

							Set<String> attributeNames = new HashSet<>();
							for(Collection<MatchableTableColumn> a : att) {
								attributeNames.add(Distribution.fromCollection(Q.project(a, (c)->c.getHeader())).getMode());
							}

							// if the cluster cotains only a single table, remove it
							if(tbls.size()>1) {
								// if it has more than one table, add it to the result
								Set<Integer> unassignedTables = new HashSet<>(tbls);
								unassignedTables.removeAll(assignedTables);

								// if any unassigned tables are left in the cluster, add them to the result
								if(unassignedTables.size()>0) {
									nonOverlappingClusters.put(att, unassignedTables);

									System.out.println(String.format("[TableReconstructor]\tcreating cluster with tables %s",
										StringUtils.join(Q.sort(unassignedTables), ",")));

									// and remove its tables from all other clusters
									assignedTables.addAll(unassignedTables);
								} else {
									System.out.println(String.format("[TableReconstructor]\tremoving cluster with no unassigned tables %s",
										StringUtils.join(Q.sort(tbls), ",")));
								}
							} else {
								System.out.println(String.format("[TableReconstructor]\tremoving singleton cluster %s: {%s}",
									StringUtils.join(tbls, ","),
									StringUtils.join(Q.sort(attributeNames), ",")
									));
							}

						}

						attributesToTables = nonOverlappingClusters;
					}

					// create one cluster for every set of tables
					for(Set<Collection<MatchableTableColumn>> att : attributesToTables.keySet()) {
						Set<String> attributeNames = new HashSet<>();
						for(Collection<MatchableTableColumn> a : att) {
							attributeNames.add(Distribution.fromCollection(Q.project(a, (c)->c.getHeader())).getMode());
						}
						Set<Integer> tableSet = attributesToTables.get(att);
						newClustering.put(tableSet, null);
						System.out.println(String.format("[TableReconstructor]\t%s:\t%s",
							StringUtils.join(tableSet, ","),
							StringUtils.join(Q.sort(attributeNames), ",")
						));
					}

				} else {
					// make sure we don't loose any tables
					newClustering.put(cluster, null);
				}
			}
			clustering = newClustering;
		}

		return clustering;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> getCorrespondencesForTableCluster(
		Collection<Integer> cluster,
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> filtered = new ProcessableCollection<>();
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
			if(cluster.contains(cor.getFirstRecord().getTableId()) && cluster.contains(cor.getSecondRecord().getTableId())) {
				filtered.add(cor);
			}
		}
		
		return filtered;
	}
	
	protected Processable<MatchableTableColumn> getAttributesForTableCluster(
			Collection<Integer> cluster,
			Processable<MatchableTableColumn> attributes) {
		
		Processable<MatchableTableColumn> filtered = new ProcessableCollection<>();
		
		for(MatchableTableColumn col : attributes.get()) {
			if(cluster.contains(col.getTableId())) {
				filtered.add(col);
			}
		}
		
		return filtered;
	}
	
	protected Processable<MatchableTableRow> getRecordsForTableCluster(
			Collection<Integer> cluster,
			Processable<MatchableTableRow> records) {
		
		Processable<MatchableTableRow> filtered = new ProcessableCollection<>();
		
		for(MatchableTableRow row : records.get()) {
			if(cluster.contains(row.getTableId())) {
				filtered.add(row);
			}
		}
		
		return filtered;
	}
	
	protected Processable<MatchableTableDeterminant> getMappedKeysForTableCluster(
			Collection<Integer> cluster,
			Processable<MatchableTableDeterminant> keys,
			Processable<MatchableTableColumn> attributes) {
		
		Processable<MatchableTableDeterminant> result = new ProcessableCollection<>();
		
		Set<MatchableTableColumn> attributesForCluster = new HashSet<>(getAttributesForTableCluster(cluster, attributes).get());
		
		for(MatchableTableDeterminant k : keys.get()) {
			
			if(attributesForCluster.containsAll(k.getColumns())) {
				result.add(k);
			}
			
		}
		
		return result;
	}
	
	protected Processable<MatchableTableDeterminant> getMappedFunctionalDependenciesForTableCluster(
		Collection<Integer> cluster,
		Processable<MatchableTableDeterminant> functionalDependencies,
		Processable<MatchableTableColumn> attributes) {
	
		Processable<MatchableTableDeterminant> result = new ProcessableCollection<>();
		
		Set<MatchableTableColumn> attributesForCluster = new HashSet<>(getAttributesForTableCluster(cluster, attributes).get());
		
		if(functionalDependencies!=null) {
			for(MatchableTableDeterminant k : functionalDependencies.get()) {
				
				if(
					attributesForCluster.containsAll(k.getColumns())
					&& attributesForCluster.containsAll(k.getDependant())
					) {
					result.add(k);
				}
				
			}
		}
		
		return result;
	}

	protected Map<Collection<MatchableTableColumn>, MatchableTableColumn> getAttributeClusters(
			Processable<MatchableTableColumn> attributes, 
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
		
		for(MatchableTableColumn c : attributes.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(c, c, 1.0));
		}
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		
		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clustering = clusterer.createResult();

		return clustering;
	}
	
	protected Pair<Table, Map<MatchableTableColumn, TableColumn>> createReconstructedTable(
			int tableId, 
			Map<Collection<MatchableTableColumn>, MatchableTableColumn> attributeClusters,
			Processable<MatchableTableDeterminant> mappedKeys,
			Processable<MatchableTableDeterminant> mappedFDs
			) {
		Table table = new Table();
		table.setPath(Integer.toString(tableId));
		table.setTableId(tableId);
		
		/*********************************
		 *  Column Clusters
		 *********************************/
		// get the attribute clusters
		Set<Collection<MatchableTableColumn>> schemaClusters = attributeClusters.keySet();
		
		int columnIndex = 0;
		
		Map<MatchableTableColumn, TableColumn> columnMapping = new HashMap<>();
		Map<TableColumn, Collection<MatchableTableColumn>> columnToCluster = new HashMap<>();
		
		List<Collection<MatchableTableColumn>> sorted = Q.sort(schemaClusters, new Comparator<Collection<MatchableTableColumn>>() {

			@Override
			public int compare(Collection<MatchableTableColumn> o1, Collection<MatchableTableColumn> o2) {
				int idx1 = Q.min(o1, new MatchableTableColumn.ColumnIndexProjection()).getColumnIndex();
				int idx2 = Q.min(o2, new MatchableTableColumn.ColumnIndexProjection()).getColumnIndex();
				
				return Integer.compare(idx1, idx2);
			}
		});
		
		// map all columns in the cluster to a new column in the merged table
		for(Collection<MatchableTableColumn> schemaCluster : sorted) {
			
			TableColumn c = new TableColumn(columnIndex++, table);
			
			// use the most frequent header as the header for the merged column
			Distribution<String> headers = Distribution.fromCollection(schemaCluster, new MatchableTableColumn.ColumnHeaderProjection());
			String newHeader = getMostFrequentHeaderForCluster(schemaCluster);
			c.setHeader(newHeader);
			c.setSynonyms(headers.getElements());
			
			// use the most frequent data type
			Distribution<DataType> types = Distribution.fromCollection(schemaCluster, new Func<DataType, MatchableTableColumn>() {

				@Override
				public DataType invoke(MatchableTableColumn in) {
					return in.getType();
				}});
			c.setDataType(types.getMode());
			
			if(addProvenance) {
				// add provenance for column
				for(MatchableTableColumn c0 : schemaCluster) {
					TableColumn col = web.get(c0.getTableId()).getSchema().get(c0.getColumnIndex());
					c.addProvenanceForColumn(col);
					c.getProvenance().add(c0.getIdentifier());
				}
			}
			
			table.addColumn(c);
			
			for(MatchableTableColumn col : schemaCluster) {
				columnMapping.put(col, c);
			}
			columnToCluster.put(c, schemaCluster);
		}

		// checking attribute cluster size is problematic if correspondences are noisy (not 1:1), in which case a cluster can be larger than the number of tables
		// int numTables = Q.<Integer>max(Q.project(attributeClusters.keySet(), (clu)->clu.size()));
		Set<Integer> tableIds = new HashSet<>();
		for(Collection<MatchableTableColumn> clu : attributeClusters.keySet()) {
			tableIds.addAll(Q.project(clu, (c)->c.getTableId()));
		}
		int numTables = tableIds.size();

		// add all provided functional dependencies
		for(MatchableTableDeterminant fd : mappedFDs.get()) {
			Set<TableColumn> det = new HashSet<>();
			Set<TableColumn> dep = new HashSet<>();

			// translate determinant & dependant to the new table's columns
			for(MatchableTableColumn col : fd.getColumns()) {
				TableColumn c = columnMapping.get(col);
				if(c==null) {
					break;
				} else {
					det.add(c);
				}
			}
			for(MatchableTableColumn col : fd.getDependant()) {
				TableColumn c = columnMapping.get(col);
				if(c==null) {
					break;
				} else {
					dep.add(c);
				}
			}

			// check if all columns of the FD are mapped (i.e., exist in the stitched table)
			if(det.size()==fd.getColumns().size() && dep.size()==fd.getDependant().size()) {
				Set<TableColumn> existingDep = table.getSchema().getFunctionalDependencies().get(det);
				if(existingDep!=null) {
					// if there was already a FD with the same determinant, merge the dependants
					dep = Q.union(existingDep, dep);
				}
				// add the FD to the table
				table.getSchema().getFunctionalDependencies().put(det, dep);
			}
		}

		if(verifyDependencies) {
			//  make sure the FDs are consistent
			FunctionalDependencySet fdSet = new FunctionalDependencySet(new HashMap<>());
			fdSet.setVerbose(true);
			fdSet.setPropagationCondition((fd)->propagateDependencies && fd.getFirst().size()>0);
			// - propagate to all columns in clusters which are smaller than the number of tables that is being stitched
			// --- we stitch tables with matching keys, so attribute clusters as large as the number of tables are potential keys for all tables
			// propagate invalidation to all columns which are not determined by {}
			fdSet.setPropagateInvalidationToDependants((fd)->Q.without(table.getColumns(), table.getSchema().getFunctionalDependencies().get(new HashSet<>())));
			// only specialising with attributes from all tables forces key to be extended to context attributes
			fdSet.setSpecialiseWith((fd)->Q.where(Q.without(table.getColumns(), table.getSchema().getFunctionalDependencies().get(new HashSet<>())), new ContextColumns.IsNoContextColumnPredicate()));

			for(Pair<Set<TableColumn>, Set<TableColumn>> fd : Q.sort(
				FunctionalDependencyUtils.split(Pair.fromMap(table.getSchema().getFunctionalDependencies())),
				(fd1,fd2)->Integer.compare(fd1.getFirst().size(), fd2.getFirst().size())) // sort FDs by determinant size, so general FDs are added first and can be invalidated by larger FDs
			) {
				fdSet.addVerifiedMinimalFunctionalDependency(fd);
			}
			//DEBUG
			table.getSchema().setFunctionalDependencies(fdSet.getFunctionalDependencies());
			table.getSchema().setCandidateKeys(FunctionalDependencyUtils.listCandidateKeys(table));
			System.out.println(table.formatFunctionalDependencies());

			System.out.println(String.format("[TableReconstructor] Calculating canonical cover for %d FDs", fdSet.getFunctionalDependencies().size()));
			table.getSchema().setFunctionalDependencies(FunctionalDependencyUtils.canonicalCover(fdSet.getFunctionalDependencies(), false));

			// re-calculate candidate keys
			System.out.println(String.format("[TableReconstructor] Listing candidate keys for %d FDs", table.getSchema().getFunctionalDependencies().size()));
			table.getSchema().setCandidateKeys(FunctionalDependencyUtils.listCandidateKeys(table));

			System.out.println(table.formatFunctionalDependencies());

			// only keep candidate keys with attributes that occurred in all tables
			Iterator<Set<TableColumn>> keyIt = table.getSchema().getCandidateKeys().iterator();
			while(keyIt.hasNext()) {
				Set<TableColumn> key = keyIt.next();
				boolean isAvailableInAllTables = true;
				for(TableColumn c : key) {
					if(columnToCluster.get(c).size()<numTables) {
						isAvailableInAllTables = false;
						break;
					}
				}
				if(!isAvailableInAllTables) {
					keyIt.remove();
				}
			}
		} else {
			// add all candidate keys that are completely mapped
			// - if we add a key which does not exist in all tables, the deduplication (merge-to-set) will merge all rows in tables where the attriutes are not included
			System.out.println(String.format("[TableReconstructor] Checking %d candidate keys", mappedKeys.size()));
			Set<Set<TableColumn>> newKeys = new HashSet<>();
			for(MatchableTableDeterminant k : mappedKeys.get()) {

				boolean isCompletelyMapped = true;
				// check that the key only consists of columns from all stitched tables
				for(MatchableTableColumn keyAttribute : k.getColumns()) {
					if(!Q.any(attributeClusters.keySet(), (clu)->clu.contains(keyAttribute) && clu.size()==numTables)) {
						isCompletelyMapped = false;
						break;
					}
				}

				if(isCompletelyMapped) {
					Set<TableColumn> keyInNewTable = new HashSet<>();
					
					for(MatchableTableColumn col : k.getColumns()) {
						TableColumn c = columnMapping.get(col);
						
						if(c==null) {
							break;
						} else {
							keyInNewTable.add(c);
						}
					}
					
					if(keyInNewTable.size()==k.getColumns().size()) {
						newKeys.add(keyInNewTable);
					}

					System.out.println(String.format("[TableReconstructor]\tAdding mapped candidate key {%s} ", StringUtils.join(Q.project(keyInNewTable, (c)->c.getHeader()), ",")));
				} else {
					System.out.println(String.format("[TableReconstructor]\tIncomplete mapping for candidate key {%s} ", StringUtils.join(Q.project(k.getColumns(), (c)->c.getHeader()), ",")));
				}
			}
			for(Set<TableColumn> key : newKeys) {
				table.getSchema().getCandidateKeys().add(key);
			}
		}

		
		if(table.getSchema().getCandidateKeys().size()==0) {
			table.getSchema().getCandidateKeys().add(new HashSet<>(table.getColumns()));
		}
		
		
		return new Pair<Table, Map<MatchableTableColumn,TableColumn>>(table, columnMapping);
	}
	
	protected String getMostFrequentHeaderForCluster(Collection<MatchableTableColumn> schemaCluster) {
		Map<String, Integer> numOriginalColumnsPerHeader = new HashMap<>();
		for(MatchableTableColumn c : schemaCluster) {
			Table t = web.get(c.getTableId());
			
			if(t==null) {
				System.err.println(String.format("[Error] Could not find table %d", c.getTableId()));
			}
			
			TableColumn col = null;
			
			try {
				col = t.getSchema().get(c.getColumnIndex());
			} catch(Exception e) {
				System.err.println(String.format("[Error] Could not find column %s", c));
			}
			
			int originalColumns = col.getProvenance().size();
			MapUtils.add(numOriginalColumnsPerHeader, c.getHeader(), originalColumns);
		}
		return MapUtils.max(numOriginalColumnsPerHeader);
	}
	
	protected Table populateTable(
			Table t, 
			Map<MatchableTableColumn, TableColumn> attributeMapping, 
			Processable<MatchableTableRow> records) {
		
		int rowIdx = 0;
		
		System.out.println(String.format("[TableReconstructor] Populating Table #%d with %d records", t.getTableId(), records.size()));
		
		for(MatchableTableRow row : records.get()) {
		
			TableRow newRow = new TableRow(rowIdx++, t);
			
			Object[] values = new Object[t.getColumns().size()];
			
			for(MatchableTableColumn c : row.getSchema()) {
				TableColumn t2Col = attributeMapping.get(c);
				
				if(t2Col!=null) {
					Object value = row.get(c.getColumnIndex());
					values[t2Col.getColumnIndex()] = value;
				}
			}
			
			newRow.set(values);
			
			if(addProvenance) {
				// add provenance
				TableRow originalRow = web.get(row.getTableId()).get(row.getRowNumber());
				newRow.addProvenanceForRow(originalRow);
				newRow.getProvenance().add(row.getIdentifier());
			}
			
			t.addRow(newRow);
		}
		
		return t;
	}
	
	public Collection<Table> reconstruct(
			int firstTableId,
			Processable<MatchableTableRow> records,
			Processable<MatchableTableColumn> attributes, 
			Processable<MatchableTableDeterminant> candidateKeys,
			Processable<MatchableTableDeterminant> functionalDependencies,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		Map<Collection<Integer>, Integer> tableClusters = getTableClusters(attributes, schemaCorrespondences);
		System.out.println(String.format("[TableReconstructor] %d table clusters.", tableClusters.size()));
		
		// log the table clusters
		int clusterNo = 1;
		for(Collection<Integer> tableCluster : tableClusters.keySet()) {
			System.out.println(String.format("[TableReconstructor] Cluster #%d", clusterNo++));
			for(Integer tableId : tableCluster) {
				Table table = web.get(tableId);
				System.out.println(String.format("[TableReconstructor]\ttable #%d\t{%s} / %d rows", tableId, StringUtils.join(Q.sort(Q.project(table.getSchema().getRecords(), new TableColumn.ColumnHeaderProjection())), ","), table.getRows().size()));
			}
		}
		
		Collection<Table> result = new ArrayList<>(tableClusters.size());
		
		stitchedProvenance = new HashMap<>();

		int tableId = firstTableId;
		for(Collection<Integer> tableCluster : tableClusters.keySet()) {

			Table t = null;
			if(tableCluster.size()==1) {
				if(keepUnchangedTables) {
					t = web.get(Q.firstOrDefault(tableCluster));
				}
			} else {
				System.out.println(String.format("[TableReconstructor] Consolidating schema of tables %s", StringUtils.join(tableCluster, ",")));
				
				Processable<Correspondence<MatchableTableColumn, Matchable>> clusterCorrespondences = getCorrespondencesForTableCluster(tableCluster, schemaCorrespondences);
				Processable<MatchableTableColumn> clusterAttributes = getAttributesForTableCluster(tableCluster, attributes);
				
				if(log) {
					SimilarityMatrix<MatchableTableColumn> m = SimilarityMatrix.fromCorrespondences(clusterCorrespondences.get(), new SparseSimilarityMatrixFactory());
					System.out.println(m.getOutput());
					
					try {
						Correspondence.toGraph(clusterCorrespondences.get()).writePajekFormat(new File(logDirectory, StringUtils.join(tableCluster, "_") + ".net"));
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				Map<Collection<MatchableTableColumn>, MatchableTableColumn> attributeClusters = getAttributeClusters(clusterAttributes, clusterCorrespondences);

				if(log) {
					for(Collection<MatchableTableColumn> clu : attributeClusters.keySet()) {
						if(clu.size()==1) {
							
							MatchableTableColumn col = Q.firstOrDefault(clu);
							TableColumn c = web.get(col.getTableId()).getSchema().get(col.getColumnIndex());
							System.out.println(String.format("[TableReconstructor] Unmatched Column: [%s] %s / provenance: %s", col.getIdentifier(), col.getHeader(),
									Q.where(c.getProvenance(), (s)->s.length()<(c.getHeader().length()+10))));
							
							for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.where((cor)->cor.getFirstRecord().getTableId()==col.getTableId() || cor.getSecondRecord().getTableId()==col.getTableId()).get()) {
								System.out.println(String.format("[TableReconstructor]\t\t%s <-> %s", cor.getFirstRecord(), cor.getSecondRecord()));
							}
							
						}
					}
				}

				System.out.println("[TableReconstructor] Column Clusters:");
				Iterator<Collection<MatchableTableColumn>> cluIt = attributeClusters.keySet().iterator();
				while(cluIt.hasNext()) {
					Collection<MatchableTableColumn> clu = cluIt.next();

					if(overlappingAttributesOnly && tableCluster.size()>clu.size()) {
						cluIt.remove();
					} else {
						System.out.println(String.format("[TableReconstructor]\t%s", 
							StringUtils.join(
									Q.project(clu, (c)->c.toString())
									, ",")));
					}
				}
				
				Processable<MatchableTableDeterminant> mappedKeys = getMappedKeysForTableCluster(tableCluster, candidateKeys, clusterAttributes);
				Processable<MatchableTableDeterminant> mappedFDs = getMappedFunctionalDependenciesForTableCluster(tableCluster, functionalDependencies, clusterAttributes);
				
				Pair<Table, Map<MatchableTableColumn, TableColumn>> reconstruction = createReconstructedTable(tableId++, attributeClusters, mappedKeys, mappedFDs);
	
				if(log) {
					for(MatchableTableColumn col : reconstruction.getSecond().keySet()) {
						TableColumn to = reconstruction.getSecond().get(col);
						System.out.println(String.format("[TableReconstructor]\t%s -> %s", col, to));
					}
				}
				
				Processable<MatchableTableRow> clusterRecords = getRecordsForTableCluster(tableCluster, records);
				t = populateTable(reconstruction.getFirst(), reconstruction.getSecond(), clusterRecords);


				// make sure the used key contains the FK
				Set<TableColumn> selectedKey = Q.max(Q.where(t.getSchema().getCandidateKeys(), (k)->Q.any(k,(c)->"FK".equals(c.getHeader()))), (k)->k.size());
				if(selectedKey==null) {
					selectedKey = new HashSet<>(t.getColumns());
				}
				if(mergeToSet) {
					for(Set<TableColumn> key : t.getSchema().getCandidateKeys()) {
						System.out.println(String.format("[TableReconstructor]\tCandidate key {%s}", 
							StringUtils.join(Q.project(key, (c)->c.getHeader()), ",")
						));	
					}
					Set<TableColumn> dedupKey = selectedKey;
					System.out.println(String.format("[TableReconstructor] Deduplicating with merge-to-set using candidate key {%s}", 
						StringUtils.join(Q.project(dedupKey, (c)->c.getHeader()), ",")
					));
					t.deduplicate(dedupKey, ConflictHandling.CreateSet);
					System.out.println(String.format("[TableReconstructor] %d rows after deduplication", t.getSize())); 
				} else {
					System.out.println(String.format("[TableReconstructor] Deduplicating with NULL replacement using candidate key {%s}", 
						StringUtils.join(Q.project(selectedKey, (c)->c.getHeader()), ",")
					));
				}
				t.deduplicate(selectedKey, ConflictHandling.ReplaceNULLs);
				System.out.println(String.format("[TableReconstructor] %d rows after deduplication", t.getSize())); 
			}

			if(t!=null) {
				result.add(t);

				Set<Table> provenance = new HashSet<>();
				for(Integer id : tableCluster) {
					provenance.add(web.get(id));
				}
				stitchedProvenance.put(t, provenance);
			}
		}
		
		return result;
	}
	
	public Table removeSparseColumns(Table t, double minDensity) throws Exception {
		
		Set<TableColumn> sparseColumns = new HashSet<>();
		
		for(TableColumn c : t.getColumns()) {
			
			int values = 0;
			
			for(TableRow r : t.getRows()) {
				
				if(r.get(c.getColumnIndex())!=null) {
					values++;
				}
				
			}
			
			double density = values / (double)t.getRows().size();
			
			if(density<minDensity) {
				sparseColumns.add(c);
			}
			
		}
		
		Collection<TableColumn> newColumns = Q.without(t.getColumns(), sparseColumns);
		
		return t.project(newColumns);
	}
}
