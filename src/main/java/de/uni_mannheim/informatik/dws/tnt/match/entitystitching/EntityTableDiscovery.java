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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.EntityTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableLodColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.KeyCorrespondenceFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.determinants.DeterminantMatcher;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * 
 * Creates groups of tables which contain the same type of entities. Each group contains clusters of columns that represent the same attribute.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EntityTableDiscovery {

	// load entity definitions
	private KnowledgeBase kb;
	private MatchingGoldStandard entityGoldStandard;
	
	public void loadEntityDefinitions(File entityDefinitionLocation) throws IOException {
		kb = KnowledgeBase.loadKnowledgeBase(new File(entityDefinitionLocation, "tables/"), null, null, true, true);
		kb.loadCandidateKeys(new File(entityDefinitionLocation, "keys/"));	
	}
	
	public void loadEntityGoldStandard(File entityGoldStandardLocation) throws IOException {
		entityGoldStandard = new MatchingGoldStandard();
		entityGoldStandard.loadFromCSVFile(entityGoldStandardLocation);
	}
	
	public Collection<EntityTable> discoverEntityColumns(Processable<MatchableTableRow> records,
			Processable<MatchableTableColumn> attributes, 
			Processable<MatchableTableDeterminant> candidateKeys,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		// get clusters of tables that are connected, i.e., different entity types
		Map<Collection<Integer>, Integer> tableClusters = getTableClusters(attributes, schemaCorrespondences);
		
		Collection<EntityTable> entityAttributeGroups = new LinkedList<>();
		
		// for each table cluster
		for(Collection<Integer> tableCluster : tableClusters.keySet()) {
		
			// get the correspondences for the current cluster
			Processable<Correspondence<MatchableTableColumn, Matchable>> clusterCorrespondences = getCorrespondencesForTableCluster(tableCluster, schemaCorrespondences);
			
			// get the attributes for the current cluster
			Processable<MatchableTableColumn> clusterAttributes = getAttributesForTableCluster(tableCluster, attributes);
			
			// get clusters of columns that are the same attribute
			Map<Collection<MatchableTableColumn>, MatchableTableColumn> attributeClusters = getAttributeClusters(clusterAttributes, clusterCorrespondences);
			
			Map<String, Collection<MatchableTableColumn>> entityAttributes = new HashMap<>();
			for(Collection<MatchableTableColumn> att : attributeClusters.keySet()) {
				String name = Q.firstOrDefault(att).getHeader();
				entityAttributes.put(name, att);
			}
			
			EntityTable entity = new EntityTable();
			entity.setAttributes(entityAttributes);
			entityAttributeGroups.add(entity);
		}
		
		return entityAttributeGroups;
	}
	

	
	public Collection<EntityTable> createEntityColumnsFromCorrespondences(
			Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences, 
			Map<Integer, String> classesById,
			DataSet<MatchableTableColumn, MatchableTableColumn> attributes,
			Map<String, Collection<Collection<String>>> candidateKeys) {
		
		Set<MatchableTableColumn> unmappedAttributes = new HashSet<>(attributes.get());
		Map<String, Integer> nameCounts = new HashMap<>();
		EntityTable nonEntity = new EntityTable();
		nonEntity.setEntityName("ne");
		System.out.println(String.format("Creating connected components for %d schema correspondences", correspondences.size()));
		Set<Collection<MatchableTableColumn>> components = Correspondence.getConnectedComponents(correspondences.get());
		Set<String> nonEntityTableProvenance = new HashSet<>();
		for(Collection<MatchableTableColumn> component : components) {
			Distribution<String> headerDist = new Distribution<>();
			Set<MatchableTableColumn> entityColumns = new HashSet<>();
			Set<String> attributeProvenance = new HashSet<>();
			for(MatchableTableColumn c : component) {
				headerDist.add(c.getHeader());
				if(c instanceof MatchableLodColumn) {
					entityColumns.add(c);
				} else {
					attributeProvenance.add(c.getIdentifier());
					nonEntityTableProvenance.add(c.getIdentifier().split("~")[0]);
				}
				unmappedAttributes.remove(c);
			}

			// assign a name to the attribute
			String name = headerDist.getMode();
			int nameCount = MapUtils.get(nameCounts, name, 0);
			MapUtils.increment(nameCounts, name);
			if(nameCount>0) {
				name = String.format("%s (%d)", name, nameCount+1);
			}

			System.out.println(String.format("Cluster '%s' (%s) %d columns: %s",
				name,
				StringUtils.join(Q.project(entityColumns, (c)->c.toString()), ","),
				component.size(),
				StringUtils.join(Q.project(component, (c)->c.toString()), ",")
			));

			// if the cluster does not contain any KB attributes, add it to the non-entity attributes
			if(entityColumns.size()==0) {
				nonEntity.getAttributes().put(name, component);
				nonEntity.getAttributeProvenance().put(name, attributeProvenance);
			}
		}
		// add all unmapped attributes to non-entity
		for(MatchableTableColumn c : unmappedAttributes) {
			String name = c.getHeader();
			int nameCount = MapUtils.get(nameCounts, name, 0);
			MapUtils.increment(nameCounts, name);
			if(nameCount>0) {
				// name = String.format("%s (%d)", name, nameCount+1);
				name = c.toString();
			}
			nonEntity.getAttributes().put(name, Q.toSet(c));
			nonEntity.getAttributeProvenance().put(name, Q.toSet(c.getIdentifier()));
			nonEntityTableProvenance.add(c.getIdentifier().split("~")[0]);

			System.out.println(String.format("Cluster '%s': %s",
				name,
				c.toString()
			));
		}
		nonEntity.setProvenance(nonEntityTableProvenance);

		// group correspondences by KB class and create entity attribute groups
		Processable<EntityTable> entityTables = correspondences
			.where(
					// only use correspondences to an entity definition
					(c)->c.getSecondRecord() instanceof MatchableLodColumn)
			.group(
					// group them by the entity definition's table id (i.e., one entity type per group
					(Correspondence<MatchableTableColumn, Matchable> record,DataIterator<Pair<Integer, Correspondence<MatchableTableColumn, Matchable>>> resultCollector)
					-> resultCollector.next(new Pair<Integer, Correspondence<MatchableTableColumn,Matchable>>(record.getSecondRecord().getTableId(), record))
			)
			.map(
					// map the correspondences to an entity group
					(Group<Integer, Correspondence<MatchableTableColumn, Matchable>> record, DataIterator<EntityTable> resultCollector) 
					-> {
					
						// group correspondences by KB property ID
						Processable<Pair<MatchableTableColumn, Collection<MatchableTableColumn>>> attributeGroups = record.getRecords()
								.group(
									// group by property ID
									(Correspondence<MatchableTableColumn, Matchable> r,DataIterator<Pair<Integer, Correspondence<MatchableTableColumn, Matchable>>> rc)
									-> rc.next(new Pair<Integer, Correspondence<MatchableTableColumn,Matchable>>(r.getSecondRecord().getColumnIndex(), r)))
								.map(
									(Group<Integer, Correspondence<MatchableTableColumn, Matchable>> r,DataIterator<Pair<MatchableTableColumn, Collection<MatchableTableColumn>>> rc) 
									-> {
									
										MatchableTableColumn property = null;
										Collection<MatchableTableColumn> columns = new ArrayList<>(r.getRecords().size());
										
										// create a list of all columns that are mapped to this property
										for(Correspondence<MatchableTableColumn, Matchable> cor : r.getRecords().get()) {
											if(property==null) {
												property = cor.getSecondRecord();
											}
											columns.add(cor.getFirstRecord());
										}
										
										rc.next(new Pair<MatchableTableColumn, Collection<MatchableTableColumn>>(property, columns));
										
									}
							);
					
						String cls =  classesById.get(record.getKey());
						Map<String, Collection<MatchableTableColumn>> atts = new HashMap<>();
						Map<String, Set<String>> attributeProvenance = new HashMap<>();
						Set<String> tableProvenance = new HashSet<>();
						Map<String, String> attMapping = new HashMap<>();
						Map<Collection<String>, Collection<Collection<MatchableTableColumn>>> keys = new HashMap<>();
						Collection<Collection<String>> entityKeys = candidateKeys.get(cls);
						
						if(entityKeys==null) {
							System.out.println(String.format("No keys defined for class %s!", cls));
						}
						
						for(Pair<MatchableTableColumn, Collection<MatchableTableColumn>> p : attributeGroups.get()) {
							// create attribute definition
							atts.put(p.getFirst().getHeader(), p.getSecond());
							Set<String> attProv = new HashSet<>();
							for(MatchableTableColumn c : p.getSecond()) {
								attProv.add(c.getIdentifier());
								tableProvenance.add(c.getIdentifier().split("~")[0]);
							}
							attributeProvenance.put(p.getFirst().getHeader(), attProv);
							attMapping.put(p.getFirst().getHeader(), p.getFirst().getIdentifier());
							
							// create key definition
							for(Collection<String> key : entityKeys) {
								if(key.contains(p.getFirst().getHeader())) {
									
									Collection<Collection<MatchableTableColumn>> keyColumns = keys.get(key);
									if(keyColumns==null) {
										keyColumns = new LinkedList<>();
										keys.put(key, keyColumns);
									}
									
									keyColumns.add(p.getSecond());
								}
							}
						}
	
						EntityTable ent = new EntityTable();
						ent.setEntityName(cls);
						ent.setAttributes(atts);
						ent.setAttributeMapping(attMapping);
						ent.setCanddiateKeys(keys.values());
						ent.setAttributeProvenance(attributeProvenance);
						ent.setProvenance(tableProvenance);
						
						resultCollector.next(ent);
			});
		
		for(EntityTable et : entityTables.get()) {
			for(String att : et.getAttributes().keySet()) {
				System.out.println(String.format("Mapped Cluster [%s] '%s'",
					et.getEntityName(),
					StringUtils.join(Q.project(et.getAttributes().get(att), (c)->c.toString()), ",")
				));
			}
		}

		Collection<EntityTable> result = entityTables.get();
		result.add(nonEntity);
		return result;
	}
	
	protected void printCorrespondencesWithValues(Processable<Correspondence<MatchableTableColumn, MatchableValue>> correspondences, Map<Integer, Table> tables) {
		for(Correspondence<MatchableTableColumn, MatchableValue> cor : Q.sort(correspondences.get(), (c1,c2)->Integer.compare(c1.getFirstRecord().getTableId(), c2.getFirstRecord().getTableId()))) {
			Distribution<String> valueDist = new Distribution<>();
			for(Correspondence<MatchableValue, Matchable> cause : cor.getCausalCorrespondences().get()) {
				valueDist.add(cause.getFirstRecord().getValue().toString());
			}
			System.out.println(String.format("%.6f\t%s <-> {%d}%s\t(%d/%d)", 
				cor.getSimilarityScore(), 
				cor.getFirstRecord(), 
				cor.getSecondRecord().getDataSourceIdentifier(),
				cor.getSecondRecord(), 
				cor.getCausalCorrespondences().size(), 
				tables.get(cor.getFirstRecord().getTableId()).getSize(),
				valueDist.format()));
		}
	}
	
	protected void printCorrespondences(Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences, Map<Integer, Table> tables) {
		for(Correspondence<MatchableTableColumn, Matchable> cor : Q.sort(correspondences.get(), (c1,c2)->Integer.compare(c1.getFirstRecord().getTableId(), c2.getFirstRecord().getTableId()))) {
			if(cor.getSecondRecord() instanceof MatchableLodColumn) {
				System.out.println(String.format("%.6f\t%s <-> {%d}%s\t(%d/%d)", 
					cor.getSimilarityScore(), 
					cor.getFirstRecord(), 
					cor.getSecondRecord().getDataSourceIdentifier(),
					cor.getSecondRecord(), 
					cor.getCausalCorrespondences()==null ? 0 : cor.getCausalCorrespondences().size(), 
					tables.get(cor.getFirstRecord().getTableId()).getSize()));
			}
		}
	}
	
// 	/**
// 	 * Creates entity table definitions from the mapped candidate keys among the union tables
// 	 * @return
// 	 */
// 	public Collection<EntityTable> createFromKeyMapping(DataSet<MatchableTableRow, MatchableTableColumn> records,
// 			DataSet<MatchableTableColumn, MatchableTableColumn> attributes, 
// 			DataSet<MatchableTableDeterminant, MatchableTableColumn> candidateKeys,
// 			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
// 			File entityDefinitionLocation,
// 			Map<Integer, Table> tables,
// 			DisjointHeaders disjointHeaders) {
		
// 		//TODO unfinished code here ...
// 		DeterminantMatcher m = new DeterminantMatcher();
// //		m.createCandidateKeys(1, tables.values(), attributes);
// 		Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> determinantCors = m.propagateDependencies(schemaCorrespondences, candidateKeys);
		
// 		ConnectedComponentClusterer<Integer> tableKeyClusterer = new ConnectedComponentClusterer<>();
// 		for(Correspondence<MatchableTableDeterminant, MatchableTableColumn> cor : determinantCors.get()) {
// 			System.out.println(String.format("\t#%d{%s} <-> #%d{%s}",
// 					cor.getFirstRecord().getTableId(),
// 					StringUtils.join(Q.project(cor.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ","),
// 					cor.getSecondRecord().getTableId(),
// 					StringUtils.join(Q.project(cor.getSecondRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ",")
// 					));
// 			tableKeyClusterer.addEdge(new Triple<Integer, Integer, Double>(cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId(), cor.getSimilarityScore()));
// 		}
// 		for(Collection<Integer> cluster : tableKeyClusterer.createResult().keySet()) {
// 			System.out.println(String.format("Cluster %s", StringUtils.join(cluster, ",")));
// 			for(Integer tableId : cluster) {
// 				System.out.println(String.format("\t{%s}", StringUtils.join(Q.project(tables.get(tableId).getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
// 			}
// 		}
		
// 		Collection<EntityTable> entityTables = new LinkedList<>();
		
// 		// remove all correspondences that do not belong to a key mapping
// 		KeyCorrespondenceFilter filter = new KeyCorrespondenceFilter();
// 		System.out.println(String.format("%d correspondences before key-filtering", schemaCorrespondences.size()));
// 		schemaCorrespondences = filter.runBlocking(candidateKeys, true, schemaCorrespondences);
// 		System.out.println(String.format("%d correspondences after key-filtering", schemaCorrespondences.size()));
		
// 		// create the entities by clustering the correspondences between tables
// 		// create the attributes by clustering the correspondences between columns
// 		ConnectedComponentClusterer<Integer> tableClusterer = new ConnectedComponentClusterer<>();
// 		ConnectedComponentClusterer<MatchableTableColumn> columnClusterer = new ConnectedComponentClusterer<>();
// 		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
// 			columnClusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
// 			tableClusterer.addEdge(new Triple<Integer, Integer, Double>(cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId(), cor.getSimilarityScore()));
// 		}
// 		Map<Collection<Integer>, Integer> tableClustering = tableClusterer.createResult();
// 		Map<Collection<MatchableTableColumn>, MatchableTableColumn> columnClustering = columnClusterer.createResult();
		
// 		//TODO change table clustering ... there should be more clusters ... based on key mapping?
// 		int entityId = 0;
// 		for(Collection<Integer> tableCluster : tableClustering.keySet()) {
// 			Map<String, Collection<MatchableTableColumn>> entityAttributes = new HashMap<>();
// 			Collection<Collection<Collection<MatchableTableColumn>>> entityKeys = new HashSet<>();
// 			Map<MatchableTableColumn, Collection<MatchableTableColumn>> columnToCluster = new HashMap<>();
			
// 			// create the attributes
// 			for(Collection<MatchableTableColumn> columnCluster : columnClustering.keySet()) {
				
// 				MatchableTableColumn first = Q.firstOrDefault(columnCluster);
				
// 				if(tableCluster.contains(first.getTableId())) {
					
// 					Distribution<String> headerDistribution = Distribution.fromCollection(columnCluster, (c)->c.getHeader());
// 					entityAttributes.put(headerDistribution.getMode(), columnCluster);
					
// 					for(MatchableTableColumn c : columnCluster) {
// 						columnToCluster.put(c, columnCluster);
// 					}
// 				}
				
// 			}
			
// 			// create the candidate keys
// 			for(Integer tableId : tableCluster) {
// 				for(MatchableTableDeterminant key : candidateKeys.where((c)->c.getTableId()==tableId).get()) {
					
// 					Collection<Collection<MatchableTableColumn>> entityKey = new LinkedList<>();
					
// 					boolean isMapped = true;
					
// 					for(MatchableTableColumn col : key.getColumns()) {
						
// 						Collection<MatchableTableColumn> cluster = columnToCluster.get(col);
						
// 						if(cluster==null) {
// 							isMapped = false;
// 							break;
// 						} else {
// 							entityKey.add(cluster);
// 						}
						
// 					}
					
// 					if(isMapped) {
// 						entityKeys.add(entityKey);
// 					}
// 				}
// 			}
			
// 			// remove subsets from candidate keys
// 			Iterator<Collection<Collection<MatchableTableColumn>>> keyIt = entityKeys.iterator();
// 			while(keyIt.hasNext()) {
// 				Collection<Collection<MatchableTableColumn>> key = keyIt.next();
				
				
// 				if(Q.where(entityKeys, (k)->k.containsAll(key)).size()>1) {
// 					// if any key other than key itself contains the same set of attributes, remove key
// 					keyIt.remove();
// 				}
// 			}
			
// 			//TODO not all keys are valid for all tables ... how do we select which keys to use?
			
// 			EntityTable et = new EntityTable();
// 			et.setEntityName(String.format("e%d", entityId++));
// 			et.setAttributes(entityAttributes);
// 			et.setCanddiateKeys(entityKeys);
// 			entityTables.add(et);
// 		}
		
// 		return entityTables;
		
// 	}
	
	protected Map<Collection<Integer>, Integer> getTableClusters(
			Processable<MatchableTableColumn> attributes, 
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		ConnectedComponentClusterer<Integer> clusterer = new ConnectedComponentClusterer<>();
		
		for(MatchableTableColumn c : attributes.get()) {
			clusterer.addEdge(new Triple<Integer, Integer, Double>(c.getTableId(), c.getTableId(), 1.0));
		}
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
			clusterer.addEdge(new Triple<Integer, Integer, Double>(cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId(), cor.getSimilarityScore()));
		}
		
		return clusterer.createResult();
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> getCorrespondencesForTableCluster(
			Collection<Integer> cluster,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
			
			Processable<Correspondence<MatchableTableColumn, Matchable>> filtered = new ProcessableCollection<>();
			
			for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
				if(cluster.contains(cor.getFirstRecord().getTableId())) {
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
			
			return clusterer.createResult();
		}


			
		
}
