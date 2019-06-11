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
package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EntityTable {

	public static final String NO_ENTITY = "ne";
	
	Map<String, Collection<MatchableTableColumn>> attributes;
	Map<String, Set<String>> attributeProvenance;
	Set<String> provenance;
	Map<String, String> attributeMapping;
	Collection<Collection<Collection<MatchableTableColumn>>> candidateKeys;
	Collection<Collection<String>> relations;
	Collection<Collection<String>> stitchedRelations;
	Map<Set<String>, Collection<String>> stitchedRelationKeys;
	Map<Set<String>, Set<String>> functionalDependencies;
	String entityName;
	
	public EntityTable() {
		attributes = new HashMap<>();
		attributeProvenance = new HashMap<>();
		provenance = new HashSet<>();
		attributeMapping = new HashMap<>();
		candidateKeys = new LinkedList<>();
		relations = new LinkedList<>();
		stitchedRelations = new LinkedList<>();
		stitchedRelationKeys = new HashMap<>();
		functionalDependencies = new HashMap<>();
	}
	
	/**
	 * @return the attributes
	 */
	public Map<String, Collection<MatchableTableColumn>> getAttributes() {
		return attributes;
	}
	
	/**
	 * @return the attributeMapping
	 */
	public Map<String, String> getAttributeMapping() {
		return attributeMapping;
	}
	
	/**
	 * @return the canddiateKeys
	 */
	public Collection<Collection<Collection<MatchableTableColumn>>> getCanddiateKeys() {
		return candidateKeys;
	}
	
	/**
	 * @param attributes the attributes to set
	 */
	public void setAttributes(Map<String, Collection<MatchableTableColumn>> attributes) {
		this.attributes = attributes;
	}
	
	/**
	 * @param attributeMapping the attributeMapping to set
	 */
	public void setAttributeMapping(Map<String, String> attributeMapping) {
		this.attributeMapping = attributeMapping;
	}
	
	/**
	 * @param canddiateKeys the canddiateKeys to set
	 */
	public void setCanddiateKeys(Collection<Collection<Collection<MatchableTableColumn>>> canddiateKeys) {
		this.candidateKeys = canddiateKeys;
	}

	public Collection<Collection<String>> getRelations() {
		return relations;
	}

	public void setRelations(Collection<Collection<String>> relations) {
		this.relations = relations;
	}

	public Collection<Collection<String>> getStitchedRelations() {
		return stitchedRelations;
	}

	public Map<Set<String>, Collection<String>> getStitchedRelationKeys() {
		return stitchedRelationKeys;
	}

	public Map<Set<String>, Set<String>> getFunctionalDependencies() {
		return functionalDependencies;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public boolean isNonEntity() {
		return NO_ENTITY.equals(entityName);
	}

	/**
	 * @return the attributeProvenance
	 */
	public Map<String, Set<String>> getAttributeProvenance() {
		return attributeProvenance;
	}

	/**
	 * @param attributeProvenance the attributeProvenance to set
	 */
	public void setAttributeProvenance(Map<String, Set<String>> attributeProvenance) {
		this.attributeProvenance = attributeProvenance;
	}

	/**
	 * @return the provenance
	 */
	public Set<String> getProvenance() {
		return provenance;
	}

	/**
	 * @param provenance the provenance to set
	 */
	public void setProvenance(Set<String> provenance) {
		this.provenance = provenance;
	}

	public int getNumberOfOriginalColumnsForRelation(Collection<String> relation, EntityTable nonEntityTable, Map<String, Integer> originalWebTableCount) {
		int total = 0;

		for(String attribute : relation) {
			if(getAttributeProvenance().containsKey(attribute)) {
				for(String prov : getAttributeProvenance().get(attribute)) {
					String tbl = prov.split("~")[0];
					if(originalWebTableCount.containsKey(tbl)) {	// the table can only be missing if we didn't load all input tables
						total += originalWebTableCount.get(tbl);
					}
				}
			} else if(nonEntityTable.getAttributeProvenance().containsKey(attribute)) {
				for(String prov : nonEntityTable.getAttributeProvenance().get(attribute)) {
					String tbl = prov.split("~")[0];
					if(originalWebTableCount.containsKey(tbl)) {
						total += originalWebTableCount.get(tbl);
					}
				}
			}
		}

		return total;
	}

	public List<String> sortByNumberOfOriginalColumns(Collection<String> relation, EntityTable nonEntityTable, Map<String, Integer> originalWebTableCount) {
		List<Pair<String, Integer>> attributesWithProvenanceSize = new LinkedList<>();

		for(String attribute : relation) {
			int provCount = 0;
			if(getAttributeProvenance().containsKey(attribute)) {
				for(String prov : getAttributeProvenance().get(attribute)) {
					String tbl = prov.split("~")[0];
					if(originalWebTableCount.containsKey(tbl)) {	// the table can only be missing if we didn't load all input tables
						provCount += originalWebTableCount.get(tbl);
					}
				}
			} else if(nonEntityTable.getAttributeProvenance().containsKey(attribute)) {
				for(String prov : nonEntityTable.getAttributeProvenance().get(attribute)) {
					String tbl = prov.split("~")[0];
					if(originalWebTableCount.containsKey(tbl)) {
						provCount += originalWebTableCount.get(tbl);
					}
				}
			}
			attributesWithProvenanceSize.add(new Pair<>(attribute, provCount));
		}

		// for(Pair<String, Integer> p : Q.sort(attributesWithProvenanceSize, (p1,p2)->-Integer.compare(p1.getSecond(), p2.getSecond()))) {
		// 	System.out.println(String.format("\t\t%s\t%d",p.getFirst(), p.getSecond()));
		// }

		return new ArrayList<>(Q.<String,Pair<String, Integer>>project(Q.sort(attributesWithProvenanceSize, (p1,p2)->-Integer.compare(p1.getSecond(), p2.getSecond())), (p)->p.getFirst()));
	}

	public Set<String> getReducedRelation(Collection<String> relation, Collection<TableColumn> matchedColumns, EntityTable nonEntityTable) {
		Set<String> reducedRelation = new HashSet<>();

		for(TableColumn matchedColumn : matchedColumns) {

			List<String> prov = matchedColumn.getProvenance();

			int maxOverlap = 0;
			String maxAttribute = null;

			// check attributes of this entity table for matches
			for(String attribute : getAttributes().keySet()) {
				Set<String> attProv = getAttributeProvenance().get(attribute);
				int intersectionSize = Q.intersection(prov, attProv).size();
				if(intersectionSize>maxOverlap) {
					maxOverlap = intersectionSize;
					maxAttribute = attribute;
				}
			}

			// check attributes of the non-entity table for matches
			for(String attribute : nonEntityTable.getAttributes().keySet()) {
				Set<String> attProv = nonEntityTable.getAttributeProvenance().get(attribute);
				int intersectionSize = Q.intersection(prov, attProv).size();
				if(intersectionSize>maxOverlap) {
					maxOverlap = intersectionSize;
					maxAttribute = attribute;
				}
			}

			if(maxAttribute!=null) {
				reducedRelation.add(maxAttribute);
			}
		}

		return reducedRelation;
	}

	public Collection<TableColumn> translateAttributes(Table t, Collection<String> relation, EntityTable nonEntityTable) {
		Collection<TableColumn> relationColumns = new LinkedList<>();
		
		for(String att : relation) {
			// System.out.println(String.format("[EntityTable.translateAttributes] Translating attribute '%s' for entity table %s",att, getEntityName()));
			Collection<MatchableTableColumn> attColumns = getAttributes().get(att);
			Set<String> attProv = getAttributeProvenance().get(att);
			
			if(nonEntityTable!=null && attColumns==null) {
				// System.out.println("[EntityTable.translateAttributes]\tAttribute is not contained in entity tables attributes, checking non-entity attributes");
				attColumns = nonEntityTable.getAttributes().get(att);
				attProv = nonEntityTable.getAttributeProvenance().get(att);
			} 

			// if(attColumns!=null) {
			// 	System.out.println(String.format("[EntityTable.translateAttributes]\tAttribute '%s' has columns: %s",
			// 	att,
			// 		StringUtils.join(Q.project(attColumns, (c)->c.toString()), ",")
			// 	));
			// }
			
			int maxOverlap = 0;
			TableColumn maxColumn = null;

			if(attProv!=null) {
				for(TableColumn c : t.getColumns()) {
					Set<String> matches = Q.intersection(c.getProvenance(), attProv);
					if(matches.size()>maxOverlap) {
						maxOverlap = matches.size();
						maxColumn = c;
					}
				}
				if(maxColumn!=null) {
					relationColumns.add(maxColumn);
				}
			}

			if(maxColumn==null) {
				System.out.println(String.format("[EntityTable.translateAttributes] unable to translate attribute '%s' (%s)", att, StringUtils.join(attProv, ",")));
			}
		}

		return relationColumns;
	}

	public int getNumberOfMatchedColumns(Collection<TableColumn> attributes, Collection<String> relation, EntityTable nonEntityTable, Map<String, Integer> originalWebTableCount) {
		int matchedColumns = 0;
		
		for(String att : relation) {
			// System.out.println(String.format("[EntityTable.translateAttributes] Translating attribute '%s' for entity table %s",att, getEntityName()));
			Collection<MatchableTableColumn> attColumns = getAttributes().get(att);
			Set<String> attProv = getAttributeProvenance().get(att);
			
			if(nonEntityTable!=null && attColumns==null) {
				// System.out.println("[EntityTable.translateAttributes]\tAttribute is not contained in entity tables attributes, checking non-entity attributes");
				attColumns = nonEntityTable.getAttributes().get(att);
				attProv = nonEntityTable.getAttributeProvenance().get(att);
			} 

			// if(attColumns!=null) {
			// 	System.out.println(String.format("[EntityTable.translateAttributes]\tAttribute '%s' has columns: %s",
			// 	att,
			// 		StringUtils.join(Q.project(attColumns, (c)->c.toString()), ",")
			// 	));
			// }
			
			int maxOverlap = 0;
			TableColumn maxColumn = null;

			if(attProv!=null) {
				Set<String> maxMatches = null;
				Set<String> matches = null;
				for(TableColumn c : attributes) {
					matches = Q.intersection(c.getProvenance(), attProv);
					if(matches.size()>maxOverlap) {
						maxOverlap = matches.size();
						maxColumn = c;
						maxMatches = matches;
					}
				}
				if(maxColumn!=null) {
					for(String col : maxMatches) {
						String tbl = col.split("~")[0];
						matchedColumns += originalWebTableCount.get(tbl);
					}
					// matchedColumns += matches.size();
				}
			}
		}

		return matchedColumns;
	}

	public void renameAttributes(Table t, Collection<String> relation, EntityTable nonEntityTable) {
		for(String att : relation) {
			Collection<MatchableTableColumn> attColumns = getAttributes().get(att);
			Set<String> attProv = getAttributeProvenance().get(att);
			
			if(nonEntityTable!=null && attColumns==null) {
				attColumns = nonEntityTable.getAttributes().get(att);
				attProv = nonEntityTable.getAttributeProvenance().get(att);
			} 
			
			int maxOverlap = 0;
			TableColumn maxColumn = null;
			if(attProv!=null) {
				for(TableColumn c : t.getColumns()) {
					Set<String> matches = Q.intersection(c.getProvenance(), attProv);
					if(matches.size()>maxOverlap) {
						maxOverlap = matches.size();
						maxColumn = c;
					}
				}
				if(maxColumn!=null) {
					maxColumn.setHeader(att);
				}
			}

			if(maxColumn==null) {
				System.out.println(String.format("[EntityTable.translateAttributes] unable to translate attribute '%s'", att));
			}

			// boolean found = false;
			// for(TableColumn c : t.getColumns()) {
			// 	// Set<String> matches = Q.intersection(c.getProvenance(), Q.project(attColumns, new MatchableTableColumn.ColumnIdProjection()));
			// 	Set<String> matches = Q.intersection(c.getProvenance(), attProv);
			// 	if(matches.size()>0) {
			// 		c.setHeader(att);
			// 		found = true;
			// 		break;
			// 	}
			// }

			// if(!found) {
			// 	System.out.println(String.format("[EntityTable.renameAttributes] unable to translate attribute name %s", att));
			// }
		}
	}

	public static Collection<EntityTable> loadFromDefinition(
			Processable<MatchableTableColumn> attributes, 
			File structureFile, 
			File candidateKeyFile,
			File relationsFile, 
			File stitchedRelationsFile, 
			File functionalDependenciesFile,
			boolean addContextColumnsToEntityTables,
			Map<String, String> columnIdTranslation) throws IOException {
		
		Set<MatchableTableColumn> assignedAttributes = new HashSet<>();
		
		/******************************************************
		 * READ CANDIDATE KEYS
		 ******************************************************/
		
		BufferedReader r = new BufferedReader(new FileReader(candidateKeyFile));
		
		Map<String, EntityTable> entityToGroup = new HashMap<>();
		
		String line = null;
		
		// maps entity name to all candidate keys
		// -> the candidate keys are a map from attribute name to all keys that contain the attribute
		Map<String, Map<String, Collection<Collection<Collection<MatchableTableColumn>>>>> candidateKeys = new HashMap<>();
		Map<String, Collection<Collection<Collection<MatchableTableColumn>>>> allKeys = new HashMap<>();
		
		while((line = r.readLine())!=null) {
			String[] values = line.split("\t");
			String entity = values[0];
			String key = values[1];
			
			Map<String, Collection<Collection<Collection<MatchableTableColumn>>>> entityMap = candidateKeys.get(entity);
			if(entityMap==null) {
				entityMap = new HashMap<>();
				candidateKeys.put(entity, entityMap);
			}
			
			Collection<Collection<MatchableTableColumn>> thisKey = new LinkedList<>();
			
			for(String attribute : key.split(",")) {
				
				Collection<Collection<Collection<MatchableTableColumn>>> attributeKeys = entityMap.get(attribute);
				
				if(attributeKeys==null) {
					attributeKeys = new LinkedList<>();
					entityMap.put(attribute, attributeKeys);
				}
				
				// add an empty list, columns are added after we translated attribute names to column identifiers in the next step
				attributeKeys.add(thisKey);
			}
			
			Collection<Collection<Collection<MatchableTableColumn>>> entityKeys = allKeys.get(entity);
			if(entityKeys==null) {
				entityKeys = new LinkedList<>();
				allKeys.put(entity, entityKeys);
			}
			entityKeys.add(thisKey);
		}
		
		r.close();
		
		/******************************************************
		 * READ ENTITY STRUCTURE
		 ******************************************************/
		
		r = new BufferedReader(new FileReader(structureFile));
		
		while((line = r.readLine())!=null) {
			
			// parse line
			String[] values = line.split("\t");
			
			// entity name
			String entity = values[0];
			// attribute name
			String attribute = values[1];
			
			Collection<MatchableTableColumn> cols = null;
			Set<String> columnIds = null;
			if(values.length>2) {
				// column identifiers
				String columns = values[2];
				// split the list of column identifiers
				columnIds = new HashSet<>(Arrays.asList(columns.split(",")));
				
				Set<String> columnIdsFinal = columnIds;
				if(columnIdTranslation==null) {
					cols = attributes.where((c)->columnIdsFinal.contains(c.getIdentifier())).get();
				} else {
					//Set<String> translatedColumnIds = new HashSet<>(Q.project(columnIds, (s)->columnIdTranslation.get(s)));
					Set<String> translatedColumnIds = new HashSet<>();
					for(String columnId : columnIds) {
						String translated = columnIdTranslation.get(columnId);
						if(translated==null) {
							System.out.println(String.format("Could not translate column id '%s'!", columnId));
							translated = columnId;
						}
						translatedColumnIds.add(translated);
					}
					columnIds = translatedColumnIds;
					cols = attributes.where((c)->translatedColumnIds.contains(c.getIdentifier())).get();
				}
				// get all columns that are in the column identifier list
			} else if(ContextColumns.isContextColumn(attribute)) {
				cols = attributes.where((c)->c.getHeader().equals(attribute)).get();
				columnIds = new HashSet<>(Q.project(cols, (c)->c.getIdentifier()));
			}
	
			
			// get the entityTable for the entity
			EntityTable group = entityToGroup.get(entity);
			
			// or create it if it does not exist yet
			if(group == null) {
				group = new EntityTable();
				group.setEntityName(entity);
				entityToGroup.put(entity, group);
				
				group.setCanddiateKeys(allKeys.get(entity));
			}

			// entity context columns are created after splitting the input tables into PK-FK relations, so we cannot find them at this point
			if(!attribute.startsWith("Entity Context")) {
				// we couldn't find any of the columns in the provided list of attributes
				if(cols==null || cols.size()==0) {
					System.out.println(String.format("Invalid attribute cluster: %s", attribute));
					continue;
				}
			}
			
			// add the column list as new attribute to the entity table
			group.getAttributes().put(attribute, cols);
			group.getAttributeProvenance().put(attribute, columnIds);
			for(String colId : columnIds) {
				group.getProvenance().add(colId.split("~")[0]);
			}
			// for(MatchableTableColumn c : cols) {
			// 	group.getProvenance().add(c.getIdentifier().split("~")[0]);
			// }
			
			/// get the entity table's candidate keys
			Map<String, Collection<Collection<Collection<MatchableTableColumn>>>> keys = candidateKeys.get(entity);
			
			// if any exist
			if(keys!=null) {
				
				// get all keys that contain the attribute
				Collection<Collection<Collection<MatchableTableColumn>>> keysWithAttribute = keys.get(attribute);
				
				if(keysWithAttribute!=null) {
					
					// add the columns to all of those keys
					for(Collection<Collection<MatchableTableColumn>> key : keysWithAttribute) {
						key.add(cols);
					}
					
				}
				
			}
			
			// remember which columns have already been assigned to an attribute
			assignedAttributes.addAll(cols);
		}
		
		r.close();
		
		if(addContextColumnsToEntityTables) {
			// add all context attributes
			// for each entity, we add all context columns from all tables that have attributes belonging to this entity
			for(EntityTable entity : entityToGroup.values()) {
				
				// list all tables for this entity
				Set<Integer> tables = new HashSet<>();
				
				for(Collection<MatchableTableColumn> clu : entity.getAttributes().values()) {
					
					for(MatchableTableColumn col : clu) {
						
						tables.add(col.getTableId());
						
					}
					
				}
				
				// select all context attributes from the selected tables
				Processable<Group<String, MatchableTableColumn>> contextAttributes = attributes
						.where((c)->tables.contains(c.getTableId()) && ContextColumns.isContextColumn(c))
						.group((MatchableTableColumn att, DataIterator<Pair<String, MatchableTableColumn>> c)
								->c.next(new Pair<String, MatchableTableColumn>(att.getHeader(), att)));
				
				// add all context attributes to the entity
				for(Group<String, MatchableTableColumn> group : contextAttributes.get()) {
					
					Collection<MatchableTableColumn> contextAttribute = group.getRecords().get();
					
					if(contextAttribute.size()>0) {
	
						String name = Q.firstOrDefault(contextAttribute).getHeader();
						
						// only add if it does not exist already (if it was used in a candidate key)
						if(!entity.getAttributes().containsKey(name)) {
							entity.getAttributes().put(name, contextAttribute);
						}
					
					}
					
				}
				
			}
		} else {
			// add all context attributes to the 'ne' group
			// EntityTable nonEntity = entityToGroup.get("ne");
			
			// if(nonEntity==null) {
			// 	nonEntity = new EntityTable();
			// 	nonEntity.setEntityName("ne");
			// 	entityToGroup.put(nonEntity.getEntityName(), nonEntity);
			// }

			// Processable<Group<String, MatchableTableColumn>> grouped = attributes.group((MatchableTableColumn record,DataIterator<Pair<String, MatchableTableColumn>> resultCollector) 
			// 		-> {					
			// 			if(ContextColumns.isContextColumn(record)) {
			// 				resultCollector.next(new Pair<>(record.getHeader(), record));
			// 			}
			// 		});
			
			// for(Group<String, MatchableTableColumn> group : grouped.get()) {
				
			// 	Collection<MatchableTableColumn> unassignedColumns = Q.without(group.getRecords().get(), assignedAttributes);
				
			// 	if(unassignedColumns.size()>0) {
			// 		nonEntity.getAttributes().put(group.getKey(), unassignedColumns);
			// 	}
			// }
		}

		if(!entityToGroup.containsKey(EntityTable.NO_ENTITY)) {
			EntityTable nonEntity = new EntityTable();
			nonEntity.setEntityName(EntityTable.NO_ENTITY);
			entityToGroup.put(EntityTable.NO_ENTITY, nonEntity);
		}
		
		/******************************************************
		 * READ RELATIONS
		 ******************************************************/
		
		if(relationsFile!=null) {
			r = new BufferedReader(new FileReader(relationsFile));
			
			EntityTable nonEntity = entityToGroup.get("ne");
			
			int lineNo = 1;
			while((line = r.readLine())!=null) {
				String[] values = line.split("\t");
				
				if(values.length==2) {
					String entity = values[0];
					String relation = values[1];
					
					String[] relAttributes = relation.split(",");
					
					if(relAttributes.length!=Q.toSet(relAttributes).size()) {
						System.err.println(String.format("Invalid relation specification: %s", StringUtils.join(relAttributes, ",")));
					}
					
					EntityTable et = entityToGroup.get(entity);
					
					if(et!=null) {
					
						for(String attribute : relAttributes) {
							if(et.getAttributes().get(attribute)==null && nonEntity.getAttributes().get(attribute)==null) {
								System.err.println(String.format("No columns defined for attribute '%s' in relation %s", attribute, StringUtils.join(relAttributes, ",")));
							}
						}
						
						et.getRelations().add(Arrays.asList(relAttributes));
						
					} else {
						System.err.println(String.format("Entity %s not found (in file %s)", entity, relationsFile.getAbsolutePath()));
					}
				} else {
					System.err.println(String.format("Incomplete relation specification in file %s line %d", relationsFile.getAbsolutePath(), lineNo));
				}
				lineNo++;
			}
			
			r.close();
		}
		
		/******************************************************
		 * READ STITCHED RELATIONS
		 ******************************************************/
		
		if(stitchedRelationsFile!=null) {
			r = new BufferedReader(new FileReader(stitchedRelationsFile));
			
			EntityTable nonEntity = entityToGroup.get("ne");
			
			int lineNo = 1;
			while((line = r.readLine())!=null) {
				String[] values = line.split("\t");
				
				if(values.length>=2) {
					String entity = values[0];
					String relation = values[1];
					String key = null;

					if(values.length>2) {
						key = values[2];
					}
					
					String[] relAttributes = relation.split(",");
					
					if(relAttributes.length!=Q.toSet(relAttributes).size()) {
						System.err.println(String.format("Invalid relation specification: %s", StringUtils.join(relAttributes, ",")));
					}
					
					EntityTable et = entityToGroup.get(entity);
					
					if(et!=null) {
					
						for(String attribute : relAttributes) {
							if(et.getAttributes().get(attribute)==null && nonEntity.getAttributes().get(attribute)==null) {
								System.err.println(String.format("No columns defined for attribute '%s' in relation %s", attribute, StringUtils.join(relAttributes, ",")));
							}
						}
						
						Collection<String> rel = Arrays.asList(relAttributes);
						et.getStitchedRelations().add(rel);
						if(key!=null) {
							List<String> relKey = new LinkedList<>(Arrays.asList(key.split(",")));
							relKey.remove("FK");
							et.getStitchedRelationKeys().put(new HashSet<>(rel), relKey);
						}
						
					} else {
						System.err.println(String.format("Entity %s not found (in file %s)", entity, relationsFile.getAbsolutePath()));
					}
				} else {
					System.err.println(String.format("Incomplete relation specification in file %s line %d", relationsFile.getAbsolutePath(), lineNo));
				}
				lineNo++;
			}
			
			r.close();
		}

		/******************************************************
		 * READ FUNCTIONAL DEPENDENCIES
		 ******************************************************/
		
		if(functionalDependenciesFile!=null) {
			r = new BufferedReader(new FileReader(functionalDependenciesFile));
			
			EntityTable nonEntity = entityToGroup.get("ne");
			
			int lineNo = 1;
			while((line = r.readLine())!=null) {
				String[] values = line.split("\t");
				
				if(values.length==3) {
					String entity = values[0];
					String determinant = values[1];
					String dependant = values[2];
					
					String[] determinantAttributes = determinant.split(",");
					String[] dependantAttributes = dependant.split(",");
					
					if(determinantAttributes.length!=Q.toSet(determinantAttributes).size()) {
						System.err.println(String.format("Invalid determinant specification: %s", StringUtils.join(determinantAttributes, ",")));
					}
					if(dependantAttributes.length!=Q.toSet(dependantAttributes).size()) {
						Collection<String> duplicates = Q.toList(dependantAttributes);
						for(String s : Q.toSet(dependantAttributes)) {
							duplicates.remove(s);
						}
						System.err.println(String.format("Invalid dependant specification: duplicates {%s} in {%s}", 
							StringUtils.join(duplicates, ","),
							StringUtils.join(dependantAttributes, ",")
						));
					}
					
					EntityTable et = entityToGroup.get(entity);
					
					if(et!=null) {
					
						for(String attribute : determinantAttributes) {
							if(et.getAttributes().get(attribute)==null && nonEntity.getAttributes().get(attribute)==null && !"FK".equals(attribute)) {
								System.err.println(String.format("No columns defined for attribute '%s' in FD {%s}->{%s}", 
									attribute, 
									StringUtils.join(determinantAttributes, ","),
									StringUtils.join(dependantAttributes, ",")
									));
							}
						}

						for(String attribute : dependantAttributes) {
							if(et.getAttributes().get(attribute)==null && nonEntity.getAttributes().get(attribute)==null) {
								System.err.println(String.format("No columns defined for attribute '%s' in FD {%s}->{%s}", 
									attribute, 
									StringUtils.join(determinantAttributes, ","),
									StringUtils.join(dependantAttributes, ",")
									));
							}
						}
						
						Set<String> det = Q.toSet(determinantAttributes);
						Set<String> dep = Q.toSet(dependantAttributes);
						et.getFunctionalDependencies().put(det, dep);
						System.out.println(String.format("Reference FD loaded - %s: {%s}->{%s}", 
							et.getEntityName(),
							StringUtils.join(det, ","),
							StringUtils.join(dep, ",")
						));
						
					} else {
						System.err.println(String.format("Entity %s not found (in file %s)", entity, functionalDependenciesFile.getAbsolutePath()));
					}
				} else {
					System.err.println(String.format("Incomplete functional dependency specification in file %s line %d", functionalDependenciesFile.getAbsolutePath(), lineNo));
				}
				lineNo++;
			}
			
			r.close();
		}

		return new ArrayList<>(entityToGroup.values());
	}

}
