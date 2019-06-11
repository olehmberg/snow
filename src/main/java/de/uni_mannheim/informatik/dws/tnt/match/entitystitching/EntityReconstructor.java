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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.EntityTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.P;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * Reconstructs entity-centric tables.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EntityReconstructor {

	/**
	 * 
	 * @param firstTableId
	 * @param web
	 * 		maps the table ids to tables (needed to access the column provenance lists)
	 * @param records
	 * @param entityCentricAttributeGroups
	 * @return
	 */
	public Map<Table, EntityTable> reconstruct(int firstTableId,
			Map<Integer, Table> web,
			Processable<MatchableTableRow> records, 
			Collection<EntityTable> entityCentricAttributeGroups,
			String tableNamePrefix) {
		
		Map<Table, EntityTable> reconstructedTables = new HashMap<>();
		
		int tid = firstTableId;
		
		// for each group of attributes that identifies a type of entity
		for(EntityTable group : entityCentricAttributeGroups) {

			System.out.println(String.format("Extracting entity '%s'",group.getEntityName()));
			if(!group.isNonEntity()) {
			
				// reconstruct a table with the entity columns
				Pair<Table, Map<MatchableTableColumn, TableColumn>> result = createReconstructedTable(tid++, group, web, tableNamePrefix);
				for(TableColumn c : result.getFirst().getColumns()) {
					System.out.println(String.format("\t%s (%s)", c.getHeader(), c.getDataType()));
				}
				
				// get all records in the current group
				Processable<MatchableTableRow> selectedRecords = getRecordsForGroup(group, records);			
				System.out.println(String.format("\tfound %d matching records in input tables", selectedRecords.size()));
				
				// populate the reconstructed table with the selected records
				Table t = populateTable(result.getFirst(), result.getSecond(), selectedRecords);
				
				reconstructedTables.put(t, group);
				System.out.println(String.format("\tpopulated table with %d unique records", t.getSize()));
			}
		}
		
		return reconstructedTables;
	}
	
	protected Pair<Table, Map<MatchableTableColumn, TableColumn>> createReconstructedTable(
			int tableId, 
			EntityTable entityTable,
			Map<Integer, Table> web,
			String tableNamePrefix) {
		Table table = new Table();
		// table.setPath(String.format("%s_%s_%d", entityTable.getEntityName(), tableNamePrefix, tableId));
		table.setPath(String.format("%s_%s", entityTable.getEntityName(), tableNamePrefix));
		table.setTableId(tableId);
		table.getMapping().setMappedClass(new Pair<String, Double>(entityTable.getEntityName(), 1.0));
		
		/*********************************
		 *  Column Clusters
		 *********************************/
		int columnIndex = 0;
		
		Map<MatchableTableColumn, TableColumn> columnMapping = new HashMap<>();
		
		Map<Collection<MatchableTableColumn>, String> clusterToAttributeIndex = MapUtils.invert(entityTable.getAttributes());
		
		// only extract columns which are part of a key
		Collection<Collection<MatchableTableColumn>> attributes = entityTable.getAttributes().values();
		attributes = Q.where(attributes, new P.IsContainedIn<Collection<MatchableTableColumn>>(Q.union(entityTable.getCanddiateKeys())));
		
		List<Collection<MatchableTableColumn>> sorted = Q.sort(attributes, new Comparator<Collection<MatchableTableColumn>>() {

			@Override
			public int compare(Collection<MatchableTableColumn> o1, Collection<MatchableTableColumn> o2) {
				int idx1 = Q.min(o1, new MatchableTableColumn.ColumnIndexProjection()).getColumnIndex();
				int idx2 = Q.min(o2, new MatchableTableColumn.ColumnIndexProjection()).getColumnIndex();
				
				return Integer.compare(idx1, idx2);
			}
		});
		
		// prepare candidate keys
		Set<Set<TableColumn>> candidateKeys = new HashSet<>();
		Map<Collection<MatchableTableColumn>, Collection<Set<TableColumn>>> keyMap = new HashMap<>();
		if(entityTable.getCanddiateKeys()!=null) {
			for(Collection<Collection<MatchableTableColumn>> candidateKey : entityTable.getCanddiateKeys()) {
				Set<TableColumn> key = new HashSet<>();
				candidateKeys.add(key);
				
				for(Collection<MatchableTableColumn> attribute : candidateKey) {
					
					Collection<Set<TableColumn>> keys = keyMap.get(attribute);
					
					if(keys==null) {
						keys = new LinkedList<>();
						keyMap.put(attribute, keys);
					}
					
					keys.add(key);
				}
			}
			table.getSchema().setCandidateKeys(candidateKeys);
		}
		
//		// add the PK column
//		TableColumn pkColumn = new TableColumn(0, table);
//		pkColumn.setHeader("PK");
//		pkColumn.setDataType(DataType.string);
//		table.insertColumn(columnIndex++, pkColumn);
		
		// map all columns in the cluster to a new column in the merged table
		for(Collection<MatchableTableColumn> schemaCluster : sorted) {
			
			String attribute = clusterToAttributeIndex.get(schemaCluster);
			TableColumn c = new TableColumn(columnIndex, table);
			

			Distribution<String> headers = Distribution.fromCollection(schemaCluster, new MatchableTableColumn.ColumnHeaderProjection());
			String newHeader = getMostFrequentHeaderForCluster(schemaCluster, web);
			
			if(attribute!=null) {
				// set the attribute name from the entity definition as header
				c.setHeader(attribute);
				table.getMapping().setMappedProperty(columnIndex, new Pair<String, Double>(entityTable.getAttributeMapping().get(attribute), 1.0));
			} else {
				// use the most frequent header as the header for the merged column
				c.setHeader(newHeader);
			}
			c.setSynonyms(headers.getElements());
			
			// use the most frequent data type
			Distribution<DataType> types = Distribution.fromCollection(schemaCluster, new Func<DataType, MatchableTableColumn>() {

				@Override
				public DataType invoke(MatchableTableColumn in) {
					return in.getType();
				}});
			c.setDataType(types.getMode());
			// System.out.println(newHeader);
			// System.out.println(types.format());
			System.out.println(String.format("Attribute %s (%s)\n\tHeader distribution: %s\n\tType distribution: %s",
				newHeader,
				types.getMode().toString(),
				headers.formatCompact(),
				types.formatCompact()
			));
			
			// add provenance for column
			for(MatchableTableColumn c0 : schemaCluster) {
				c.getProvenance().add(c0.getIdentifier());
			}
			
			table.addColumn(c);
			
			for(MatchableTableColumn col : schemaCluster) {
				columnMapping.put(col, c);
			}
			
			// add the column to the candidate keys that contain it
			Collection<Set<TableColumn>> keys = keyMap.get(schemaCluster);
			if(keys!=null) {
				for(Set<TableColumn> key : keys) {
					key.add(c);
				}
			}
			
			columnIndex++;
		}
		
		return new Pair<Table, Map<MatchableTableColumn,TableColumn>>(table, columnMapping);
	}
	
	protected String getMostFrequentHeaderForCluster(Collection<MatchableTableColumn> schemaCluster, Map<Integer, Table> web) {
		Map<String, Integer> numOriginalColumnsPerHeader = new HashMap<>();
		for(MatchableTableColumn c : schemaCluster) {
			int originalColumns = web.get(c.getTableId()).getSchema().get(c.getColumnIndex()).getProvenance().size();
			MapUtils.add(numOriginalColumnsPerHeader, c.getHeader(), originalColumns);
		}
		return MapUtils.max(numOriginalColumnsPerHeader);
	}
	
	protected Processable<MatchableTableRow> getRecordsForGroup(
			EntityTable group,
			Processable<MatchableTableRow> records) {
		
		Set<Integer> tableIds = new HashSet<>();
		
		for(Collection<MatchableTableColumn> cluster : group.getAttributes().values()) {
			for(MatchableTableColumn col : cluster) {
				tableIds.add(col.getTableId());
			}
		}
		
		return records.where((r)->tableIds.contains(r.getTableId()));
	}
	
	protected Table populateTable(
			Table t, 
			Map<MatchableTableColumn, TableColumn> attributeMapping, 
			Processable<MatchableTableRow> records) {
		
		int rowIdx = 0;
		
//		TableColumn pkColumn = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"PK".equals(c.getHeader())));
		
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
			
//			// set the PK value
//			newRow.set(pkColumn.getColumnIndex(), newRow.getIdentifier());
			
			// add provenance
			newRow.getProvenance().add(row.getIdentifier());
			
			t.addRow(newRow);
		}
		
		// remove exact duplicates from the table
		t.deduplicate(t.getColumns());
		// add the PK column
		TableColumn pkColumn = new TableColumn(0, t);
		pkColumn.setHeader("PK");
		pkColumn.setDataType(DataType.string);
		t.insertColumn(0, pkColumn);
		
		for(TableRow r : t.getRows()) {
			r.set(pkColumn.getColumnIndex(), r.getIdentifier());
		}
		
		
		return t;
	}
	
	public void deduplicateReconstructedTables(Map<Table, EntityTable> entityTables) throws Exception {
		EntityTableDuplicateDetection duplicateDetection = new EntityTableDuplicateDetection();
    	for(Table t : entityTables.keySet()) {
    		
        	// run duplicate detection on entities using context values in matching rule
    		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> duplicates = duplicateDetection.detectDuplicates(t);
    		
        	// deduplicate entities from correspondences
    		EntityDeduplication deduplication = new EntityDeduplication(true);
    		deduplication.mergeDuplicates(t, Correspondence.toMatchable(duplicates));
    		t.reorganiseRowNumbers();
    		
    		// remove all entities that do not have any candidate key value without nulls
    		System.out.println(String.format("%d rows after deduplication", t.getRows().size()));
    		for(Collection<TableColumn> key : t.getSchema().getCandidateKeys()) {
    			System.out.println(String.format("\tremoving violations of candidate key {%s}", StringUtils.join(Q.project(key, new TableColumn.ColumnHeaderProjection()), ",")));
			}
    		System.out.println(String.format("%d rows after removing records with NULLs in candidate keys", t.getRows().size()));
    	}
	}
	
}
