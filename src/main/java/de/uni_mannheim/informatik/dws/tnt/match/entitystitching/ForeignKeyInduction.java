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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.EntityTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableDataSetLoader;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.P;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.Table.ConflictHandling;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ForeignKeyInduction {
	
	private Map<Table, EntityTable> entityTables;
	private Map<Integer,Table> originalTables;
	private DataSet<MatchableTableColumn, MatchableTableColumn> attributes; 
	private File csvLocation;
	private File logDirectory;
	
	private Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences;
	private Map<String, String> recordsToEntity;
	private Map<String, Table> recordsToEntityTable;
	private Map<String, Table> entityIdToEntityTable;
	private Map<String, String> entityColumns;
	
	private boolean keepEntityAttributes = false;
	
	private Collection<Table> tablesWithFK;
	private DataSet<MatchableTableRow, MatchableTableColumn> recordsWithFK;
	
	/**
	 * @return the tablesWithFK
	 */
	public Collection<Table> getTablesWithFK() {
		return tablesWithFK;
	}
	/**
	 * @return the recordsWithFK
	 */
	public DataSet<MatchableTableRow, MatchableTableColumn> getRecordsWithFK() {
		return recordsWithFK;
	}
	/**
	 * @return the attributes
	 */
	public DataSet<MatchableTableColumn, MatchableTableColumn> getAttributes() {
		return attributes;
	}
	/**
	 * @return the schemaCorrespondences
	 */
	public Processable<Correspondence<MatchableTableColumn, Matchable>> getSchemaCorrespondences() {
		return schemaCorrespondences;
	}
	/**
	 * @return the entityIdToEntityTable
	 */
	public Map<String, Table> getEntityIdToEntityTableMap() {
		return entityIdToEntityTable;
	}
	
	public void setKeepEntityAttributes(boolean keepEntityAttributes) {
		this.keepEntityAttributes = keepEntityAttributes;
	}
	
	public ForeignKeyInduction(
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences, 
			Map<Table, EntityTable> entityTables,
			Map<Integer,Table> originalTables,
			DataSet<MatchableTableColumn, MatchableTableColumn> attributes, 
			File csvLocation,
			File logDirectory) {
		this.schemaCorrespondences = schemaCorrespondences;
		this.entityTables = entityTables;
		this.originalTables = originalTables;
		this.attributes = attributes;
		this.csvLocation = csvLocation;
		this.logDirectory = logDirectory;
		
		// create a lookup from record id to new entity id from provenance
		recordsToEntity = new HashMap<>();
		recordsToEntityTable = new HashMap<>();
		entityIdToEntityTable = new HashMap<>();
		
		for(Table t : entityTables.keySet()) {
			for(TableRow r : t.getRows()) {
				TableColumn pkCol = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"PK".equals(c.getHeader())));
				String pk = (String)r.get(pkCol.getColumnIndex());
				
				entityIdToEntityTable.put(pk, t);
				for(String prov : r.getProvenance()) {

					if(recordsToEntity.containsKey(prov)) {
						System.out.println(String.format("Provenance %s for entity %s in table %s already assigned to %s", prov, pk, t.getPath(), recordsToEntity.get(prov)));
					}
					
					recordsToEntity.put(prov, pk);
//					System.out.println(String.format("[ForeignKeyInduction] mapping record %s to entity %s\t\t%s", prov, pk, r.format(40)));
					
					recordsToEntityTable.put(prov, t);
				}
			}
		}

		// get all entity-related column ids
		entityColumns = new HashMap<>();
		for(EntityTable group : entityTables.values()) {
			for(String attribute : group.getAttributes().keySet()) {
				Collection<MatchableTableColumn> cluster = group.getAttributes().get(attribute);
				
				// only extract columns which are part of a key
				cluster = Q.where(cluster, new P.IsContainedIn<MatchableTableColumn>(Q.union(Q.union(group.getCanddiateKeys()))));
				
				for(MatchableTableColumn col : cluster) {
						entityColumns.put(col.getIdentifier(), attribute);
				}
			}
		}
	}
	
	public void replaceEntityColumnsWithForeignKey() throws IOException, AlgorithmExecutionException {
		/*********************************************************************************
		 * Create Foreign Key
		 *********************************************************************************/
		// replace entity columns with FK and create correspondences between all FK columns that reference the same PK
		tablesWithFK = replaceEntityColumnsWithFKs(entityTables.keySet(), originalTables, attributes, entityColumns);
		
		for(Table t : tablesWithFK) {
			System.out.println(String.format("FK Table #%d (%s): {%s} / %d rows", t.getTableId(), t.getPath(), StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","), t.getRows().size()));
		}
		
		// re-calculate functional dependencies
//		FunctionalDependencyUtils.calculcateFunctionalDependencies(tablesWithFK, csvLocation);
		
		// re-load the datasets (records & attributes), as the column indices have changed
		WebTableDataSetLoader loader = new WebTableDataSetLoader();
		recordsWithFK = loader.loadRowDataSet(tablesWithFK);
		
		/*********************************************************************************
		 * remove all entity-related attributes
		 *********************************************************************************/
		// if the only shared correspondences between some tables are the entity-related attributes, these tables will not be merged by the reconstructor
		
		if(!keepEntityAttributes) {
			// remove the entity columns from the *old* attribute dataset (this dataset is still needed to update the correspondences)
			attributes = new ParallelHashedDataSet<>(attributes.where((c) -> !entityColumns.keySet().contains(c.getIdentifier())).get());
			
			// remove all correspondences to entity-related attributes
			schemaCorrespondences = schemaCorrespondences.where(
					(c) -> 
					!(entityColumns.keySet().contains(c.getFirstRecord().getIdentifier()) || entityColumns.keySet().contains(c.getSecondRecord().getIdentifier()))
					);
		}
		
		Correspondence.toGraph(schemaCorrespondences.get()).writePajekFormat(new File(logDirectory, "correspondences_after_FK.net"));
		
		// we removed columns from the tables, so we need to update the schema correspondences (ids have changed)
		// so we translate all old ids (which are in the new columns' provenance) to the new ids
		System.out.println(String.format("%d correspondences before projection", schemaCorrespondences.size()));
		schemaCorrespondences = CorrespondenceProjector.projectCorrespondences(schemaCorrespondences, recordsWithFK.getSchema(), tablesWithFK, attributes,false);
		System.out.println(String.format("%d correspondences after projection", schemaCorrespondences.size()));
		
		Correspondence.toGraph(schemaCorrespondences.get()).writePajekFormat(new File(logDirectory, "correspondences_projected.net"));
		
		// important: during the projection of correspondences, we need both the old and new attributes
		// afterwards, we have to use the new (reloaded) attributes, which are now used by the correspondences
		// reason: creating the foreign keys actually creates new tables, so the attributes have new identifiers
		attributes = recordsWithFK.getSchema();
		
		// remove rows without FK value & tables without FK column
//		postProcessResults();
		
		// reload the records as some might have been removed
//		recordsWithFK = loader.loadRowDataSet(tablesWithFK);
	}
	
	private Collection<Table> replaceEntityColumnsWithFKs(Collection<Table> entityTables,
			Map<Integer,Table> originalTables,
			Processable<MatchableTableColumn> attributes, 
			Map<String, String> entityColumns) throws IOException {
		
		BufferedWriter fkLog = new BufferedWriter(new FileWriter(new File(logDirectory, "fkinduction.log")));
		
		// update the table ids: we change the table structure and add provenance, without a new id the provenance would be ambiguous
		int nextTableId = Q.<Integer>max(Q.project(entityTables, (t)->t.getTableId()))+1;
		
		// add the FK column to all tables
		Map<Table, Collection<MatchableTableColumn>> fkClusters = new HashMap<>();
		Map<String, MatchableTableColumn> entityColumnToContextColumn = new HashMap<>();
		
		Collection<Table> tablesWithFK = new LinkedList<>();
		for(Table t : originalTables.values()) {			
			
			// remove the entity-related columns 
			// important: has to be done after creating all matchable objects, otherwise the column indices will be messed up
			// we have to update the original tables, because the table reconstructor will check them for provenance information 
			List<TableColumn> colsToRemove = new LinkedList<>();
			for(TableColumn c : t.getColumns()) {
				if(entityColumns.keySet().contains(c.getIdentifier())) {
					colsToRemove.add(c);
				} else {
					// all other columns get their old version as provenance (as the column index changes, the id changes as well)
					if(c.getProvenance().size()==0) {
						c.addProvenanceForColumn(c);
					} else {
						c.getProvenance().add(c.getIdentifier());
					}
				}
			}

			
			Set<Table> relatedTables = new HashSet<>();
			List<TableRow> toRemove = new LinkedList<>();
			TableColumn fkColumn = new TableColumn(t.getColumns().size(), t);
			Map<TableColumn, TableColumn> entityColumnToContext = new HashMap<>();
			
			if(colsToRemove.size()>0) { // adding the FK column only makes sense if there were entity columns to replace
				fkColumn.setHeader("FK");
				fkColumn.setDataType(DataType.string);
				t.insertColumn(fkColumn.getColumnIndex(), fkColumn);
				fkColumn.addProvenanceForColumn(fkColumn);
								
				// add context columns for the column headers of entity columns
				// but first sort them by the attribute name (so Entity Context 1/2/etc. always refers to the same attribute!)
				Collections.sort(colsToRemove, (c1,c2)->entityColumns.get(c1.getIdentifier()).compareTo(entityColumns.get(c2.getIdentifier())));
				for(TableColumn c : colsToRemove) {
					TableColumn entityCtx = new TableColumn(t.getColumns().size(), t);
					entityCtx.setHeader(String.format("Entity Context %d", colsToRemove.indexOf(c)+1));
					entityCtx.setDataType(DataType.string);
//					entityCtx.addProvenanceForColumn(entityCtx);
					t.insertColumn(entityCtx.getColumnIndex(), entityCtx);
					entityCtx.addProvenanceForColumn(entityCtx);
					entityColumnToContext.put(c, entityCtx);
				}
				
				// debug: track row ids
				TableColumn rowIdCol = new TableColumn(t.getColumns().size(), t); 
				if(keepEntityAttributes) {
					rowIdCol.setHeader("ID");
					rowIdCol.setDataType(DataType.string);
					t.insertColumn(rowIdCol.getColumnIndex(), rowIdCol);
				}
				
				
				// add the FK values 
				for(TableRow r : t.getRows()) {
					if(keepEntityAttributes) {
						r.set(rowIdCol.getColumnIndex(), r.getIdentifier());
					}
					
					String fk = recordsToEntity.get(r.getIdentifier());
					
					if(fk!=null) {
						relatedTables.add(recordsToEntityTable.get(r.getIdentifier()));
						
						// set the FK
						r.set(fkColumn.getColumnIndex(), fk);
						
						// set the column headers of entity columns as context values
						for(TableColumn c : entityColumnToContext.keySet()) {
							TableColumn ctx = entityColumnToContext.get(c);

							// debugging: if we renamed the columns based on the schema mapping, we must add the original header as value (which we stored in synonyms)
							if(c.getSynonyms()!=null && c.getSynonyms().size()>0) {
								r.set(ctx.getColumnIndex(), Q.firstOrDefault(c.getSynonyms()));
							} else {
								// otherwise add the column header as value
								r.set(ctx.getColumnIndex(), c.getHeader());
							}
						}
					} else {
						toRemove.add(r);
						
						fkLog.write(String.format("[%s] %s no entity found:\t%s\n", t.getPath(), r.getIdentifier(), r.format(30)));
					}
			
				}
			} else {
				System.out.println(String.format("[ForeignKeyInduction] No entity columns in table #%d %s {%s}", 
						t.getTableId(),
						t.getPath(),
						StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")
						));
			}
			
			if(relatedTables.size()>1) {
				System.err.println(String.format("Multiple entity tables for relation table %d", t.getTableId()));
				
				// for debugging purposes, add it to the results
//				tablesWithFK.add(t);
			} else if(relatedTables.size()==1) {
				Table entityTable = Q.firstOrDefault(relatedTables);
				
				// keep track of all FK columns referencing the same entity table, so we can create correspondences between them later
				Collection<MatchableTableColumn> fkColumns = fkClusters.get(entityTable);
				if(fkColumns==null) {
					fkColumns = new LinkedList<>();
					fkClusters.put(entityTable, fkColumns);
				}
				MatchableTableColumn matchableFkColumn = new MatchableTableColumn(t.getTableId(), fkColumn);
				fkColumns.add(matchableFkColumn);
				attributes.add(matchableFkColumn);
				
				// track the entity context columns
				for(TableColumn c : entityColumnToContext.keySet()) {
					TableColumn ctx = entityColumnToContext.get(c);
					MatchableTableColumn matchableCtx = new MatchableTableColumn(t.getTableId(), ctx);
					entityColumnToContextColumn.put(c.getIdentifier(), matchableCtx);
					attributes.add(matchableCtx);
					
					// add entity context to attribute definition in entity table
					EntityTable et = this.entityTables.get(entityTable);
					Collection<MatchableTableColumn> att = MapUtils.get(et.getAttributes(), ctx.getHeader(), new LinkedList<>());
					att.add(matchableCtx);
					// important: add the attribute provenance *before* changing the table id, so we can reference to the entity context columns using the original table names in the definition files
					Set<String> attProv = MapUtils.get(et.getAttributeProvenance(), ctx.getHeader(), new HashSet<>());
					attProv.add(matchableCtx.getIdentifier());
				}
				
				tablesWithFK.add(t);
				
				t.setTableId(nextTableId++);
				t.setPath(String.format("%d.json", t.getTableId()));
				
				if(!keepEntityAttributes) {
					for(TableColumn c : colsToRemove) {
						t.removeColumn(c);
					}
				}
				
				// remove rows that did not get an FK value assigned
				toRemove = Q.sort(toRemove, new Comparator<TableRow>() {

					@Override
					public int compare(TableRow o1, TableRow o2) {
						return -Integer.compare(o1.getRowNumber(), o2.getRowNumber());
					}
					
				});
				
				for(TableRow r : toRemove) {
					t.getRows().remove(r.getRowNumber());
				}
				t.reorganiseRowNumbers();

			} else {
				// this happens if all entities from a certain table have been removed during key enforcement in the entity table
				System.err.println(String.format("[Create FK] Could not find a related entity table for relation table '%s' (%d rows) {%s}", t.getPath(), t.getRows().size(), StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
				
				schemaCorrespondences = schemaCorrespondences.where((c)->c.getFirstRecord().getTableId()!=t.getTableId() && c.getSecondRecord().getTableId()!=t.getTableId());
			}
		}
		
		// create correspondences between FK columns
		for(Collection<MatchableTableColumn> col : fkClusters.values()) {
			List<MatchableTableColumn> colList = Q.sort(col, new MatchableTableColumn.TableIdColumnIndexComparator());
			for(int i = 0; i < colList.size(); i++) {
				for(int j = 0; j < colList.size(); j++) {
					if(i!=j) {
						Correspondence<MatchableTableColumn, Matchable> cor = new Correspondence<MatchableTableColumn, Matchable>(colList.get(i), colList.get(j), 1.0);
						schemaCorrespondences.add(cor);
					}
				}
			}
		}
		
		// create correspondences between entity context columns
		int ecCors = 0;
		ProcessableCollection<Correspondence<MatchableTableColumn, Matchable>> newCors = new ProcessableCollection<>();
		for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
			
			MatchableTableColumn entityCol1 = entityColumnToContextColumn.get(cor.getFirstRecord().getIdentifier());
			MatchableTableColumn entityCol2 = entityColumnToContextColumn.get(cor.getSecondRecord().getIdentifier());
			
			if(entityCol1!=null && entityCol2!=null) {
				Correspondence<MatchableTableColumn, Matchable> c = new Correspondence<MatchableTableColumn, Matchable>(entityCol1, entityCol2, cor.getSimilarityScore());
				newCors.add(c);
				ecCors++;
			}
		}
		System.out.println(String.format("Created %d correspondences between entity context columns", ecCors));
		schemaCorrespondences = schemaCorrespondences.append(newCors);
	
		fkLog.close();
		
		return tablesWithFK;
	}
	
	protected void postProcessResults() {
		/*********************************************************************************
		 * post-process tables: check that FK value exists
		 *********************************************************************************/
		// if rows were removed from the entity table, the corresponding rows in the relation tables cannot have an FK value
		// rows are removed from the entity tables if they violoate constraints, for example, if they contain NULL values
		
		Collection<Table> tablesToRemove = new LinkedList<>();
		
		// check that an FK column exists and filter by density
		for(Table t : tablesWithFK) {			
			Set<Table> relatedTables = new HashSet<>();

			TableColumn fkColumn = null;
			Object firstFKValue = null;
			
			for(TableColumn c : t.getColumns()) {
				if(c.getHeader().equals("FK")) {
					fkColumn = c;
					break;
				}
			}
			
			if(fkColumn!=null) {
				List<TableRow> toRemove = new LinkedList<>();
				
				for(TableRow r : t.getRows()) {
					
					// check if the row has any values
					boolean hasValues = false;
					for(TableColumn c : t.getColumns()) {
						if(!ContextColumns.isContextColumn(c) && c!=fkColumn) {
							if(r.get(c.getColumnIndex())!=null) {
								hasValues=true;
								break;
							}
						}
					}
					
					if(hasValues) {
						Object fkValue = r.get(fkColumn.getColumnIndex());

						if(fkValue!=null) {
							String fk = fkValue.toString();
							
							if(firstFKValue==null) {
								firstFKValue = fkValue;
							}

							Table entityTable = entityIdToEntityTable.get(fk);
							if(entityTable!=null) {
								relatedTables.add(entityTable);
							} else {
								System.out.println(String.format("[ForeignKeyInduction] No entity table for FK value %s!", fk));
							}
						} else {
//							System.err.println(String.format("[RelationReconstructur] Missing FK value for row: %s", r.format(20)));
							toRemove.add(r);
						}
					} else {
						toRemove.add(r); // remove rows that do not contain any values
					}
				}
				
				// remove rows that did not get an FK value assigned
				toRemove = Q.sort(toRemove, new Comparator<TableRow>() {

					@Override
					public int compare(TableRow o1, TableRow o2) {
						return -Integer.compare(o1.getRowNumber(), o2.getRowNumber());
					}
					
				});
				
				for(TableRow r : toRemove) {
					t.getRows().remove(r.getRowNumber());
				}
				
				// deduplicate the table
				System.out.println(String.format("[ForeignKeyInduction] Deduplicating Table '%s': {%s}", t.getPath(), StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
				int before = t.getRows().size();
				t.deduplicate(t.getColumns(), ConflictHandling.KeepFirst);
				System.out.println(String.format("\t\tremoved %d duplicates", before-t.getRows().size()));
			} else {
				System.err.println(String.format("[ForeignKeyInduction] Table #%d has no FK column: %s", t.getTableId(),
						StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
				tablesToRemove.add(t);
				continue;
			}
			
			if(relatedTables.size()>1) {
				System.err.println(String.format("[ForeignKeyInduction] Multiple entity tables for relation table %d: %s", t.getTableId(),
						StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
				
				for(Table rel : relatedTables) {
					System.err.println(String.format("\trelated table #%d: %s", rel.getTableId(), StringUtils.join(Q.project(rel.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
				}
				
				// for debugging purposes, add it to the results
			} else if(relatedTables.size()==1) {
				Table entityTable = Q.firstOrDefault(relatedTables);
				
				t.setPath(String.format("%s_rel_%d", entityTable.getPath(), t.getTableId()));
				t.getMapping().setMappedClass(entityTable.getMapping().getMappedClass());
			} else {
				System.err.println(String.format("[ForeignKeyInduction] Could not find a related entity table for relation table #%d: %s", t.getTableId(),
						StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
				tablesToRemove.add(t);
			}
		}
		
		for(Table t : tablesToRemove) {
			tablesWithFK.remove(t);
		}
	}
}
