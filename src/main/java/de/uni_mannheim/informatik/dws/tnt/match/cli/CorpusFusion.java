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
package de.uni_mannheim.informatik.dws.tnt.match.cli;

import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.parallel.Parallel;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.*;
import de.uni_mannheim.informatik.dws.winter.webtables.Table.ConflictHandling;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;
import java.io.File;
import java.util.*;
import org.apache.commons.lang.StringUtils;
import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.TableReconstructor;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableDataSetLoader;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.EntityTableDuplicateDetection;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers.RelationTableMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.BothTablesFullyMappedFilter;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CorpusFusion extends Executable {

    @Parameter(names = "-web", required=true)
    private String webLocation;

    @Parameter(names = "-join")
    private String joinLocation;

    @Parameter(names = "-results", required=true)
    private String resultLocation;

    @Parameter(names = "-csv")
    private String csvLocation;

    @Parameter(names = "-binary")
    private boolean binaryOnly;

    public static void main(String[] args) throws Exception {
        CorpusFusion app = new CorpusFusion();

        if(app.parseCommandLine(CorpusFusion.class, args)) {
            app.run();
        }
    }

    public void run() throws Exception {
        Parallel.setReportIfStuck(false);

        // load extracted tables
        System.out.println("Loading web tables");
        JsonTableParser p = new JsonTableParser();
        p.setInferSchema(false);
        p.setConvertValues(false);
        Collection<Table> web = new LinkedList<>();
        int tableId = 0;
        for(File f : new File(webLocation).listFiles()) {
            System.out.println(String.format("Loading file %s", f.getName()));
            Table t= p.parseTable(f);
            t.setTableId(tableId++);
            System.out.println(String.format("\tTable %d %s: %s", 
                t.getTableId(),
                t.getPath(),
                StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));

            // read the data types from the table mapping
            // (as the tables can be very large, inferring the data type from values takes too long)
            for(TableColumn c : t.getColumns()) {
                DataType columnType = t.getMapping().getDataType(c.getColumnIndex());
                if(columnType==null) {
                    c.setDataType(DataType.string);
                } else {
                    c.setDataType(columnType);
                }
            }

            t.convertValues();

            web.add(t);

            // if(joinLocation!=null) {
            //     CSVTableWriter w = new CSVTableWriter();
            //     w.write(t, new File(new File(joinLocation), t.getPath()));
            // }
        }
        // WebTables web = WebTables.loadWebTables(new File(webLocation), true, true, true, true);
        // we only need the Table objects
        // web.getRecords().ClearRecords();
        // web.getSchema().ClearRecords();
        File resultFile = new File(resultLocation);
        File csvFile = new File(csvLocation);

        // run this process for each class individually
        Map<String, Collection<Table>> tablesByClass = Q.group(web, (t)->t.getMapping().getMappedClass().getFirst());

        if(tablesByClass.size()==0) {
            System.out.println("Did not find any class mapping!");
        }

        for(String clsName : tablesByClass.keySet()) {
            System.out.println(String.format("Fusing tables for class %s", clsName));
            Collection<Table> tables = tablesByClass.get(clsName);

            // get entity & relation tables
            Collection<Table> entityTables = Q.where(tables, (t)->!t.getPath().contains("_rel_"));
            Collection<Table> relationTables = Q.where(tables, (t)->t.getPath().contains("_rel_"));

            if(binaryOnly) {
                relationTables = Q.where(relationTables, (t)->t.getColumns().size()==2);
            }

            // link entities and merge entity tables
            System.out.println(String.format("Running identity resolution for %d entity tables", entityTables.size()));
            EntityTableDuplicateDetection duplicateDetection = new EntityTableDuplicateDetection();
            Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = duplicateDetection.detectDuplicates(entityTables);
            // update PKs in entity tables
            System.out.println(String.format("Found %d correspondenecs, updating PK values", instanceCorrespondences.size()));
            Map<String, String> pkMapping = updateEntityTablePrimaryKeys(entityTables, instanceCorrespondences);
            // merge and deduplicate entity tables
            System.out.println("Merging entity tables");
            Table entityTable = mergeAndDeduplicateEntityTables(entityTables);
            entityTable.setPath(clsName);
            entityTable.getMapping().setMappedClass(new Pair<>(clsName, 1.0));
            System.out.println(String.format("Completed fusion for entity table of class %s, created %d entities", clsName, entityTable.getSize()));

            // update FKs in relation tables
            System.out.println("Updating FK values in relation tables");
            updateRelationTableForeignKeys(relationTables, pkMapping);

            if(relationTables.size()>0) {
                // match schema and merge relation tables
                System.out.println(String.format("Running schema matching for %d relation tables", relationTables.size()));
                RelationTableMatcher relationTableMatcher = new RelationTableMatcher();
                WebTableDataSetLoader loader = new WebTableDataSetLoader();
                DataSet<MatchableTableRow, MatchableTableColumn> relationTableRecords = loader.loadRowDataSet(relationTables);
                Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = relationTableMatcher.matchRelationTables(relationTables, relationTableRecords, new HashSet<>(pkMapping.values()));

                // stitch relation tables based on correspondences
                System.out.println(String.format("Found %d correspondenecs, stitching relation tables", schemaCorrespondences.size()));
                relationTables = stitchRelationTables(relationTables, relationTableRecords, schemaCorrespondences, clsName);
                System.out.println(String.format("Completed fusion for relation tables of class %s, created %d relation tables", clsName, relationTables.size()));
            } else {
                System.out.println(String.format("No relation tables for class %s", clsName));
            }

            // write results
            CSVTableWriter csvW = new CSVTableWriter();
            JsonTableWriter w = new JsonTableWriter();
            w.setWriteMapping(true);
            w.write(entityTable, new File(resultFile, entityTable.getPath()));
            if(csvLocation!=null) {
                csvW.write(entityTable, new File(csvFile, entityTable.getPath()));
            }
            for(Table t : relationTables) {
                t.getMapping().setMappedClass(new Pair<>(clsName, 1.0));
                w.write(t, new File(resultFile, t.getPath()));
                if(csvLocation!=null) {
                    csvW.write(t, new File(csvFile, t.getPath()));
                }
            }
        }
    }

    protected Collection<Table> stitchRelationTables(
        Collection<Table> relationTables, 
        DataSet<MatchableTableRow, MatchableTableColumn> relationTableRecords,
        Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
        String clsName) {
        //TODO extend! for now, we only stitch completely matching table

        BothTablesFullyMappedFilter filter = new BothTablesFullyMappedFilter();
        schemaCorrespondences = filter.run(schemaCorrespondences, relationTables);

        WebTableDataSetLoader loader = new WebTableDataSetLoader();
        TableReconstructor reconstructor = new TableReconstructor(Q.map(relationTables, (t)->t.getTableId()));
        int firstTableId = Q.<Integer>max(Q.project(relationTables, (t)->t.getTableId()))+1;
        DataSet<MatchableTableDeterminant, MatchableTableColumn> candidateKeys = loader.loadCandidateKeyDataSet(relationTables, relationTableRecords);

        Collection<Table> result = reconstructor.reconstruct(firstTableId, relationTableRecords, relationTableRecords.getSchema(), candidateKeys, null, schemaCorrespondences);

        int relationId = 0;
        for(Table t : result) {
            t.setPath(String.format("%s_rel_%d", clsName, relationId++));
            t.getSchema().getFunctionalDependencies().clear();
            t.getSchema().getCandidateKeys().clear();
        }

        return result;
    }

    protected Table mergeAndDeduplicateEntityTables(Collection<Table> entityTables) throws Exception {
        Table result = null;
        TableColumn pkCol = null;
        
        // merge all tables
        for(Table t : entityTables) {
            if(result == null) {
                // make a copy of the first table
                result = t.project(t.getColumns());
                pkCol = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"pk".equals(c.getHeader())));
            } else {
                result.append(t);
            }
        }

        // deduplicate
        result.deduplicate(Q.toSet(pkCol), ConflictHandling.CreateSet, true);

        return result;
    }

    protected Map<String, String> updateEntityTablePrimaryKeys(Collection<Table> entityTables, Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {

        Set<Collection<MatchableTableRow>> clusters = Correspondence.getConnectedComponents(instanceCorrespondences.get());
        Map<String, String> pkMapping = new HashMap<>();

        // go through the clusters and create new PK values
        int clusterId = 0;
        for(Collection<MatchableTableRow> rowCluster : clusters) {

            String newPK = String.format("cluster_%d", clusterId++);

            for(MatchableTableRow r : rowCluster) {
                MatchableTableColumn pkCol = Q.firstOrDefault(Q.where(Q.toList(r.getSchema()), (c)->"pk".equals(c.getHeader())));
                String oldPK = r.get(pkCol.getColumnIndex()).toString();
                pkMapping.put(oldPK, newPK);
                r.set(pkCol.getColumnIndex(), newPK);
            }

        }

        // go through the tables and update the PK values
        for(Table t : entityTables) {
            TableColumn pkCol = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"pk".equals(c.getHeader())));
            for(TableRow r : t.getRows()) {
                String newPK = pkMapping.get(r.get(pkCol.getColumnIndex()));
                if(newPK!=null) {
                    r.set(pkCol.getColumnIndex(), newPK);
                }
            }
        }

        return pkMapping;
    }

    protected void updateRelationTableForeignKeys(Collection<Table> relationTables, Map<String, String> pkMapping) {
        // go through the tables and update the FK values
        for(Table t : relationTables) {
            TableColumn fkCol = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"fk".equals(c.getHeader())));
            for(TableRow r : t.getRows()) {
                String newPK = pkMapping.get(r.get(fkCol.getColumnIndex()));
                if(newPK!=null) {
                    r.set(fkCol.getColumnIndex(), newPK);
                }
            }
        }
    }

    protected void joinEntityTablesToRelationTables(WebTables web) throws Exception {
		for(Table t : web.getTables().values()) {
			if(t.getPath().contains("_rel_")) {
				String entityTableName = t.getPath().replaceAll("_rel_\\d+", "");
				Integer entityTableId = web.getTableIndices().get(entityTableName);
				if(entityTableId==null) {
					System.out.println(String.format("Could not find entity table for '%s'", t.getPath()));
				} else {
					Table entityTable = web.getTables().get(entityTableId);
					
					TableColumn pk = Q.firstOrDefault(Q.where(entityTable.getColumns(), (c)->"PK".equalsIgnoreCase(c.getHeader())));
					TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equalsIgnoreCase(c.getHeader())));
					
					Table joined = t.join(entityTable, Q.toList(new Pair<>(fk, pk)), Q.union(Q.without(entityTable.getColumns(), Q.toList(pk)), Q.without(t.getColumns(), Q.toList(fk))));
					joined.setTableId(t.getTableId());
					
					System.out.println(String.format("joining relation table #%d %s {%s} with entity table {%s} to {%s} / %d rows", 
							t.getTableId(),
							t.getPath(),
							StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
							StringUtils.join(Q.project(entityTable.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
							StringUtils.join(Q.project(joined.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
							t.getRows().size()));
					
					System.out.println("*** Schema mapping of entity table ***");
					for(TableColumn c : entityTable.getColumns()) {
						Pair<String, Double> columnMapping = entityTable.getMapping().getMappedProperty(c.getColumnIndex());
						if(columnMapping!=null) {
							System.out.println(String.format("Schema mapping (gs): %s -> %s", c, columnMapping.getFirst()));
						}
					}
					System.out.println("*** Schema mapping of relation table ***");
					for(TableColumn c : t.getColumns()) {
						Pair<String, Double> columnMapping = t.getMapping().getMappedProperty(c.getColumnIndex());
						if(columnMapping!=null) {
							System.out.println(String.format("Schema mapping (gs): %s -> %s", c, columnMapping.getFirst()));
						}
					}
					System.out.println("*** Schema mapping of joined table ***");
					for(TableColumn c : joined.getColumns()) {
						Pair<String, Double> columnMapping = joined.getMapping().getMappedProperty(c.getColumnIndex());
						if(columnMapping!=null) {
							System.out.println(String.format("Schema mapping (gs): %s -> %s", c, columnMapping.getFirst()));
						}
					}
					
					web.getTables().put(t.getTableId(), joined);
					
					String path = String.format("%s_%s", entityTable.getPath(), t.getPath());
					
					if(joinLocation!=null) {
						JsonTableWriter w = new JsonTableWriter();
						w.setWriteMapping(true);
						w.write(joined, new File(new File(joinLocation), path));
					}
					
					joined.setPath(path);
				}
			}
		}
	}

}