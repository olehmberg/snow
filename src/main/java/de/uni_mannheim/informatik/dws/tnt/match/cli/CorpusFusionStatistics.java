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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
public class CorpusFusionStatistics extends Executable {

    @Parameter(names = "-web", required=true)
    private String webLocation;

    @Parameter(names = "-results", required=true)
    private String resultLocation;

    @Parameter(names = "-binary")
    private boolean binaryOnly;

    public static void main(String[] args) throws Exception {
        CorpusFusionStatistics app = new CorpusFusionStatistics();

        if(app.parseCommandLine(CorpusFusionStatistics.class, args)) {
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

            web.add(t);
        }
        File resultFile = new File(resultLocation);

        // run this process for each class individually
        // Map<String, Collection<Table>> tablesByClass = Q.group(web, (t)->t.getMapping().getMappedClass().getFirst());

        // if(tablesByClass.size()==0) {
        //     System.out.println("Did not find any class mapping!");
        // }

        BufferedWriter entityWriter = new BufferedWriter(new FileWriter(new File(resultFile, "entity_statistics.tsv")));
        BufferedWriter relationWriter = new BufferedWriter(new FileWriter(new File(resultFile, "relation_statistics.tsv")));

        Collection<Table> tables = web;
        // for(String clsName : tablesByClass.keySet()) {
        //     System.out.println(String.format("Fusing tables for class %s", clsName));
        //     Collection<Table> tables = tablesByClass.get(clsName);

            // get entity & relation tables
            Collection<Table> entityTables = Q.where(tables, (t)->!t.getPath().contains("_rel_"));
            Collection<Table> relationTables = Q.where(tables, (t)->t.getPath().contains("_rel_"));

            if(binaryOnly) {
                relationTables = Q.where(relationTables, (t)->t.getColumns().size()==2);
            }

            for(Table entityTable : entityTables) {    
                entityWriter.write(String.format("%s\n", StringUtils.join(new String[] {
                    // clsName,
                    entityTable.getPath(),
                    Integer.toString(entityTable.getSize()),
                    Integer.toString(entityTable.getProvenance().size())
                }, "\t")));
            }

            if(relationTables.size()>0) {
                for(Table t : relationTables) {
                    Map<TableColumn, Integer> valuesPerColumn = t.getNumberOfValuesPerColumn();
                    for(TableColumn c : valuesPerColumn.keySet()) {
                        if(!"fk".equals(c.getHeader())) {
                            relationWriter.write(String.format("%s\n", StringUtils.join(new String[] {
                                // clsName,
                                t.getPath(),
                                c.getHeader(),
                                Integer.toString(valuesPerColumn.get(c)),
                                Integer.toString(t.getProvenance().size())
                            }, "\t")));
                        }
                    }
                }
            } else {
                // System.out.println(String.format("No relation tables for class %s", clsName));
            }

        // }

        entityWriter.close();
        relationWriter.close();
    }

}