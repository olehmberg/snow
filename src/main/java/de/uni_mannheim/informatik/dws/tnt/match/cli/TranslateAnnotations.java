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
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence.RecordId;
import de.uni_mannheim.informatik.dws.winter.processing.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TranslateAnnotations extends Executable {

    @Parameter(names = "-t1", required=true)
    private String table1Name;

    @Parameter(names = "-t2", required=true)
    private String table2Name;

    @Parameter(names = "-on", required=true)
    private String joinCondition;

    @Parameter(names = "-gs")
    private String goldstandardLocation;

    @Parameter(names = "-out", required=true)
    private String result;

    public static void main(String[] args) throws Exception {
        TranslateAnnotations app = new TranslateAnnotations();

        if(app.parseCommandLine(TranslateAnnotations.class, args)) {
            app.run();
        }
    }

    public void run() throws Exception {

        // join two tables, then map their row ids and translate the provided correspondences
        File f1 = new File(table1Name);
        File f2 = new File(table2Name);

        Table t1 = Q.firstOrDefault(WebTables.loadWebTables(f1, true, false, false, false).getTables().values());
        Table t2 = Q.firstOrDefault(WebTables.loadWebTables(f2, true, false, false, false).getTables().values());

        // add the row identifier as an attribute to both tables
        TableColumn rowId1 = new TableColumn(t1.getColumns().size(), t1);
        rowId1.setHeader("rowid1");
        t1.insertColumn(rowId1.getColumnIndex(), rowId1);
        for(TableRow r : t1.getRows()) {
            r.set(rowId1.getColumnIndex(), r.getIdentifier());
        }
        TableColumn rowId2 = new TableColumn(t2.getColumns().size(), t2);
        rowId2.setHeader("rowid2");
        t2.insertColumn(rowId2.getColumnIndex(), rowId2);
        for(TableRow r : t2.getRows()) {
            r.set(rowId2.getColumnIndex(), r.getIdentifier());
        }

        Collection<Pair<TableColumn,TableColumn>> joinOn = new LinkedList<>();
        Collection<TableColumn> projectedColumns = new LinkedList<>();

        for(String att : joinCondition.split(",")) {
            TableColumn c1=null, c2=null;

            for(TableColumn c : t1.getColumns()) {
                if(c.getHeader().equals(att)) {
                    c1 = c;
                    break;
                }
            }
            for(TableColumn c : t2.getColumns()) {
                if(c.getHeader().equals(att)) {
                    c2 = c;
                    break;
                }
            }

            if(c1!=null && c2!=null) {
                joinOn.add(new Pair<>(c1,c2));
            }
        }

        projectedColumns.add(rowId1);
        projectedColumns.add(rowId2);

        // rename the tables:
        // - if both tables have the same name, their column IDs will overlap, which changes the result of the join!
        t1.setTableId(1);
        t1.setPath("1");
        t2.setTableId(2);
        t2.setPath("2");

        System.out.println(String.format("Joining table [%d] %s with [%d] %s on %s",
            t1.getTableId(),
            t1.getPath(),
            t2.getTableId(),
            t2.getPath(),
            StringUtils.join(Q.project(joinOn, (p)->String.format("%s==%s", p.getFirst().toString(), p.getSecond().toString())), " AND ")
        ));

        System.out.println(String.format("Projecting columns %s",
            StringUtils.join(Q.project(projectedColumns, (c)->c.toString()), ",")
        ));

        Table joined = t1.join(t2, joinOn, projectedColumns);

        System.out.println(String.format("Result is {%s} with %d rows",
            StringUtils.join(Q.project(joined.getColumns(), (c)->c.getHeader()), ","),
            joined.getSize()
        ));

        Map<String, String> mappedRowIds = new HashMap<>();
        TableColumn id1 = Q.firstOrDefault(Q.where(joined.getColumns(), (c)->"rowid1".equals(c.getHeader())));
        TableColumn id2 = Q.firstOrDefault(Q.where(joined.getColumns(), (c)->"rowid2".equals(c.getHeader())));
        for(TableRow r : joined.getRows()) {
            String r1 = (String)r.get(id1.getColumnIndex());
            String r2 = (String)r.get(id2.getColumnIndex());

            mappedRowIds.put(r1,r2);
        }

        MatchingGoldStandard gs = new MatchingGoldStandard();
        gs.loadFromTSVFile(new File(goldstandardLocation));
        MatchingGoldStandard translatedGs = new MatchingGoldStandard();
        
        
        for(Pair<String,String> p : gs.getPositiveExamples()) {
            if(mappedRowIds.containsKey(p.getFirst())) {
                translatedGs.addPositiveExample(new Pair<>(mappedRowIds.get(p.getFirst()), p.getSecond()));
            }
        }
        for(Pair<String,String> p : gs.getNegativeExamples()) {
            if(mappedRowIds.containsKey(p.getFirst())) {
                translatedGs.addNegativeExample(new Pair<>(mappedRowIds.get(p.getFirst()), p.getSecond()));
            }
        }

        System.out.println("Translated gold standard:");
        translatedGs.printGSReport();
        translatedGs.writeToTSVFile(new File(result));
    }

}