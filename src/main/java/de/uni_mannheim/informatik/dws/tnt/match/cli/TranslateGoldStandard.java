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
import de.uni_mannheim.informatik.dws.winter.webtables.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import com.beust.jcommander.Parameter;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.*;

/**
 * 
 * Translates an existing gold standard from one version of union tables to a different version of union tables for the same set of original tables
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TranslateGoldStandard extends Executable {

    @Parameter(names = "-oldData", required=true)
    private String oldWebLocation;

    @Parameter(names = "-newData", required=true)
    private String newWebLocation;

    @Parameter(names = "-gs", required=true)
    private String goldstandardLocation;

    @Parameter(names = "-result", required=true)
    private String resultLocation;

    private File gsFile;
    private File resultFile;

    public static void main(String[] args) throws IOException {
        TranslateGoldStandard app = new TranslateGoldStandard();

        if(app.parseCommandLine(TranslateGoldStandard.class, args)) {
            app.run();
        }
    }

    private WebTables oldWeb;
    private WebTables newWeb;
    private Map<String, TableColumn> originalColumnToNewUnionColumn;
    private Map<String, Table> originalTableToNewUnionTable;
    // private Map<String, Table> 

    public void run() throws IOException {
        oldWeb = WebTables.loadWebTables(new File(oldWebLocation), true, false, false, false);
        newWeb = WebTables.loadWebTables(new File(newWebLocation), true, false, false, false);
        originalColumnToNewUnionColumn = new HashMap<>();
        originalTableToNewUnionTable = new HashMap<>();

        gsFile = new File(goldstandardLocation);
        resultFile = new File(resultLocation);

        // index provenance of new union tables
        for(Table t : newWeb.getTables().values()) {
            for(TableColumn c : t.getColumns()) {
                for(String prov : c.getProvenance()) {
                    originalColumnToNewUnionColumn.put(prov, c);
                }
            }
            for(String prov : t.getProvenance()) {
                if(prov!=null) {
                    // if the table uses the old provenance format, change it
                    if(prov.contains(";")) {
                        prov = prov.split(";")[0];
                    }
                    originalTableToNewUnionTable.put(prov, t);
                }
            }
        }

        // translate table-to-table correspondences
        translateTableToTableGS();

        // translate entity structure
        translateEntityStructure();
    }

    private void translateTableToTableGS() throws IOException {
        N2NGoldStandard corGs = new N2NGoldStandard();
		corGs.loadFromTSV(new File(gsFile, "union_goldstandard.tsv"));
        
        Map<Set<String>, String> translatedClusters = new HashMap<>();
        for(Pair<Set<String>,String> p : Pair.fromMap(corGs.getCorrespondenceClusters())) {

            Set<String> translatedColumns = new HashSet<>();

            for(String column : p.getFirst()) {
                translatedColumns.addAll(translateUnionColumnId(column));
            }

            translatedClusters.put(translatedColumns, p.getSecond());
        }

        corGs = new N2NGoldStandard();
        corGs.getCorrespondenceClusters().putAll(translatedClusters);
        corGs.writeToTSV(new File(resultFile, "union_goldstandard.tsv"));
    }

    private void translateEntityStructure() throws IOException {
        BufferedReader r = new BufferedReader(new FileReader(new File(gsFile, "entity_structure.tsv")));
        BufferedWriter w = new BufferedWriter(new FileWriter(new File(resultFile, "entity_structure.tsv")));
        
        String line = null;

        while((line = r.readLine())!=null) {
            String[] values = line.split("\t");
            if(values.length==3) {
                String[] columns = values[2].split(",");
                Set<String> newColumns = new HashSet<>();
                for(String column : columns) {
                    newColumns.addAll(translateUnionColumnId(column));
                }
                w.write(String.format("%s\t%s\t%s\n", values[0], values[1], StringUtils.join(newColumns, ",")));
            }
        }

        r.close();
        w.close();
    }

    private Set<String> translateUnionColumnId(String oldId) {
        Set<String> newIds = new HashSet<>();

        // an 'old' union column was stitched from a set of original columns C
        // these columns C are now stitched into one or more 'new' union columns
        // to translate, get all original columns and return the new union columns which have their ids as provenance
        MatchableTableColumn oldMC = oldWeb.getSchema().getRecord(oldId);
        if(oldMC==null) {
            System.out.println(String.format("Cannot find column %s", oldId));
            // if a column cannot be found, it might be a reference to a column that is generated during runtime (such as disambiguation columns)
            String oldUnionName = oldId.split("~")[0];
            Integer oldUnionId = oldWeb.getTableIndices().get(oldUnionName);
            if(oldUnionId!=null) {
                Table oldUnion = oldWeb.getTables().get(oldUnionId);
                int oldUnionExtraColumns = Integer.parseInt(oldId.split("~Col")[1]) - oldUnion.getColumns().size();
                System.out.println(String.format("\tadding extra column %d to new union table(s)",oldUnionExtraColumns+1));
                for(String prov : oldUnion.getProvenance()) {
                    if(prov.contains(";")) {
                        prov = prov.split(";")[0];
                    }

                    // in this case, get the new union tables which contain the tables that were stitched into the old union table
                    Table newUnion = originalTableToNewUnionTable.get(prov);

                    // and add an extra column to these new union tables to perform the column translation
                    newIds.add(String.format("%s~Col%d", newUnion.getPath(), newUnion.getColumns().size() + oldUnionExtraColumns));
                }
            }
        } else {
            Table oldTable = oldWeb.getTables().get(oldMC.getTableId());
            TableColumn oldC = oldTable.getSchema().get(oldMC.getColumnIndex());

            Set<String> originalColumns = new HashSet<>(oldC.getProvenance());

            

            for(String prov : originalColumns) {
                // check if the input table uses the old provenance format: $table_name;$column_index;$header
                // if so, translate to the new provenance format: $table_name~Col$column_index
                if(prov.contains(";")) {
                    String[] parts = prov.split(";");
                    prov = String.format("%s~Col%s", parts[0], parts[1]);
                }

                if(originalColumnToNewUnionColumn.containsKey(prov)) {
                    newIds.add(originalColumnToNewUnionColumn.get(prov).getIdentifier());
                } else {
                    System.out.println(String.format("missing column: %s", prov));
                }
            }
        }

        return newIds;

    }

}