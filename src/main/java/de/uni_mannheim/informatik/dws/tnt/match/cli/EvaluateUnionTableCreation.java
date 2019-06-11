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
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.*;
import de.uni_mannheim.informatik.dws.winter.webtables.*;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EvaluateUnionTableCreation extends Executable {

    @Parameter(names = "-union", required=true)
    private String unionLocation;

    @Parameter(names = "-web", required=true)
    private String webLocation;

    @Parameter(names = "-result", required=true)
    private String resultLocation;

    @Parameter(names = "-unionMapping")
    private String unionMappingLocation;

    @Parameter(names = "-contextMapping")
    private String contextMappingLocation;

    @Parameter(names = "-sampleSize")
    private int sampleSize = 10;

    public static void main(String[] args) throws Exception {
        EvaluateUnionTableCreation app = new EvaluateUnionTableCreation();

        if(app.parseCommandLine(EvaluateUnionTableCreation.class, args)) {
            app.run();
        }
    }

    public void run() throws Exception {
        Scanner s = new Scanner(System.in);

        // load union tables
        WebTables web = WebTables.loadWebTables(new File(unionLocation), true, false, false, true);
        web.removeHorizontallyStackedTables();
        JsonTableParser p = new JsonTableParser();
        p.setConvertValues(false);
        p.setInferSchema(false);

        Map<Table, String> originalSchemas = new HashMap<>();
        for(Table t : web.getTables().values()) {
            originalSchemas.put(t, t.getSchema().format(20));
        }

        // load union mapping
        if(unionMappingLocation!=null) {
            renameColumns(unionMappingLocation, contextMappingLocation, web.getSchema(), web.getTables());
        }

        Set<String> annotatedTables = new HashSet<>();
        if(new File(resultLocation).exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(new File(resultLocation)));
            String line = null;
            while((line = reader.readLine()) != null) {
                String[] values = line.split("\t");
                annotatedTables.add(values[0] + "/" + values[1]);
            }
            reader.close();
        }

        BufferedWriter w = new BufferedWriter(new FileWriter(new File(resultLocation), true));

        // for each union table
        for(Table t : web.getTables().values()) {
            if(!annotatedTables.contains(unionLocation + "/" + t.getPath())) {
                Set<String> prov = t.getProvenance();

                // sample original tables
                Set<String> selected = randomSample(prov, sampleSize);

                for(String tableName : selected) {
                    File path = new File(webLocation, tableName);

                    System.out.println();
                    // show schema
                    System.out.println(String.format("*** Union Table '%s' stitched from %,d original web tables", t.getPath(), prov.size()));
                    System.out.println(originalSchemas.get(t));
                    System.out.println(t.getSchema().format(20));

                    if(!path.exists()) {
                        System.out.println(String.format("Could not find web table '%s'", path.getAbsolutePath()));
                    } else {
                        // load original table
                        Table wt = p.parseTable(path);

                        // show original table
                        System.out.println(String.format("original web table '%s'", path.getName()));
                        // show context!
                        System.out.println(String.format("URL: %s", wt.getContext().getUrl()));
                        System.out.println(String.format("Page Title: %s", wt.getContext().getPageTitle()));
                        System.out.println(String.format("Heading: %s", wt.getContext().getTableTitle()));
                        System.out.println(wt.getSchema().format(20));
                        // show the first 5 rows
                        int i= 0;
                        for(TableRow r : wt.getRows()) {
                            System.out.println(r.format(20));
                            if(i++==5) break;
                        }

                        // ask for annotation
                        System.out.println("Correct [+/-/q]? ");
                        String annotation = null;
                        while(annotation==null) {
                            annotation = s.next();
                            switch(annotation)
                            {
                                case "+":
                                    annotation = "true";
                                    break;
                                case "-":
                                    annotation = "false";
                                    break;
                                case "q":
                                    w.close();
                                    return;
                                default:
                                    annotation = null;
                                    break;
                            }
                        }

                        // write annotation
                        w.write(StringUtils.join(new String[] {
                            unionLocation,
                            t.getPath(),
                            wt.getPath(),
                            annotation
                        }, "\t"));
                        w.write("\n");
                        w.flush();
                    }
                }
            }
        }

        w.close();
    }

    protected Set<String> randomSample(Set<String> values, int sampleSize) {
        Random r = new Random();
        String[] a = Q.toArrayFromCollection(values, String.class);
        Set<String> sample = new HashSet<>();

        while(sample.size()<Math.min(sampleSize, values.size())) {
            sample.add(a[r.nextInt(a.length)]);
        }

        return sample;
    }

    private void renameColumns(
        String correspondencesLocation, 
        String contextCorrespondencesLocation, 
		DataSet<MatchableTableColumn,MatchableTableColumn> attributes, 
		Map<Integer, Table> tables
	) throws Exception {
		System.err.println("Loading Union Table Correspondences");
		N2NGoldStandard corGs = new N2NGoldStandard();
		corGs.loadFromTSV(new File(correspondencesLocation));

        // rename the columns according to the gs (for debugging)
        for(Set<String> cluster : corGs.getCorrespondenceClusters().keySet()) {
            String name = corGs.getCorrespondenceClusters().get(cluster);
            for(String id : cluster) {
                MatchableTableColumn mc = attributes.getRecord(id);
                if(mc!=null) {
                    TableColumn c = tables.get(mc.getTableId()).getSchema().get(mc.getColumnIndex());
                    //String newHeader = String.format("%s {%s}", name, c.getHeader());
                    String newHeader = name;
                    c.setHeader(newHeader);

                }
            }
        }

        if(contextCorrespondencesLocation!=null) {
            corGs.loadFromTSV(new File(contextCorrespondencesLocation));
            // rename the columns according to the gs (for debugging)
            for(Set<String> cluster : corGs.getCorrespondenceClusters().keySet()) {
                String name = corGs.getCorrespondenceClusters().get(cluster);
                for(String id : cluster) {
                    MatchableTableColumn mc = attributes.getRecord(id);
                    if(mc!=null) {
                        TableColumn c = tables.get(mc.getTableId()).getSchema().get(mc.getColumnIndex());
                        //String newHeader = String.format("%s {%s}", name, c.getHeader());
                        String newHeader = name;
                        c.setHeader(newHeader);

                    }
                }
            }
        }
    }

}