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
import de.uni_mannheim.informatik.dws.winter.webtables.FunctionalDependencyDiscovery;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.commons.lang.StringUtils;
import com.beust.jcommander.Parameter;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.ApproximateFunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CalculateApproximateFunctionalDependencies extends Executable {

    @Parameter(names = "-web", required=true)
    private String webLocation;

    @Parameter(names = "-csv", required=true)
    private String csvLocation;

    @Parameter(names = "-minApproximationRate")
    private double minApproximationRate = 1.0;

    @Parameter(names = "-result")
    private String resultLocation;

    @Parameter(names = "-verbose")
    private boolean verbose;

    @Parameter(names = "-fdLog")
    private String fdLogLocation;

    @Parameter(names = "-silent")
    private boolean silent;

    @Parameter(names = "-cover")
    private boolean calculateCover;

    @Parameter(names = "-minimal")
    private boolean minimalCover;

    @Parameter(names = "-keys")
    private boolean listKeys;

    @Parameter(names = "-approximate")
    private boolean useApproximate;

    @Parameter(names = "-tane_original")
    private boolean useOriginalTane;

    @Parameter(names = "-tane_adjusted")
    private boolean useAdjustedTane;

    @Parameter(names = "-tane_location")
    private String taneLocation;

    public static void main(String[] args) throws Exception {
        CalculateApproximateFunctionalDependencies app = new CalculateApproximateFunctionalDependencies();

        if(app.parseCommandLine(CalculateApproximateFunctionalDependencies.class, args)) {
            app.run();
        }
    }

    public void run() throws Exception {
        WebTables web = WebTables.loadWebTables(new File(webLocation), true, false, false, false);

        File csv = new File(csvLocation);
        csv.mkdirs();

        CSVTableWriter w = new CSVTableWriter();

        for(Table t : web.getTables().values()) {
            w.write(t, new File(csv, t.getPath()));
        }

        if(minApproximationRate<1.0) {
            useApproximate = true;
        }

        if(useOriginalTane) {
            FunctionalDependencyDiscovery.taneRoot = new File(taneLocation);
            FunctionalDependencyDiscovery.calculateApproximateFunctionalDependencies(web.getTables().values(), csv, 1.0 - minApproximationRate);
        // } else if(useAdjustedTane) {
        //     FunctionalDependencyDiscovery.calculateApproximateFunctionalDependenciesAdjustedTane(web.getTables().values(), csv, 1.0 - minApproximationRate);
        // } else if(useApproximate) {
        //     FunctionalDependencyDiscovery.calculateApproximateFunctionalDependenciesPYRO(web.getTables().values(), csv, 1.0 - minApproximationRate);
        } else {
            FunctionalDependencyUtils.calculcateFunctionalDependencies(web.getTables().values(), csv);

            // if(minApproximationRate<1.0) {
            //     ApproximateFunctionalDependencyUtils.setApproximateFDs(web.getTables().values(), minApproximationRate, verbose);
            // }
        }

        if(minimalCover) {
            for(Table t : web.getTables().values()) {
                t.getSchema().setFunctionalDependencies(FunctionalDependencyUtils.minimise(t.getSchema().getFunctionalDependencies()));
            }
        }

        if(calculateCover) {
            for(Table t : web.getTables().values()) {
                t.getSchema().setFunctionalDependencies(FunctionalDependencyUtils.canonicalCover(t.getSchema().getFunctionalDependencies()));
            }
        }

        BufferedWriter log = null;
        if(fdLogLocation!=null) {
            File fdLog = new File(fdLogLocation);
            log = new BufferedWriter(new FileWriter(fdLog));
        }

        for(Table t : web.getTables().values()) {
            Collection<Pair<Set<TableColumn>,Set<TableColumn>>> fds = Pair.fromMap(t.getSchema().getFunctionalDependencies());
            Collection<String> lines = new LinkedList<>();
            for(Pair<Set<TableColumn>,Set<TableColumn>> fd : fds) {
                lines.add(String.format("{%s}->{%s}\n",
                    StringUtils.join(Q.<String>sort(Q.<String, TableColumn>project(fd.getFirst(), (c)->c.getHeader())), ","),
                    StringUtils.join(Q.<String>sort(Q.<String, TableColumn>project(fd.getSecond(), (c)->c.getHeader())), ",")
                ));
            }
            for(String line : Q.sort(lines)) {
                if(log!=null) {
                    log.write(line);
                }
                if(!silent) {
                    System.out.print(line);
                }
            }
            if(listKeys) {
                System.out.println("*** Candidate Keys");
                for(Set<TableColumn> key : FunctionalDependencyUtils.listCandidateKeys(t)) {
                    System.out.println(String.format("{%s}",StringUtils.join(Q.project(key, (c)->c.getHeader()), ",")));
                }
            }
        }

        if(log!=null) {
            log.close();
        }

        if(resultLocation!=null) {
            JsonTableWriter jsonW = new JsonTableWriter();
            File result = new File(resultLocation);
            for(Table t : web.getTables().values()) {
                jsonW.write(t, new File(result, t.getPath()));
            }
        }

        System.exit(0);
    }

    public void calculateApproximateFunctionalDependencies_OriginalTane(Collection<Table> tables, double errorThreshold) throws Exception {
        CSVTableWriter csvWriter = new CSVTableWriter();
        File taneRoot = new File(taneLocation);
        File taneDataLocation = new File(taneLocation, "original");
        File taneDescriptionLocation = new File(taneLocation, "descriptions");
        // File taneExec = new File(taneLocation, "bin/taneg3");
        // File tanePrepare = new File(taneLocation, "bin/select.perl");
        for(Table t : tables) {
            System.out.println(String.format("[calculateApproximateFunctionalDependencies] calculating functional dependencies for table #%d %s {%s}", 
                    t.getTableId(),
                    t.getPath(),
                    StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));

            // write file
            File tableAsCsv = csvWriter.write(t, new File(taneDataLocation, t.getPath()));

            // write description
            String descriptionFileName = t.getPath() + ".dsc";
            File description = new File(taneDescriptionLocation, descriptionFileName);
            BufferedWriter w = new BufferedWriter(new FileWriter(description));
            w.write("Umask = 007\n");
            w.write(String.format("DataIn = ../original/%s\n", t.getPath()));
            w.write("RemoveDuplicates = OFF\nAttributesOut = $BASENAME.atr\nStandardOut = ../data/$BASENAME.dat\nSavnikFlachOut = ../data/$BASENAME.rel\nNOOFDUPLICATES=1\n");
            w.close();

            // prepare dataset
            String cmd = "../bin/select.perl ../descriptions/" + descriptionFileName;
            System.out.println(String.format("%s$ %s", taneDataLocation.getAbsolutePath(), cmd));
            Process p = Runtime.getRuntime().exec(cmd, null, taneDataLocation);
            String line = null;
            BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while((line = r.readLine()) != null) {
                System.out.println(line);
            }
            r.close();
            r = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            while((line = r.readLine()) != null) {
                System.out.println(line);
            }
            r.close();
            
            // run tane
            String nameWithoutExtension = t.getPath().replaceAll("\\..{3,4}$", "");
            File dataLocation = new File(taneLocation, "data/" + nameWithoutExtension + ".dat");
            cmd = String.format("./bin/taneg3 11 %d %d %s %f", t.getRows().size(), t.getColumns().size(), dataLocation.getAbsolutePath(), errorThreshold);
            System.out.println(String.format("%s$ %s", taneRoot.getAbsolutePath(), cmd));
            p = Runtime.getRuntime().exec(cmd, null, taneRoot);

            Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies = new HashMap<>();
            r = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while((line = r.readLine()) != null) {
                System.out.println(line);
                // FDs lines always start with a number
                String[] values = line.split("\\s");
                boolean isFdLine = false;
                try {
                    Integer.parseInt(values[0]);
                    isFdLine = true;
                } catch(NumberFormatException ex) { isFdLine = false; }

                if(isFdLine) {
                    Set<TableColumn> det = new HashSet<>();
                    TableColumn dep = null;

                    boolean depStart = false;
                    for(int i = 0; i < values.length; i++) {
                        if(depStart) {
                            int idx = Integer.parseInt(values[i]) - 1;
                            dep = t.getSchema().get(idx);
                            break;
                        } else {
                            if("->".equals(values[i])) {
                                depStart = true;
                            } else {
                                int idx = Integer.parseInt(values[i]) - 1;
                                det.add(t.getSchema().get(idx));
                            }
                        }
                    }

                    Set<TableColumn> mergedDep = null;
                    // check if we already have a dependency with the same determinant
                    if(functionalDependencies.containsKey(det)) {
                        // if so, we add the dependent to the existing dependency
                        mergedDep = functionalDependencies.get(det);
                    } 
                    if(mergedDep==null) {
                        // otherwise, we create a new dependency
                        mergedDep = new HashSet<>();
                        functionalDependencies.put(det, mergedDep);
                    }
                    mergedDep.add(dep);
                }
            }
            r.close();
            r = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            while((line = r.readLine()) != null) {
                System.out.println(line);
            }
            r.close();
            
            t.getSchema().setFunctionalDependencies(functionalDependencies);
        }
	}

}