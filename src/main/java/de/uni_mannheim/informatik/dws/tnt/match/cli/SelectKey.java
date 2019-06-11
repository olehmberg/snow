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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.StitchedModel;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FDScorer;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.StitchedFunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.preprocessing.TableDisambiguationExtractor;
import de.uni_mannheim.informatik.dws.winter.webtables.preprocessing.TableNumberingExtractor;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SelectKey extends Executable {

    @Parameter(names = "-input") // the union tables
    private String inputLocation;

    @Parameter(names = "-web") // the stitched tables (=output of SNoW)
    private String webLocation;

    @Parameter(names = "-out")
    private String outLocation;

    public static void main(String[] args) throws IOException {
        SelectKey app = new SelectKey();

        if(app.parseCommandLine(SelectKey.class, args)) {
            app.run();
        }
    }

    public void run() throws IOException {

        File outFile = new File(outLocation);
        outFile.mkdirs();
      
        // load web tables
        File webFile = new File(webLocation);
        WebTables web = WebTables.loadWebTables(webFile, true, true, false, false);

        // create a stitching model
        // load the input tables and create the stitching provenance
        File inputFile = new File(inputLocation);
        WebTables input = WebTables.loadWebTables(inputFile, true, false, false, true);
        TableDisambiguationExtractor dis = new TableDisambiguationExtractor();
		Map<Integer, Map<Integer, TableColumn>> tableToColumnToDisambiguation = dis.extractDisambiguations(input.getTables().values());
		TableNumberingExtractor num = new TableNumberingExtractor();
		Map<Integer, Map<Integer, TableColumn>> tableToColumnToNumbering = num.extractNumbering(input.getTables().values());
        System.out.println("Input tables");
        for(Table t : input.getTables().values()) {
            System.out.println(String.format("%s: {%s}",
                t.getPath(),
                StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
            ));
        }

        StitchedModel model = new StitchedModel();
        // the provenance is a map frmo the stitched table to all tables that were stitched into it
        Map<Table, Collection<Table>> stitchedProvenance = new HashMap<>();
        for(Table stitched : web.getTables().values()) {
            Collection<Table> provenance = new LinkedList<>();
            System.out.println(String.format("Provenance for table %s: %s", 
                stitched.getPath(),
                StringUtils.join(stitched.getProvenance(), ",")
            ));
            for(String prov : stitched.getProvenance()) {
                if(input.getTableIndices().containsKey(prov)) {
                    int id = input.getTableIndices().get(prov);
                    Table inputTable = input.getTables().get(id);
                    provenance.add(inputTable);
                }
            }
            stitchedProvenance.put(stitched, provenance);
            System.out.println(String.format("Table %s has %d tables in its provenance",stitched.getPath(), provenance.size()));
        }
        model.setTables(web.getTables());
        model.setStitchedProvenance(stitchedProvenance);

        // as the input tables have no FK columns yet, we have to add an artificial provenance for these columns
        // - as the FK is create for every table mapped to the same class, each FK has provenance from all tables 
        for(Table stitched : stitchedProvenance.keySet()) {
            Collection<Table> provenance = stitchedProvenance.get(stitched);
            for(TableColumn c : stitched.getColumns()) {
                if("fk".equals(c.getHeader()) || c.getHeader().startsWith("entity context")) {
                    model.getStitchedColumnProvenance().put(c, new HashSet<>(provenance));
                }
            }
        }


        FDScorer scorer = new FDScorer();
        scorer.addScoringFunction(FDScorer.dataTypeScore, "data type score");
        scorer.addScoringFunction(FDScorer.lengthScore, "length score");
        // scorer.addScoringFunction(FDScorer.positionScore, "position score");
        // scorer.addScoringFunction(FDScorer.determinantProvenanceScore, "determinant provenance score");
        // scorer.addScoringFunction(FDScorer.dependantProvenanceScore, "determinant provenance score");
        scorer.addScoringFunction(FDScorer.extensionalDeterminantProvenanceScore, "extensional determinant provenance score");
        // scorer.addScoringFunction(FDScorer.extensionalDependantProvenanceScore, "extensional dependant provenance score");
        // scorer.addScoringFunction(FDScorer.duplicationScore, "duplication score");
        // scorer.addScoringFunction(FDScorer.attributeDeterminantFrequency, "attribute determinant frequency score");
        // scorer.addScoringFunction(FDScorer.attributeCombinationDeterminantFrequency, "attribute combination determinant frequency score");
        // scorer.addScoringFunction(FDScorer.attributeDependantFrequency, "attribute dependant frequency score");
        // scorer.addScoringFunction(FDScorer.disambiguation, "disambiguation score");
        scorer.addScoringFunction(FDScorer.disambiguationOfForeignKey, "disambiguation of FK score");
        // scorer.addScoringFunction(FDScorer.weakFDScore, "weak FD score");
        // scorer.addScoringFunction(FDScorer.strongFDScore, "strong FD score");
        // scorer.addScoringFunction(FDScorer.conflictRateScore, "conflict rate score");
        scorer.addScoringFunction(FDScorer.filenameScore, "filename score");
        scorer.addScoringFunction(FDScorer.disambiguationSplitScore, "disambiguation split score");

        // generate statistics for all FDs in relation tables
        for(Table t : web.getTables().values()) {
            // revert the lowercasing of special columns
            for(TableColumn c : t.getColumns()) {
                if(c.getHeader().equals("fk")) c.setHeader("FK");
                if(c.getHeader().startsWith("disambiguation of")) c.setHeader(c.getHeader().replace("disambiguation of", "Disambiguation of"));
            }

            if(t.getPath().contains("_rel_")) {
                String className = "";
                if(t.getMapping().getMappedClass()!=null) {
                    className = t.getMapping().getMappedClass().getFirst();
                }

                for(TableColumn c : t.getColumns()) {
                    if(model.getStitchedColumnProvenance().get(c)==null) {
                        System.out.println(String.format("%s %s has no stitched provenance! (%s)",
                            t.getPath(),
                            c.toString(),
                            StringUtils.join(c.getProvenance(), ",")
                        ));
                    }
                }

                List<Pair<Pair<Set<TableColumn>,Set<TableColumn>>,Double>> scoredFDs = scorer.scoreFunctionalDependenciesForDecomposition(Pair.fromMap(t.getSchema().getFunctionalDependencies()), t, model);

                

                Set<TableColumn> maxKey = null;
                for(Pair<Pair<Set<TableColumn>,Set<TableColumn>>,Double> scoredFD : scoredFDs) {
                    Pair<Set<TableColumn>,Set<TableColumn>> fd = scoredFD.getFirst();
                    Double score = scoredFD.getSecond();
                    boolean isKey = t.getSchema().getCandidateKeys().contains(fd.getFirst());
                    Set<TableColumn> closure = StitchedFunctionalDependencyUtils.closure(fd.getFirst(), t.getSchema().getFunctionalDependencies(), model);
                    boolean isKey2 = closure.size()==t.getColumns().size();

                    if(isKey2) {
                        if(maxKey==null) {
                            maxKey = fd.getFirst();
                        } else if(fd.getFirst().size()>maxKey.size()) {
                            maxKey = fd.getFirst();
                        }
                    }
                }

                if(maxKey!=null) {
                    t.getSchema().getCandidateKeys().clear();
                    t.getSchema().getCandidateKeys().add(maxKey);
                    JsonTableWriter w = new JsonTableWriter();
                    w.setWriteMapping(true);
                    w.write(t, new File(outFile, t.getPath()));
                }
            }
        }
    }

}