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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.StitchedModel;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FDScorer;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.StitchedFunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.preprocessing.TableDisambiguationExtractor;
import de.uni_mannheim.informatik.dws.winter.webtables.preprocessing.TableNumberingExtractor;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class GenerateFDStatistics extends Executable {

    @Parameter(names = "-input")    // the union tables
    private String inputLocation;

    @Parameter(names = "-web")      // the stitched tables (=output of SNoW)
    private String webLocation;

    @Parameter(names = "-log")
    private String logLocation;

    public static void main(String[] args) throws IOException {
        GenerateFDStatistics app = new GenerateFDStatistics();

        if(app.parseCommandLine(GenerateFDStatistics.class, args)) {
            app.run();
        }
    }

    public void run() throws IOException {

        // load web tables
        File webFile = new File(webLocation);
        WebTables web = WebTables.loadWebTables(webFile, true, true, false, false);

        BufferedWriter w = new BufferedWriter(new FileWriter(new File(logLocation), true));
        BufferedWriter wT = new BufferedWriter(new FileWriter(new File(logLocation + "_tuple_statistics.tsv"), true));
        BufferedWriter wK = new BufferedWriter(new FileWriter(new File(logLocation + "_key_statistics.tsv"), true));
        BufferedWriter wD = new BufferedWriter(new FileWriter(new File(logLocation + "_dependant_statistics.tsv"), true));

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

                System.out.println(t.getPath());
                System.out.println(scorer.formatScores());

                Set<TableColumn> maxKey = null;

                int rank = 1;
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

                    // create data type statistics
                    int numInt = 0;
                    int numString = 0;
                    int numDate = 0;
                    int numBool = 0;

                    for(TableColumn c : scoredFD.getFirst().getFirst()) {
                        if(!"FK".equals(c.getHeader())) {
                            switch(c.getDataType()) {
                                case numeric:
                                    numInt++;
                                    break;
                                case date:
                                    numDate++;
                                    break;
                                case bool:
                                    numBool++;
                                    break;
                                default:
                                    numString++;
                            }
                        }
                    }

                // for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(t.getSchema().getFunctionalDependencies())) {
                    w.write(String.format("%s\n", StringUtils.join(new String[] {
                        webFile.getParentFile().getName(),
                        t.getPath(),
                        className,
                        Integer.toString(t.getSize()),
                        Integer.toString(rank),
                        Double.toString(score),
                        Boolean.toString(isKey),
                        Boolean.toString(isKey2),
                        Integer.toString(fd.getFirst().size()),
                        Integer.toString(Q.where(fd.getFirst(), new ContextColumns.IsContextColumnPredicate()).size()),
                        Integer.toString(fd.getSecond().size()),
                        Integer.toString(closure.size()),
                        StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
                        StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
                        Integer.toString(numInt),
                        Integer.toString(numDate),
                        Integer.toString(numBool),
                        Integer.toString(numString)
                    }, "\t")));

                    rank++;
                }

                if(maxKey!=null) {
                    int numTuples = 0;
                    Collection<TableColumn> nonKeyColumns = Q.without(t.getColumns(), maxKey);
                    nonKeyColumns = Q.where(nonKeyColumns, new ContextColumns.IsNoContextColumnPredicate());
                    if(nonKeyColumns.size()>0) {
                        for(TableRow r : t.getRows()) {
                            for(TableColumn c : nonKeyColumns) {
                                if(r.get(c.getColumnIndex())!=null) {
                                    numTuples++;
                                }
                            }
                        }
                    } else {
                        numTuples = t.getSize();
                    }

                    // create data type statistics
                    int numInt = 0;
                    int numString = 0;
                    int numDate = 0;
                    int numBool = 0;

                    for(TableColumn c : maxKey) {
                        if(!"FK".equals(c.getHeader())) {
                            switch(c.getDataType()) {
                                case numeric:
                                    numInt++;
                                    break;
                                case date:
                                    numDate++;
                                    break;
                                case bool:
                                    numBool++;
                                    break;
                                default:
                                    numString++;
                            }
                        }
                    }

                    wT.write(String.format("%s\n", StringUtils.join(new String[] {
                        webFile.getParentFile().getName(),
                        t.getPath(),
                        className,
                        Integer.toString(t.getSize()),
                        Integer.toString(maxKey.size()),
                        Integer.toString(numTuples),
                        StringUtils.join(Q.project(maxKey, (c)->c.getHeader()), ","),
                        StringUtils.join(Q.project(nonKeyColumns, (c)->c.getHeader()), ","),
                        Integer.toString(numInt),
                        Integer.toString(numDate),
                        Integer.toString(numBool),
                        Integer.toString(numString),
                        Integer.toString(nonKeyColumns.size()),
                        Boolean.toString(Q.any(nonKeyColumns, new ContextColumns.IsContextColumnPredicate()))
                    }, "\t")));

                    int prov = t.getProvenance().size();
                    Collection<Table> provenance = stitchedProvenance.get(t);
                    if(provenance!=null) {
                        for(Table p : provenance) {
                            prov += p.getProvenance().size();
                        }
                    }
                    for(TableColumn c : maxKey) {
                        if(!"FK".equals(c.getHeader())) {

                            int colProv = 0;
                            if(provenance!=null) {
                                for(Table p : provenance) {
                                    for(String cp : c.getProvenance()) {
                                        TableColumn col = p.getSchema().getRecord(cp);
                                        if(col!=null) {
                                            colProv+=col.getProvenance().size();
                                        }
                                    }
                                }
                            }

                            wK.write(String.format("%s\n", StringUtils.join(new String[] {
                                webFile.getParentFile().getName(),
                                t.getPath(),
                                className,
                                Integer.toString(t.getSize()),
                                c.getHeader(),
                                c.getDataType().toString(),
                                Boolean.toString(ContextColumns.isContextColumn(c)),
                                //Integer.toString(c.getProvenance().size())
                                Integer.toString(prov),
                                Integer.toString(colProv)
                            }, "\t")));
                        }
                    }
                    for(TableColumn c : Q.without(t.getColumns(), maxKey)) {
                        int colProv = 0;
                        if(provenance!=null) {
                            for(Table p : provenance) {
                                for(String cp : c.getProvenance()) {
                                    TableColumn col = p.getSchema().getRecord(cp);
                                    if(col!=null) {
                                        colProv+=col.getProvenance().size();
                                    }
                                }
                            }
                        }

                        wD.write(String.format("%s\n", StringUtils.join(new String[] {
                            webFile.getParentFile().getName(),
                            t.getPath(),
                            className,
                            Integer.toString(t.getSize()),
                            c.getHeader(),
                            c.getDataType().toString(),
                            Boolean.toString(ContextColumns.isContextColumn(c)),
                            //Integer.toString(c.getProvenance().size())
                            Integer.toString(prov),
                            Integer.toString(colProv)
                        }, "\t")));
                    }
                }
            }
        }

        w.close();
        wT.close();
        wK.close();
        wD.close();
    }

    // private Collection<EntityTable> loadGS(File gsLocation) {
    //     File entityFile = entityStructureLocation==null ? null : new File(entityStructureLocation);
	// 	File candidateKeyFile = candidateKeysLocation==null ? null : new File(candidateKeysLocation);
	// 	File relationFile = relationsLocation==null ? null : new File(relationsLocation);
	// 	File stitchedRelationFile = stitchedRelationsLocation==null ? null : new File(stitchedRelationsLocation);
	// 	File functionalDependencyFile = functionalDependencyLocation==null ? null : new File(functionalDependencyLocation);
	// 	Collection<EntityTable> entityGroups = EntityTable.loadFromDefinition(model.getAttributes(), entityFile, candidateKeyFile, relationFile, stitchedRelationFile, functionalDependencyFile, false);
    // }

}