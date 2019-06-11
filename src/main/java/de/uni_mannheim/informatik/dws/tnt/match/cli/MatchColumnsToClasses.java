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
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import org.slf4j.Logger;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers.RelationTableMatcher;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence.RecordId;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.*;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;

/**
 * 
 * Matches columns to classes in the KB
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchColumnsToClasses extends Executable {

    @Parameter(names = "-web")
    private String webLocation;

    @Parameter(names = "-kb")
    private String entityDefinitionLocation;

    @Parameter(names = "-result")
    private String resultLocation;

    @Parameter(names = "-idf")
    private String idfLocation;

    @Parameter(names = "-sf")
    private boolean useSurfaceForms = false;

    @Parameter(names = "-gs")
    private String gsLocation;

    @Parameter(names = "-train")
    private String trainLocation;

    @Parameter(names = "-eval")
    private boolean evalOnly;

    @Parameter(names = "-patterns")
    private String patternLocation;

    @Parameter(names = "-candidatesOnly")
    private boolean candidatesOnly;

    @Parameter(names = "-skipSer")
    private boolean skipSerialisation;

    @Parameter(names = "-entityLinkingThreshold")
    private double entityLinkingThreshold = 0.4;

    @Parameter(names = "-minMappedEntities")
    private int minMappedEntities = 10;

    @Parameter(names = "-finalThreshold")
    private double finalThreshold = 0.0;

    private static final Logger logger = WinterLogManager.activateLogger("trace");

    public static void main(String[] args) throws Exception {
        MatchColumnsToClasses app = new MatchColumnsToClasses();

        if (app.parseCommandLine(MatchColumnsToClasses.class, args)) {

            if(app.evalOnly) {
                app.runEval();
            } else {
                app.run();
            }
        }
    }

    public void run() throws Exception {
        // load web tables
        File webFile = new File(webLocation);
        WebTables web = WebTables.loadWebTables(webFile, true, true, false, !skipSerialisation);

        RelationTableMatcher relMatcher = new RelationTableMatcher();
        relMatcher.loadKnowledgeBase(new File(entityDefinitionLocation), useSurfaceForms);

        Processable<Pair<String, Double>> idf = null;

        if (idfLocation != null) {
            File f = new File(idfLocation);

            if (f.exists()) {
                idf = relMatcher.readIDF(f);
            } else {
                idf = relMatcher.calculateIDF();
                relMatcher.writeIDF(idf, f);
            }
        }

        // Collection<Table> entityTables = Q.where(web.getTables().values(),
        // (t)->!t.getPath().contains("_rel_"));
        Collection<Table> relationTables = Q.where(web.getTables().values(), (t) -> t.getPath().contains("_rel_"));

        System.out.println(String.format("Matching %d relations", relationTables.size()));

        BufferedWriter wStatistics = new BufferedWriter(new FileWriter(new File(resultLocation + "_table_statistics.tsv")));
        BufferedWriter wColumnStatistics = new BufferedWriter(new FileWriter(new File(resultLocation + "_column_statistics.tsv")));

        LinkedList<Table> keyOnlyRelations = new LinkedList<>();
        for (Table t : relationTables) {
            Set<TableColumn> key = Q.firstOrDefault(t.getSchema().getCandidateKeys());

            System.out.println(String.format("Table %s", t.getPath()));

            String tblClass = null;
            Pair<String, Double> cls = t.getMapping().getMappedClass();
            if(cls!=null) {
                tblClass = cls.getFirst();
            } else {
                tblClass = t.getPath().split("\\_")[0];
            }

            int originalKeySize = key.size();
            int originalColumnCount = t.getColumns().size();
            wStatistics.write(String.format("%s\n", StringUtils.join(new String[] {
                t.getPath(),
                Integer.toString(originalKeySize),
                Integer.toString(originalColumnCount),
                Integer.toString(t.getSize()),
                tblClass
            }, "\t")));
            

            Iterator<TableColumn> it = key.iterator();
            while (it.hasNext()) {
                TableColumn c = it.next();
                if ("fk".equals(c.getHeader()) || c.getDataType() != DataType.string) {
                    it.remove();
                    System.out.println(String.format("\tRemoving %s: '%s'", c.getIdentifier(), c.getHeader()));
                } else {
                    System.out.println(String.format("\tKeeping %s: '%s'", c.getIdentifier(), c.getHeader()));
                }
            }

            Table projection = t.project(key);
            projection.setTableId(t.getTableId());
            keyOnlyRelations.add(projection);

            for(TableColumn c : projection.getColumns()) {
                wColumnStatistics.write(String.format("%s\n", StringUtils.join(new String[] {
                    t.getPath(),
                    Integer.toString(originalKeySize),
                    Integer.toString(originalColumnCount),
                    Integer.toString(t.getSize()),
                    tblClass,
                    c.getIdentifier(),
                    c.getHeader()
                }, "\t")));
            }
        }

        wStatistics.close();
        wColumnStatistics.close();

        // Processable<Correspondence<MatchableTableColumn, Matchable>> cors = null;
        Processable<Correspondence<MatchableTableColumn, Matchable>> cors = relMatcher
                .matchRelationTableColumnsToClasses(keyOnlyRelations, idf);

        System.out.println(String.format("Found %d correspondences", cors.size()));

        BufferedWriter w = new BufferedWriter(new FileWriter(new File("matching_results.tsv")));
        for (Correspondence<MatchableTableColumn, Matchable> cor : cors.get()) {
            w.write(String.format("%s\n",
                    StringUtils.join(
                            new String[] { cor.getFirstRecord().getIdentifier(),
                                    web.getTables().get(cor.getFirstRecord().getTableId()).getPath(),
                                    cor.getFirstRecord().getHeader(), cor.getFirstRecord().getType().toString(),
                                    cor.getSecondRecord().getIdentifier(), Double.toString(cor.getSimilarityScore()) },
                            "\t")));
        }
        w.close();

        Correspondence.writeToCsv(new File(resultLocation), cors);
        evaluate(Correspondence.toMatchable2(cors));

        writeDetailedResults(cors, web, new File(resultLocation + "_details.tsv"));

        // cors = relMatcher.matchRelationTableColumnsToClassesByEntitiesViaIndexingPerTable(keyOnlyRelations, idf, cors, 0.0, 0.4, new TokenizingJaccardSimilarity(), patternLocation, resultLocation + "_blocks.csv");
        // evaluate(Correspondence.toMatchable2(cors));
        // writeDetailedResults(cors, web, new File(resultLocation + "_bycell_details.tsv"));

        if(candidatesOnly) {
            return;
        }

        System.out.println("Refining matches...");
        if(trainLocation!=null) {
            MatchingGoldStandard gs = new MatchingGoldStandard();
            gs.loadFromCSVFile(new File(trainLocation));
            gs.setComplete(true);
            cors = relMatcher.tuneMatchRelationTableColumnsToClassesByEntities(keyOnlyRelations, cors, gs, resultLocation, patternLocation);
        } else {
            cors = relMatcher.matchRelationTableColumnsToClassesByEntities(keyOnlyRelations, cors, entityLinkingThreshold, new TokenizingJaccardSimilarity(), patternLocation, 0.0, minMappedEntities, finalThreshold, new File(resultLocation + "_refined_details.tsv"), gsLocation);
        }
        Correspondence.writeToCsv(new File(resultLocation + "_refined.csv"), cors);

        w = new BufferedWriter(new FileWriter(new File("matching_results_refined.tsv")));
        for (Correspondence<MatchableTableColumn, Matchable> cor : cors.get()) {
            w.write(String.format("%s\n",
                    StringUtils.join(
                            new String[] { web.getTables().get(cor.getFirstRecord().getTableId()).getPath(),
                                    cor.getFirstRecord().getHeader(), cor.getFirstRecord().getType().toString(),
                                    cor.getSecondRecord().getIdentifier(), Double.toString(cor.getSimilarityScore()) },
                            "\t")));
        }
        w.close();

        evaluate(Correspondence.toMatchable2(cors));
    }

    protected void writeDetailedResults(Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences, WebTables web, File f) throws IOException {
        BufferedWriter w = new BufferedWriter(new FileWriter(f));

        try {

            w.write(String.format("%s\n", StringUtils.join(new String[] {
                "Column",
                "Class",
                "Score",
                "Table rows",
                "Table columns",
                "Table class",
                "Column header",
                "Is Context",
                "Key Size"
            }, "\t")));

            for(Correspondence<MatchableTableColumn, Matchable> cor : correspondences.get()) {

                Table t = web.getTables().get(cor.getFirstRecord().getDataSourceIdentifier());
                String tblClass = null;
                Pair<String, Double> cls = t.getMapping().getMappedClass();
                if(cls!=null) {
                    tblClass = cls.getFirst();
                } else {
                    tblClass = t.getPath().split("\\_")[0];
                }

                Set<TableColumn> key = Q.firstOrDefault(t.getSchema().getCandidateKeys());
                int keySize = 0;
                if(key!=null) {
                    keySize = key.size();
                }

                w.write(String.format("%s\n", StringUtils.join(new String[] {
                    cor.getFirstRecord().getIdentifier(),
                    cor.getSecondRecord().getIdentifier(),
                    Double.toString(cor.getSimilarityScore()),
                    Integer.toString(t.getSize()),
                    Integer.toString(t.getColumns().size()),
                    tblClass,
                    cor.getFirstRecord().getHeader(),
                    Boolean.toString(ContextColumns.isContextColumn(cor.getFirstRecord())),
                    Integer.toString(keySize)
                }, "\t")));
            }

        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            w.close();
        }
    }

    public void runEval() throws IOException {
        Processable<Correspondence<RecordId, RecordId>> cors = Correspondence.loadFromCsv(new File(resultLocation + "_refined.csv"));
        evaluate(Correspondence.toMatchable2(Correspondence.toMatchable(cors)));
    }

    public void evaluate(Processable<Correspondence<Matchable, Matchable>> correspondences) throws IOException {
        if(gsLocation!=null) {
            MatchingEvaluator<Matchable, Matchable> evaluator = new MatchingEvaluator<>();
            MatchingGoldStandard gs = new MatchingGoldStandard();
            gs.loadFromCSVFile(new File(gsLocation));
            gs.setComplete(true);
            Set<String> mappedColumns = new HashSet<>();
            for(Pair<String, String> p : gs.getPositiveExamples()) {
                mappedColumns.add(p.getFirst());
            }
            System.out.println(String.format("The gold standard contains correspondences for %d unique columns", mappedColumns.size()));

            Set<String> containsCorrect = new HashSet<>();
            for(Correspondence<Matchable, Matchable> cor : correspondences.get()) {
                if(gs.containsPositive(cor.getFirstRecord(), cor.getSecondRecord())) {
                    containsCorrect.add(cor.getFirstRecord().getIdentifier());
                }
            }

            evaluator.writeEvaluation(new File(resultLocation + "_evaluation_all.csv"), correspondences, gs);

            // GS contains multiple possible correspondences for a column
            // but as long as correspondences only map each column once, we can apply the usual evaluation
            MatchingEngine<Matchable, Matchable> engine = new MatchingEngine<>();
            correspondences = engine.getTopKInstanceCorrespondences(correspondences, 1, 0.0);            
            evaluator.writeEvaluation(new File(resultLocation + "_evaluation.csv"), correspondences, gs);
            Performance perf = evaluator.evaluateMatching(correspondences, gs);
            double recall = perf.getNumberOfCorrectlyPredicted() / (double)mappedColumns.size();
            System.out.println(String.format("Precision: %f\nRecall: %f\nF1: %f", perf.getPrecision(), recall, (2*perf.getPrecision()*recall)/(perf.getPrecision()+recall)));
            System.out.println(String.format("Max Recall is %f", containsCorrect.size() / (double)mappedColumns.size()));
        }
    }

    /**
     * Removes tokens which are create by a template on the web site, i.e., the same tokens occur at the same position in every value
     */
    public void removeTemplateTokens(TableColumn c) {

    }

}