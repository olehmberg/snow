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
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.*;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers.RelationTableMatcher;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.VectorCreationMethod;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.*;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceCosineSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceJaccardSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceSimilarity;

/**
 * 
 * Matches relations across different hosts
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchCrossSite extends Executable {

    @Parameter(names = "-web")
    private String webLocation;

    @Parameter(names = "-kb")
    private String entityDefinitionLocation;

    @Parameter(names = "-result")
    private String resultLocation;

    @Parameter(names = "-vector")
    private VectorCreationMethod vectorCreation;

    @Parameter(names = "-sim")
    private Sim similarity;

    @Parameter(names = "-threshold")
    private double threshold;

    public enum Sim {
        Jaccard,
        Cosine
    }

    public static void main(String[] args) throws Exception {
        MatchCrossSite app = new MatchCrossSite();

        if(app.parseCommandLine(MatchCrossSite.class, args)) {
            app.run();
        }
    }

    public void run() throws Exception {
        // load web tables
        File webFile = new File(webLocation);
        WebTables web = WebTables.loadWebTables(webFile, true, true, false, false);

        RelationTableMatcher relMatcher = new RelationTableMatcher();

        // Collection<Table> entityTables = Q.where(web.getTables().values(), (t)->!t.getPath().contains("_rel_"));
        Collection<Table> relationTables = Q.where(web.getTables().values(), (t)->t.getPath().contains("_rel_"));

        System.out.println(String.format("Matching %d relations", relationTables.size()));

        Collection<Table> keyOnlyRelations = relationTables;
        // Collection<Table> keyOnlyRelations = new LinkedList<>();
        // for(Table t : relationTables) {
        //     // Set<TableColumn> key = Q.firstOrDefault(t.getSchema().getCandidateKeys());
        //     Set<TableColumn> key = new HashSet<>(t.getColumns());

        //     Iterator<TableColumn> it = key.iterator();
        //     while(it.hasNext()) {
        //         TableColumn c = it.next();
        //         if(
        //             "fk".equals(c.getHeader())
        //             || c.getDataType()!=DataType.string
        //         ) {
        //             it.remove();
        //         }
        //     }

        //     Table projection = t.project(key);
        //     projection.setTableId(t.getTableId());
        //     projection.setMapping(t.getMapping());
        //     projection.setContext(t.getContext());
        //     keyOnlyRelations.add(projection);
        // }

        VectorSpaceSimilarity measure = null;

        switch(similarity) {
            case Jaccard:
                measure = new VectorSpaceJaccardSimilarity();
                break;
            case Cosine:
                measure = new VectorSpaceCosineSimilarity();
                break;
            default:
        }

        // Scalability: if we do not prevent correspondences between context columns in this step,
        // the correspondence graph can be huge, which means that the graph-based refinement cannot handle it in a reasonable time
        // (if we add context columns to the set of disjoint column headers by setting the second parameter to false, 
        // the pair-wise refinement will take care of such conflicts and reduce the size of the graph before the graph-based refinement)
        DisjointHeaders dh = DisjointHeaders.fromTables(web.getTables().values(), false);

        // Processable<Correspondence<MatchableTableColumn, Matchable>> cors = relMatcher.matchRelationTables(keyOnlyRelations);
        Processable<Correspondence<MatchableTableColumn, Matchable>> cors = relMatcher.matchRelationTablesWithRefinement(
            keyOnlyRelations, dh, new File("./"),
            vectorCreation, measure, threshold);

        // filter out correspondences within the same site
        cors = cors.where((cor)->!url(web.getTables().get(cor.getFirstRecord().getTableId())).equals(url(web.getTables().get(cor.getSecondRecord().getTableId()))));

        System.out.println(String.format("Found %d correspondences", cors.size()));

        Set<Collection<MatchableTableColumn>> clusters = Correspondence.getConnectedComponents(cors.get());

        BufferedWriter w = new BufferedWriter(new FileWriter(new File("matching_results.tsv")));
        BufferedWriter wA = new BufferedWriter(new FileWriter(new File("matching_attributes.tsv")));
        BufferedWriter wD = new BufferedWriter(new FileWriter(new File("matching_attribute_domains.tsv")));
        int clusterId = 0;
        for(Collection<MatchableTableColumn> clu : clusters) {
            Set<String> headers = new HashSet<>();
            for(MatchableTableColumn col : clu) {
                headers.add(col.getIdentifier() + ":" + col.getHeader());
            }

            w.write(String.format("%s\n", StringUtils.join(new String[] {
                // web.getTables().get(cor.getFirstRecord().getTableId()).getPath(),
                StringUtils.join(headers, ",")
            }, "\t")));

            for(MatchableTableColumn c : clu) {
                Table t = web.getTables().get(c.getTableId());
                wA.write(String.format("%s\n", StringUtils.join(new String[] {
                    Integer.toString(clusterId),
                    t.getPath(),
                    url(t),
                    t.getMapping().getMappedClass().getFirst(),
                    Integer.toString(c.getColumnIndex()),
                    c.getHeader(),
                    c.getType().toString()
                }, "\t")));

                TableColumn col = t.getSchema().get(c.getColumnIndex());
                Set<Object> dom = t.getColumnDomains().get(col);

                if(dom!=null) {
                    for(Object o : dom) {
                        wD.write(String.format("%s\n", StringUtils.join(new String[] {
                            Integer.toString(clusterId),
                            t.getPath(),
                            Integer.toString(c.getColumnIndex()),
                            c.getHeader(),
                            o.toString()
                        }, "\t")));
                    }
                }
            }
            clusterId++;
        }
        w.close();
        wA.close();
        wD.close();

        Correspondence.writeToCsv(new File(resultLocation), cors);

        BufferedWriter wS = new BufferedWriter(new FileWriter(new File("matching_scores.tsv")));
        for(Correspondence<MatchableTableColumn, Matchable> cor : cors.get()) {
            Table t1 = web.getTables().get(cor.getFirstRecord().getTableId());
            Table t2 = web.getTables().get(cor.getSecondRecord().getTableId());

            wS.write(String.format("%s\n", StringUtils.join(new String [] {
                t1.getPath(),
                url(t1),
                t1.getMapping().getMappedClass().getFirst(),
                Integer.toString(cor.getFirstRecord().getColumnIndex()),
                cor.getFirstRecord().getHeader(),
                t2.getPath(),
                url(t2),
                t2.getMapping().getMappedClass().getFirst(),
                Integer.toString(cor.getSecondRecord().getColumnIndex()),
                cor.getSecondRecord().getHeader(),
                Double.toString(cor.getSimilarityScore())
            }, "\t")));
        }
        wS.close();
    }

    private static String url(Table t) {
        return t.getPath().replaceAll(".+_(.+)_rel_\\d+\\.json", "$1").replace(".json", "");
    }
}