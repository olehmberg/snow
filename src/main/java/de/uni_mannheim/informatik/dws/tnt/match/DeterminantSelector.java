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
package de.uni_mannheim.informatik.dws.tnt.match;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
//import de.uni_mannheim.informatik.dws.winter.clustering.OverlappingClusteringWithPositiveAndNegativeEdges;
import de.uni_mannheim.informatik.dws.winter.clustering.PartitioningWithPositiveAndNegativeEdges;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.*;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SetAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SumDoubleAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Edge;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Graph;
import de.uni_mannheim.informatik.dws.winter.utils.graph.Partitioning;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DeterminantSelector {

    private File logDirectory;

    private DataSet<MatchableTableDeterminant, MatchableTableColumn> propagatedCanddiateKeys;
    private Map<MatchableTableDeterminant, Triple<MatchableTableDeterminant, Set<Integer>, Double>> determinantScores;
    private boolean candidateKeysOnly = true;

    /**
     * @return the propagatedCanddiateKeys
     */
    public DataSet<MatchableTableDeterminant, MatchableTableColumn> getPropagatedCanddiateKeys() {
        return propagatedCanddiateKeys;
    }

    /**
     * @return the determinantScores
     */
    public Map<MatchableTableDeterminant, Triple<MatchableTableDeterminant, Set<Integer>, Double>> getDeterminantScores() {
        return determinantScores;
    }

    public DeterminantSelector(boolean candidateKeysOnly, File logDirectory) {
        this.candidateKeysOnly = candidateKeysOnly;
        this.logDirectory = logDirectory;
    }

    public Processable<Correspondence<MatchableTableColumn, Matchable>> selectDeterminant(
        Processable<MatchableTableRow> records,
        Processable<MatchableTableColumn> attributes, 
        Processable<MatchableTableDeterminant> candidateKeys,
        Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
        Map<Integer, Table> tables
    ) {

        Map<MatchableTableColumn, Integer> columnToCluster = getColumnToSchemaClusterMap(schemaCorrespondences);

        // create a collection of all determinants
        Processable<MatchableTableDeterminant> determinants = new ParallelProcessableCollection<>();
        for(Table t : tables.values()) {
            int tableId = t.getTableId();

            Set<TableColumn> combinedDeterminantsWithFK = new HashSet<>();
            Collection<TableColumn> combinedDependantsOfFK = new HashSet<>();
            TableColumn fk =Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));

            if(!candidateKeysOnly) {
                for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
                    if(det.contains(fk)) {
                        Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
                        
                        Collection<String> detIds = Q.project(det, (c)->c.getIdentifier());

                        if(det.contains(fk)) {
                            combinedDeterminantsWithFK.addAll(det);
                            combinedDependantsOfFK.addAll(dep);
                        }

                        Collection<TableColumn> closure = FunctionalDependencyUtils.closure(new HashSet<>(det), t.getSchema().getFunctionalDependencies());
                        Collection<String> depIds = Q.project(closure, (c)->c.getIdentifier());

                        MatchableTableDeterminant mdet = new MatchableTableDeterminant(
                            tableId, 
                            new HashSet<>(attributes.where((c)->detIds.contains(c.getIdentifier())).get()),
                            new HashSet<>(attributes.where((c)->depIds.contains(c.getIdentifier())).get()),
                            new HashSet<>(t.getColumns()).equals(new HashSet<>(closure))
                            );

                        if(mdet.getColumns().size()>0) {
                            determinants.add(mdet);

                            // System.out.println(String.format("[DeterminantSelector] Functional dependency for table #%d %s: {%s} -> {%s}", 
                            //     t.getTableId(),
                            //     t.getPath(),
                            //     StringUtils.join(Q.project(det, new TableColumn.ColumnHeaderProjection()), ","),
                            //     StringUtils.join(Q.project(dep, new TableColumn.ColumnHeaderProjection()), ",")
                            // ));
                        } else {
                            System.out.println(String.format("[DeterminantSelector] unmapped determinant: %s",
                                StringUtils.join(Q.project(det, new TableColumn.ColumnHeaderProjection()), ",")
                                ));
                        }
                    }
                }
            } else {
                // we only use candidate keys
                for(Collection<TableColumn> key : t.getSchema().getCandidateKeys()) {
                    if(key.contains(fk)) {
                        Collection<String> detIds = Q.project(key, (c)->c.getIdentifier());

                        Collection<TableColumn> closure = t.getColumns();
                        Collection<String> depIds = Q.project(closure, (c)->c.getIdentifier());

                        MatchableTableDeterminant mdet = new MatchableTableDeterminant(
                            tableId, 
                            new HashSet<>(attributes.where((c)->detIds.contains(c.getIdentifier())).get()),
                            new HashSet<>(attributes.where((c)->depIds.contains(c.getIdentifier())).get()),
                            new HashSet<>(t.getColumns()).equals(new HashSet<>(closure))
                            );

                        if(mdet.getColumns().size()>0) {
                            determinants.add(mdet);

                            // System.out.println(String.format("[DeterminantSelector] Functional dependency for table #%d %s: {%s} -> {%s}", 
                            //     t.getTableId(),
                            //     t.getPath(),
                            //     StringUtils.join(Q.project(det, new TableColumn.ColumnHeaderProjection()), ","),
                            //     StringUtils.join(Q.project(dep, new TableColumn.ColumnHeaderProjection()), ",")
                            // ));
                        } else {
                            System.out.println(String.format("[DeterminantSelector] unmapped determinant: %s",
                                StringUtils.join(Q.project(key, new TableColumn.ColumnHeaderProjection()), ",")
                                ));
                        }
                    }
                }
            }
        }

        // merge determinants if they are aligned by schema correspondences
        schemaCorrespondences = getDeterminantCorrespondencesFromClusters(determinants, schemaCorrespondences, tables);

        return schemaCorrespondences;
    }

    protected Processable<Correspondence<MatchableTableDeterminant, Matchable>> getDeterminantCorrespondences(
        Processable<MatchableTableDeterminant> determinants,
        Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences
    ) {
        
        Set<Collection<MatchableTableColumn>> schemaClusters = Correspondence.getConnectedComponents(schemaCorrespondences.get());
        Map<MatchableTableColumn, Integer> columnToCluster = new HashMap<>();
        int clusterId = 0;
        for(Collection<MatchableTableColumn> cluster : schemaClusters) {
            for(MatchableTableColumn c : cluster) {
                columnToCluster.put(c,clusterId);
            }
            clusterId++;
        }

        Processable<Correspondence<MatchableTableDeterminant, Matchable>> determinantCorrespondences = determinants
            .join(determinants, (d)->1)
            .map(
                (Pair<MatchableTableDeterminant,MatchableTableDeterminant> p)
                -> {
                    if(p.getFirst().getTableId()!=p.getSecond().getTableId()) {

                        Set<Integer> d1SchemaClusters = new HashSet<>();
                        for(MatchableTableColumn c : p.getFirst().getColumns()) {
                            d1SchemaClusters.add(columnToCluster.get(c));
                        }
                        Set<Integer> d2SchemaClusters = new HashSet<>();
                        for(MatchableTableColumn c : p.getSecond().getColumns()) {
                            d2SchemaClusters.add(columnToCluster.get(c));
                        }

                        if(d1SchemaClusters.size()==d2SchemaClusters.size() && d1SchemaClusters.containsAll(d2SchemaClusters)) {
                            return new Correspondence<>(p.getFirst(), p.getSecond(), 1.0);
                        }

                    }

                    return null;
                }
            );

        return determinantCorrespondences;

    }

    protected Map<MatchableTableColumn, Integer> getColumnToSchemaClusterMap(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
        Set<Collection<MatchableTableColumn>> schemaClusters = Correspondence.getConnectedComponents(schemaCorrespondences.get());
        Map<MatchableTableColumn, Integer> columnToCluster = new HashMap<>();
        int clusterId = 0;
        for(Collection<MatchableTableColumn> cluster : schemaClusters) {
            // System.out.println(String.format("[DeterminantSelector] Column cluster %d: {%s}", clusterId, MatchableTableColumn.formatCollection(cluster)));
            for(MatchableTableColumn c : cluster) {
                columnToCluster.put(c,clusterId);
            }
            clusterId++;
        }
        return columnToCluster;
    }

    protected Map<Integer, Set<Integer>> getTableToColumnClusterMap(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
        Map<MatchableTableColumn, Integer> columnToCluster = getColumnToSchemaClusterMap(schemaCorrespondences);

        Map<Integer, Set<Integer>> tableToColumnToCluster = new HashMap<>();
        int clusterId = 0;
        for(MatchableTableColumn c : columnToCluster.keySet()) {

            Set<Integer> clusters = MapUtils.get(tableToColumnToCluster, c.getTableId(), new HashSet<>());

            clusters.add(columnToCluster.get(c));
        }

        // for(Integer tableId : tableToColumnToCluster.keySet()) {
        //     System.out.println(String.format("[DeterminantSelector] Column clusters for table #%d: {%s}", tableId, StringUtils.join(tableToColumnToCluster.get(tableId), ",")));
        // }

        return tableToColumnToCluster;
    }

    protected Processable<Correspondence<MatchableTableColumn, Matchable>> getDeterminantCorrespondencesFromClusters(
        Processable<MatchableTableDeterminant> determinants,
        final Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
        Map<Integer, Table> tables
    ) {
        Processable<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> tablePairs = schemaCorrespondences
            // group correspondences by table combination
            .group((Correspondence<MatchableTableColumn, Matchable> record, DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) 
                -> {
                    resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
                });
        System.out.println(String.format("[DeterminantSelector] %d table pairs", tablePairs.size()));

            // join determinants based on possible table combinations
        Processable<Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>> firstJoin = tablePairs
            .join(determinants, (g)->g.getKey().getFirst(), (d)->d.getTableId());
        System.out.println(String.format("[DeterminantSelector] %d table pairs joined with left-side determinants", firstJoin.size()));

        Processable<Pair<Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>, MatchableTableDeterminant>> secondJoin = firstJoin
            .join(determinants, (p)->p.getFirst().getKey().getSecond(), (d)->d.getTableId());
        System.out.println(String.format("[DeterminantSelector] %d table pairs joined with left-side & right-side determinants", secondJoin.size()));

        Processable<Triple<MatchableTableDeterminant, MatchableTableDeterminant, Double>> determinantEdges = secondJoin
            .map(
                (Pair<Pair<Group<Pair<Integer,Integer>,Correspondence<MatchableTableColumn,Matchable>>,MatchableTableDeterminant>,MatchableTableDeterminant> p)
                -> {
                    MatchableTableDeterminant d1 = p.getFirst().getSecond();
                    MatchableTableDeterminant d2 = p.getSecond();

                    if(d1.getTableId()!=d2.getTableId()) {

                        Set<MatchableTableColumn> leftMapped = new HashSet<>();
                        Set<MatchableTableColumn> rightMapped = new HashSet<>();
                        Set<MatchableTableColumn> bothMappedLeft = new HashSet<>();
                        boolean isAllContextAttributes = true;
                        
                        
                        for(Correspondence<MatchableTableColumn, Matchable> cor : p.getFirst().getFirst().getRecords().get()) {

                            leftMapped.add(cor.getFirstRecord());
                            rightMapped.add(cor.getSecondRecord());

                            if(d1.getColumns().contains(cor.getFirstRecord())
                                && d2.getColumns().contains(cor.getSecondRecord())) {
                                bothMappedLeft.add(cor.getFirstRecord());

                                if(!ContextColumns.isContextColumn(cor.getFirstRecord()) && !"FK".equals(cor.getFirstRecord().getHeader())) {
                                    isAllContextAttributes = false;
                                }
                            }

                        }
                        
                        int leftColumns = d1.getColumns().size();
                        int rightColumns = d2.getColumns().size();

                        if(
                            (
                                bothMappedLeft.size() >= d2.getColumns().size())        // d2 is contained in d1
                                && (!candidateKeysOnly || d1.isCandidateKey() && d2.isCandidateKey())  // both are candidate keys if requested
                                && leftMapped.containsAll(d1.getColumns()) && rightMapped.containsAll(d2.getColumns())   // all columns are mapped
                                && !isAllContextAttributes      // the key is not all context (which will always match)
                            ) {
                                double score = 1.0;
                                return new Triple<>(d1, d2, score);
                            }
                    } 

                    return new Triple<>(d1, d2, -1.0);
                });

        Graph<MatchableTableDeterminant, Object> detGraph = Graph.fromTriples(determinantEdges.get());
        Partitioning<MatchableTableDeterminant> debugPartitioning = new Partitioning<>(detGraph);
        for(Triple<MatchableTableDeterminant,MatchableTableDeterminant,Double> t : determinantEdges.get()) {
            
            Set<String> colIds = new HashSet<>(Q.project(t.getFirst().getColumns(), (c)->c.getHeader()));
            if(Q.toSet("FK").equals(colIds)) {
                debugPartitioning.setPartition(t.getFirst(), 1);
            }
        
        
            colIds = new HashSet<>(Q.project(t.getSecond().getColumns(), (c)->c.getHeader()));
            if(Q.toSet("FK").equals(colIds)) {
                debugPartitioning.setPartition(t.getSecond(), 1);
            }
            
        }
        try {
            detGraph.writePajekFormat(new File(logDirectory, "determinantselector_determinantgraph.net"));
            debugPartitioning.writePajekFormat(new File(logDirectory, "determinantselector_determinantgraph_debug.clu"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // group edges by first node -> shows how many determinants match directly
        Processable<Pair<MatchableTableDeterminant, Set<Integer>>> matches = determinantEdges
            .where((t)->t.getThird()>0.0)
            .map((t)->new Triple<>(t.getFirst(), t.getSecond().getTableId(), t.getThird()))
            .aggregate(
                (Triple<MatchableTableDeterminant, Integer, Double> record,
                DataIterator<Pair<MatchableTableDeterminant, Integer>> resultCollector) -> resultCollector.next(new Pair<>(record.getFirst(), record.getSecond()))
                , new SetAggregator<>()
            );

        // add score that measures the value overlap for the determinants to prevent choosing a candidate key that does not deduplicate the tables!
        Processable<Triple<MatchableTableDeterminant,Set<Integer>,Double>> scores = matches
            .map((p)->{
                return new Triple<>(p.getFirst(), p.getSecond(), 1.0);
            });
        // - get all key values for each cluster (project determinant from table)
        // - group by key value per cluster
        // - each group contributes with its group size (=number of tables with that key value)
        // - aggregate by summing up?

        try {
            System.out.println("Determinant matches:");
            BufferedWriter w = new BufferedWriter(new FileWriter(new File(logDirectory, "determinantmatches.tsv")));
            // for(Triple<MatchableTableDeterminant, Set<Integer>, Double> p : scores.sort((p)->p.getSecond().size(),false).get()) {
                for(Triple<MatchableTableDeterminant, Set<Integer>, Double> p : scores
                    .sort((p)->p.getSecond().size(),false)  // sort by number of tables to break ties
                    .get()) {
                String determinant = StringUtils.join(Q.project(p.getFirst().getColumns(), (c)->c.getHeader()), ",");

                w.write(String.format("%s\n", StringUtils.join(new String[] {
                    determinant,
                    Integer.toString(p.getSecond().size()),
                    Double.toString(p.getThird()),
                    Integer.toString(p.getFirst().getTableId()),
                    StringUtils.join(p.getSecond(), ",")
                }, "\t")));
            }
            w.close();
        } catch(Exception e) { e.printStackTrace(); }

        determinantScores = Q.map(scores.get(), (t)->t.getFirst());

        // create table clusters from determinant matches
        Collection<Set<Integer>> tableClusters = new LinkedList<>();
        propagatedCanddiateKeys = new ParallelHashedDataSet<>();
        while(scores.size()>0) {

            // create the ranking
            scores = scores
                .sort((p)->p.getSecond().size(),false)  // sort by number of tables to break ties
                ;

            for(Triple<MatchableTableDeterminant, Set<Integer>, Double> p : scores.get()) {
                String determinant = StringUtils.join(Q.project(p.getFirst().getColumns(), (c)->c.getHeader()), ",");
                System.out.println(String.format("%d\t%f\t%s: #%d -> %s", 
                    p.getSecond().size(), 
                    p.getThird(),
                    determinant,
                    p.getFirst().getTableId(),
                    StringUtils.join(p.getSecond(), ",")
                ));
            }

            // select the largest match as cluster
            Triple<MatchableTableDeterminant, Set<Integer>, Double> p = scores.firstOrNull();

            Set<Integer> cluster = new HashSet<>(p.getSecond());
            cluster.add(p.getFirst().getTableId());
            tableClusters.add(cluster);

            System.out.println(String.format("Selected table cluster %s: %s", 
                MatchableTableColumn.formatCollection(p.getFirst().getColumns()),
                StringUtils.join(cluster, ",")));

            // remove all clusters that are contained in the selected cluster / remove all tables in this cluster from all other matches
            scores = scores
                .map((p0)->{
                    Collection<Integer> unclustered = Q.without(p0.getSecond(), cluster);
                    if(unclustered.size()>0 && !cluster.contains(p0.getFirst().getTableId())) {
                        return new Triple<>(p0.getFirst(), new HashSet<>(unclustered), p0.getThird());
                    } else {
                        return null;
                    }
                });

            // create a new MatchableTableDeterminant for every table in the cluster, which will be used as candidate key
            MatchableTableDeterminant key = p.getFirst();
            propagatedCanddiateKeys.add(key);
            // System.out.println(String.format("Candidate key: {%s}", StringUtils.join(Q.project(key.getColumns(), (c)->c.toString()), ",")));
            for(Integer tableId : p.getSecond()) {
                Collection<MatchableTableColumn> columns = getMatchingColumns(schemaCorrespondences, key.getColumns(), tableId);

                if(columns.size()>0) {
                    MatchableTableDeterminant propagatedKey = new MatchableTableDeterminant(tableId, new HashSet<>(columns));
                    // System.out.println(String.format("Candidate key: {%s}", StringUtils.join(Q.project(propagatedKey.getColumns(), (c)->c.toString()), ",")));
                    propagatedCanddiateKeys.add(propagatedKey);
                } else {
                    System.out.println(String.format("No columns found for table %d", tableId));
                }
            }
        }

        // filter schema correspondences according to table clusters
        Processable<Correspondence<MatchableTableColumn, Matchable>> filteredSchemaCorrespondences = schemaCorrespondences
            .where((c)-> Q.any(tableClusters, (clu)->clu.contains(c.getFirstRecord().getTableId()) && clu.contains(c.getSecondRecord().getTableId())));

        return filteredSchemaCorrespondences;

    }

    public List<MatchableTableColumn> getMatchingColumnsSortedByColumnIndex(
        Processable<Correspondence<MatchableTableColumn,Matchable>> schemaCorrespondences,
        Collection<MatchableTableColumn> columns,
        int tableId
    ) {
        return new ArrayList<>(schemaCorrespondences
        .where((c)-> 
            columns.contains(c.getFirstRecord()) && c.getSecondRecord().getTableId()==tableId
            ||
            columns.contains(c.getSecondRecord()) && c.getFirstRecord().getTableId()==tableId
        )
        .map(
        (Correspondence<MatchableTableColumn, Matchable> record,
                DataIterator<Pair<Integer,MatchableTableColumn>> resultCollector) 
        -> {
            if(record.getFirstRecord().getTableId()==tableId) {
                MatchableTableColumn c = Q.firstOrDefault(Q.where(columns, (c0)->c0.getIdentifier().equals(record.getSecondRecord().getIdentifier())));
                resultCollector.next(new Pair<>(c.getColumnIndex(), record.getFirstRecord()));
            } else if(record.getSecondRecord().getTableId()==tableId) {
                MatchableTableColumn c = Q.firstOrDefault(Q.where(columns, (c0)->c0.getIdentifier().equals(record.getFirstRecord().getIdentifier())));
                resultCollector.next(new Pair<>(c.getColumnIndex(), record.getSecondRecord()));
            }
        })
        .distinct()
        .sort((p)->p.getFirst())
        .map((p)->p.getSecond())
        .get());
    }

    public Collection<MatchableTableColumn> getMatchingColumns(
        Processable<Correspondence<MatchableTableColumn,Matchable>> schemaCorrespondences,
        Collection<MatchableTableColumn> columns,
        int tableId
    ) {
        return schemaCorrespondences
        .where((c)-> 
            columns.contains(c.getFirstRecord()) && c.getSecondRecord().getTableId()==tableId
            ||
            columns.contains(c.getSecondRecord()) && c.getFirstRecord().getTableId()==tableId
        )
        .map(
        (Correspondence<MatchableTableColumn, Matchable> record,
                DataIterator<MatchableTableColumn> resultCollector) 
        -> {
            if(record.getFirstRecord().getTableId()==tableId) {
                resultCollector.next(record.getFirstRecord());
            } else if(record.getSecondRecord().getTableId()==tableId) {
                resultCollector.next(record.getSecondRecord());
            }
        }).distinct().get();
    }
}