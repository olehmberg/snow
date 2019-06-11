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
package de.uni_mannheim.informatik.dws.snow;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import de.uni_mannheim.informatik.dws.tnt.match.CorrespondenceFormatter;
import de.uni_mannheim.informatik.dws.tnt.match.data.EntityTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SnowTableToTableEvaluator {

    public void evaluateTableToTableMatching(
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
		Processable<Correspondence<MatchableTableColumn, Matchable>> reference,
		Map<String, Integer> originalWebTablesCount,
		File logDirectory,
		String taskName,
        SnowPerformance performanceLog,
        String stepName,
        DataSet<MatchableTableColumn, MatchableTableColumn> attributes
	) throws IOException {
		System.out.println("Generated attribute clusters");
		CorrespondenceFormatter.printAttributeClusters(schemaCorrespondences);

		System.out.println("Reference attribute clusters");
        CorrespondenceFormatter.printAttributeClusters(reference);
        
		// evaluate table-to-table matching
		// N2NGoldStandard clusterGs = N2NGoldStandard.createFromCorrespondences(reference.get());
		Map<String, Integer> columnWeights = new HashMap<>();
		N2NGoldStandard clusterGs = new N2NGoldStandard();
		int clusterId = 0;
		for(Collection<MatchableTableColumn> cluster : Correspondence.getConnectedComponents(reference.get())) {
			Distribution<String> nameDist = Distribution.fromCollection(cluster, (c)->c.getHeader());
			Set<String> ids = new HashSet<>(Q.project(cluster, (c)->c.getIdentifier()));
			for(String id : ids) {
				String tbl = id.split("~")[0];
				int originalTables = originalWebTablesCount.get(tbl);
				columnWeights.put(id, originalTables);
			}
			clusterGs.getCorrespondenceClusters().put(ids, String.format("[%d] %s", clusterId++,StringUtils.join(nameDist.getElements(), ",")));
        }
        
        SnowStatisticsWriter.writeTableToTableGoldStandardStatistics(new File(logDirectory, "table_to_table_goldstandard_statistics.tsv"), taskName, attributes, clusterGs);

		// N2NGoldStandard results = N2NGoldStandard.createFromCorrespondences(schemaCorrespondences.get());
		// create named clusters for evaluation
		Map<Set<String>, String> correspondenceClusters = new HashMap<>();
		for(Collection<MatchableTableColumn> cluster : Correspondence.getConnectedComponents(schemaCorrespondences.get())) {
			Distribution<String> nameDist = Distribution.fromCollection(cluster, (c)->c.getHeader());
			Set<String> ids = new HashSet<>(Q.project(cluster, (c)->c.getIdentifier()));
			for(String id : ids) {
				if(!columnWeights.containsKey(id)) {
					String tbl = id.split("~")[0];
					int originalTables = originalWebTablesCount.get(tbl);
					columnWeights.put(id, originalTables);
				}
			}
			correspondenceClusters.put(ids, StringUtils.join(nameDist.getElements(), ","));
		}

		ClusteringPerformance clusterPerf = clusterGs.evaluateWeightedCorrespondenceClusters(correspondenceClusters, columnWeights, false, false);
		Performance perf = clusterPerf.getOverallPerformanceFromPerformanceByCluster();

		System.out.println("Table-to-table matching evaluation:");
		System.out.println(String.format("Precision:\t%f\nRecall:\t%f\nF1-measure:\t%f", perf.getPrecision(), perf.getRecall(), perf.getF1()));
		
		BufferedWriter w = new BufferedWriter(new FileWriter(new File(logDirectory, "tabletotable_evaluation.tsv")));
		w.write(StringUtils.join(new String[] {
                taskName,
                stepName,
				Double.toString(perf.getPrecision()),
				Double.toString(perf.getRecall()),
				Double.toString(perf.getF1())
		}, "\t") + "\n");
		w.close();

		performanceLog.addPerformance(String.format("Table-to-table matching [%s]", stepName), perf);
		for(String name : clusterPerf.getPerformanceByCluster().keySet()) {
			performanceLog.addPerformanceDetail(String.format("Table-to-table matching [%s]", stepName), name, clusterPerf.getPerformanceByCluster().get(name));
		}
	}

    public void evaluateProjectedTableToTableMatching(
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
		Processable<Correspondence<MatchableTableColumn, Matchable>> reference,
		Map<String, Integer> originalWebTablesCount,
		File logDirectory,
		String taskName,
        SnowPerformance performanceLog,
        String stepName, 
        Map<Integer,Table> tables,
        Collection<EntityTable> entityTables,
        Collection<EntityTable> referenceEntityTables,
        boolean skipDetails
	) throws IOException {
		System.out.println("Generated attribute clusters");
		CorrespondenceFormatter.printAttributeClusters(schemaCorrespondences);

		System.out.println("Reference attribute clusters");
		CorrespondenceFormatter.printAttributeClusters(reference);

        Set<String> tablesLinkedToEntities = new HashSet<>();
        for(EntityTable et : referenceEntityTables) {
            if(!et.isNonEntity()) {
                tablesLinkedToEntities.addAll(et.getProvenance());
            }
        }

		// evaluate table-to-table matching
		Map<String, Integer> columnWeights = new HashMap<>();
		N2NGoldStandard clusterGs = new N2NGoldStandard();
		int clusterId = 0;
		for(Collection<MatchableTableColumn> cluster : Correspondence.getConnectedComponents(reference.get())) {
			Distribution<String> nameDist = Distribution.fromCollection(cluster, (c)->c.getHeader());
            Set<String> ids = new HashSet<>(Q.project(cluster, (c)->c.getIdentifier()));
            
            // remove all columns which are not assigned to any entity in the gold standard (they should no longer be in the correspondences if the matcher created a correct result)
            Iterator<String> idIt = ids.iterator();
            while(idIt.hasNext()) {
                String id = idIt.next();
                String tbl = id.split("~")[0];
                if(tablesLinkedToEntities.contains(tbl)) {
                    int originalTables = originalWebTablesCount.get(tbl);
                    columnWeights.put(id, originalTables);
                } else {
                    idIt.remove();
                }
            }

            if(ids.size()>1) {
                clusterGs.getCorrespondenceClusters().put(ids, String.format("[%d] %s", clusterId++,StringUtils.join(nameDist.getElements(), ",")));
            }
		}

		// create named clusters for evaluation
		Map<Set<String>, String> correspondenceClusters = new HashMap<>();
		for(Collection<MatchableTableColumn> cluster : Correspondence.getConnectedComponents(schemaCorrespondences.get())) {
			Distribution<String> nameDist = Distribution.fromCollection(cluster, (c)->c.getHeader());
            Set<String> ids = new HashSet<>();
            // the correspondences are among projected tables, so we have to get the orignal ids from their provenance
            for(MatchableTableColumn c : cluster) {
                if(!"FK".equals(c.getHeader()) && !c.getHeader().startsWith("Entity Context")) { // exclude the artificially generated FK columns and Entity Context columns
                    ids.addAll(tables.get(c.getTableId()).getSchema().get(c.getColumnIndex()).getProvenance());
                }
            }
            if(ids.size()>1) {
                for(String id : ids) {
                    if(!columnWeights.containsKey(id)) {
                        String tbl = id.split("~")[0];
                        int originalTables = originalWebTablesCount.get(tbl);
                        columnWeights.put(id, originalTables);
                    }
                }
                correspondenceClusters.put(ids, StringUtils.join(nameDist.getElements(), ","));
            }
        }
        
        // add clusters for the entity table key columns (which are no longer in the set of correspondences)
        for(EntityTable et : entityTables) {
            if(!et.isNonEntity()) {
                System.out.println(String.format("Creating correspondence clusters for entity '%s'", et.getEntityName()));
                for(String attribute : et.getAttributeProvenance().keySet()) {
                    // Entity Context is not contained in the reference mapping and the same as the key attribute correspondences, so we skip it, too
                    if(!attribute.startsWith("Entity Context")) { // entity context attributes are in the FK tables and still contained in the correspondences!
                        Set<String> ids = et.getAttributeProvenance().get(attribute);
                        for(String id : ids) {
                            if(!columnWeights.containsKey(id)) {
                                String tbl = id.split("~")[0];
                                int originalTables = originalWebTablesCount.get(tbl);
                                columnWeights.put(id, originalTables);
                            }
                        }
                        correspondenceClusters.put(ids, String.format("%s::%s", et.getEntityName(), attribute));
                        System.out.println(String.format("\tClusters '%s' : %s", String.format("%s::%s", et.getEntityName(), attribute), StringUtils.join(ids, ",")));
                    }
                }
            }
        }

		ClusteringPerformance clusterPerf = clusterGs.evaluateWeightedCorrespondenceClusters(correspondenceClusters, columnWeights, false, false);
		Performance perf = clusterPerf.getOverallPerformanceFromPerformanceByCluster();

		System.out.println("Table-to-table matching evaluation:");
		System.out.println(String.format("Precision:\t%f\nRecall:\t%f\nF1-measure:\t%f", perf.getPrecision(), perf.getRecall(), perf.getF1()));
		
		BufferedWriter w = new BufferedWriter(new FileWriter(new File(logDirectory, "tabletotable_evaluation.tsv")));
		w.write(StringUtils.join(new String[] {
                taskName,
                stepName,
				Double.toString(perf.getPrecision()),
				Double.toString(perf.getRecall()),
				Double.toString(perf.getF1())
		}, "\t") + "\n");
		w.close();

        performanceLog.addPerformance(String.format("Table-to-table matching [%s]", stepName), perf);
        if(!skipDetails) {
            for(String name : clusterPerf.getPerformanceByCluster().keySet()) {
                performanceLog.addPerformanceDetail(String.format("Table-to-table matching [%s]", stepName), name, clusterPerf.getPerformanceByCluster().get(name));
            }
        }
	}
}