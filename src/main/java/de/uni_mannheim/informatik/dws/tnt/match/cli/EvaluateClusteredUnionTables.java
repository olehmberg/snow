/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import com.beust.jcommander.Parameter;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableDataSetLoader;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.UnionTables;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.MaximumBipartiteMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence.RecordId;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EvaluateClusteredUnionTables extends Executable {

	@Parameter(names = "-web", required=true)
	private String webLocation;

	@Parameter(names = "-reference", required=true)
	private String referenceLocation;

	@Parameter(names = "-log")
	private String logLocation;

	@Parameter(names = "-urlStatistics")
	private String urlStatisticsLocation;
	
	public static void main(String[] args) throws URISyntaxException, IOException {
		EvaluateClusteredUnionTables app = new EvaluateClusteredUnionTables();
		
		if(app.parseCommandLine(EvaluateClusteredUnionTables.class, args)) {
			app.run();
		}
	}
	
	public void run() throws URISyntaxException, IOException {
	
		System.err.println("Loading Web Tables");
		// load web tables
		WebTables web = WebTables.loadWebTables(new File(webLocation), true, false, false, false);

		// count tables per url
		Map<String, Integer> tablesPerUrl = new HashMap<>();
		for(Table t : web.getTables().values()) {
			MapUtils.increment(tablesPerUrl, t.getContext().getUrl());
		}

		UnionTables union = new UnionTables();

		System.err.println("Clustering tables by URL");
		Collection<Collection<Table>> clustering = union.getTableClustersFromURLs(web.getTables().values(), 0.1, true);

		Map<Set<String>, String> clustersToEvaluate = new HashMap<>();
		for(Collection<Table> clu : clustering) {
			Set<String> values = new HashSet<>();
			for(Table t : clu) {
				values.add(t.getContext().getUrl());
			}
			if(values.size()>0) {
				clustersToEvaluate.put(values, Q.firstOrDefault(values));
			}
		}

		N2NGoldStandard gs = new N2NGoldStandard();
		
		gs.loadFromTxt(new File(referenceLocation));

		System.out.println("Evaluating clustered union tables");
		evaluate(clustersToEvaluate, gs.getCorrespondenceClusters(), tablesPerUrl);

		System.out.println("Evaluating original union tables");
		clustering = union.getStitchableTables(web.getTables().values());
		clustersToEvaluate = new HashMap<>();
		int idx = 0;
		for(Collection<Table> clu : clustering) {
			Set<String> values = new HashSet<>();
			for(Table t : clu) {
				values.add(t.getContext().getUrl());
			}
			if(values.size()>0) {
				clustersToEvaluate.put(values, Q.firstOrDefault(values) + Integer.toString(idx++));
			}
		}
		evaluateOriginalUnion(clustersToEvaluate, gs.getCorrespondenceClusters(), tablesPerUrl);

		// ClusteringPerformance perf = gs.evaluateCorrespondenceClusters(clustersToEvaluate, true);
		// System.out.println(perf.format());


		System.err.println("Done.");
	}

	public void evaluate(Map<Set<String>, String> result, Map<Set<String>, String> reference, Map<String, Integer> tablesPerUrl) {
		Processable<Correspondence<RecordId,RecordId>> cors = new ParallelProcessableCollection<>();
		Map<String, Set<String>> resultIndex = new HashMap<>();
		Map<String, Set<String>> referenceIndex = new HashMap<>();
		for(Set<String> clu : result.keySet()) {
			String cluId = result.get(clu);
			resultIndex.put(cluId, clu);
			for(Set<String> ref : reference.keySet()) {
				String refId = reference.get(ref);
				referenceIndex.put(refId, ref);
				Set<String> intersection = Q.intersection(clu, ref);

				int tableCount = 0;
				for(String s : intersection) {
					tableCount += tablesPerUrl.get(s);
				}

				Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(cluId), new RecordId(refId), (double)tableCount);
				cors.add(cor);
			}
		}

		MaximumBipartiteMatchingAlgorithm<RecordId, RecordId> max = new MaximumBipartiteMatchingAlgorithm<>(cors);
		max.run();
		cors = max.getResult();

		Map<String, Performance> byCluster = new HashMap<>();
		int created=0, correct=0, total=0;

		for(Set<String> ref : reference.keySet()) {
			int refTbls = 0;
			for(String s : ref) {
				refTbls += tablesPerUrl.get(s);
			}
			System.out.println(String.format("Reference Cluster '%s': %d URLs / %d web tables", reference.get(ref), ref.size(), refTbls));			
			//total += ref.size();
			total += refTbls;
		}
		System.out.println();

		for(Correspondence<RecordId, RecordId> cor : cors.get()) {
			Set<String> clu = resultIndex.get(cor.getFirstRecord().getIdentifier());
			Set<String> ref = referenceIndex.get(cor.getSecondRecord().getIdentifier());

			int cluTbls = 0;
			for(String s : clu) {
				cluTbls += tablesPerUrl.get(s);
			}
			int refTbls = 0;
			for(String s : ref) {
				refTbls += tablesPerUrl.get(s);
			}

			Performance perf = new Performance((int)cor.getSimilarityScore(), cluTbls, refTbls);
			correct += (int)cor.getSimilarityScore();
			created += perf.getNumberOfPredicted();
			//created += clu.size();
			//total += ref.size();

			byCluster.put(cor.getSecondRecord().getIdentifier(), perf);

			System.out.println(String.format("Cluster '%s': ", cor.getSecondRecord().getIdentifier()));
			System.out.println(String.format("\tP: %.6f / R: %.6f / F1: %.6f", perf.getPrecision(), perf.getRecall(), perf.getF1()));
			System.out.println(String.format("\tCreated: %d / Correct: %d / Reference: %d", perf.getNumberOfPredicted(), perf.getNumberOfCorrectlyPredicted(), perf.getNumberOfCorrectTotal()));
		}

		Performance overall = new Performance(correct, created, total);
		System.out.println();
		System.out.println("Overall Performance:");
		System.out.println(String.format("\tP: %.6f / R: %.6f / F1: %.6f", overall.getPrecision(), overall.getRecall(), overall.getF1()));
		System.out.println(String.format("\tCreated: %d / Correct: %d / Reference: %d", overall.getNumberOfPredicted(), overall.getNumberOfCorrectlyPredicted(), overall.getNumberOfCorrectTotal()));
	}

	public void evaluateOriginalUnion(Map<Set<String>, String> result, Map<Set<String>, String> reference, Map<String, Integer> tablesPerUrl) {
		Processable<Correspondence<RecordId,RecordId>> cors = new ParallelProcessableCollection<>();
		Map<String, Set<String>> resultIndex = new HashMap<>();
		Map<String, Set<String>> referenceIndex = new HashMap<>();
		for(Set<String> clu : result.keySet()) {
			String cluId = result.get(clu);
			resultIndex.put(cluId, clu);
			for(Set<String> ref : reference.keySet()) {
				String refId = reference.get(ref);
				referenceIndex.put(refId, ref);
				Set<String> intersection = Q.intersection(clu, ref);

				int tableCount = 0;
				for(String s : intersection) {
					tableCount += tablesPerUrl.get(s);
				}
				int cluTbls = 0;
				for(String s : clu) {
					cluTbls += tablesPerUrl.get(s);
				}
				int refTbls = 0;
				for(String s : ref) {
					refTbls += tablesPerUrl.get(s);
				}

				Correspondence<RecordId, RecordId> cor = new Correspondence<>(new RecordId(cluId), new RecordId(refId), (double)tableCount);
				cors.add(cor);

				System.out.println(String.format("%s <-> %s: intersection: %d, created: %d, ref: %d", cluId, refId, tableCount, cluTbls, refTbls));
			}
		}

		MatchingEngine<RecordId, RecordId> engine = new MatchingEngine<>();
		cors = engine.getTopKInstanceCorrespondences(cors, 1, 0.0);

		Map<String, Performance> byCluster = new HashMap<>();
		int created=0, correct=0, total=0;

		for(Set<String> ref : reference.keySet()) {
			int refTbls = 0;
			for(String s : ref) {
				refTbls += tablesPerUrl.get(s);
			}
			System.out.println(String.format("Reference Cluster '%s': %d URLs / %d web tables", reference.get(ref), ref.size(), refTbls));			
			//total += ref.size();
			total += refTbls;
		}
		System.out.println();

		int falsePositives = 0;
		for(Correspondence<RecordId, RecordId> cor : cors.get()) {
			Set<String> clu = resultIndex.get(cor.getFirstRecord().getIdentifier());
			Set<String> ref = referenceIndex.get(cor.getSecondRecord().getIdentifier());

			int cluTbls = 0;
			for(String s : clu) {
				cluTbls += tablesPerUrl.get(s);
			}
			int refTbls = 0;
			for(String s : ref) {
				refTbls += tablesPerUrl.get(s);
			}

			Performance perf = new Performance((int)cor.getSimilarityScore(), cluTbls, refTbls);
			correct += (int)cor.getSimilarityScore();
			created += perf.getNumberOfPredicted();
			//created += clu.size();
			//total += ref.size();

			byCluster.put(cor.getSecondRecord().getIdentifier(), perf);

			System.out.println(String.format("Cluster '%s': ", cor.getSecondRecord().getIdentifier()));
			System.out.println(String.format("\tP: %.6f / R: %.6f / F1: %.6f", perf.getPrecision(), perf.getRecall(), perf.getF1()));
			System.out.println(String.format("\tCreated: %d / Correct: %d / Reference: %d", perf.getNumberOfPredicted(), perf.getNumberOfCorrectlyPredicted(), perf.getNumberOfCorrectTotal()));
			System.out.println(String.format("\tFalse Positives: %d", perf.getNumberOfPredicted() - perf.getNumberOfCorrectlyPredicted()));
			falsePositives += perf.getNumberOfPredicted() - perf.getNumberOfCorrectlyPredicted();
		}

		Performance overall = new Performance(correct, created, total);
		System.out.println();
		System.out.println("Overall Performance:");
		System.out.println(String.format("\tP: %.6f / R: %.6f / F1: %.6f", overall.getPrecision(), overall.getRecall(), overall.getF1()));
		System.out.println(String.format("\tCreated: %d / Correct: %d / Reference: %d", overall.getNumberOfPredicted(), overall.getNumberOfCorrectlyPredicted(), overall.getNumberOfCorrectTotal()));
		System.out.println(String.format("\tFalse Positives: %d", falsePositives));
	}
}
