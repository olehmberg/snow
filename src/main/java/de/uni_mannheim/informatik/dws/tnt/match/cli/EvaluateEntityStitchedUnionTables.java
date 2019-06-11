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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.beust.jcommander.Parameter;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.IRPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.CellOverlapMatchingRule;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.FullRowBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.TableCellBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.blocking.TableCellColumnBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.CorrespondenceAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.TopKAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.GreedyOneToOneMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.MaximumBipartiteMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.VectorCreationMethod;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardRecordBlocker;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.PerformanceAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SumDoubleAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceJaccardSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.parallel.Parallel;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.Table.ConflictHandling;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EvaluateEntityStitchedUnionTables extends Executable {

	@Parameter(names = "-tables", required=true)
	private String tablesLocation;
	
	@Parameter(names = "-reference", required=true)
	private String referenceLocation;
	
	@Parameter(names = "-containment")
	private boolean measureContainment = false;
	
	@Parameter(names = "-globalLog")
	private String globalLogLocation;
	
	@Parameter(names = "-join")
	private String joinLocation;

	@Parameter(names = "-labelsMatch")
	private boolean labelsMatch;
	
	@Parameter(names = "-filenamesMatch")
	private boolean filenamesMatch;

	@Parameter(names = "-serialise")
	private boolean serialise;

	@Parameter(names = "-evaluateNulls")
	private boolean evaluateNulls;

	@Parameter(names = "-top1")
	private boolean useTop1;

	public static void main(String[] args) throws Exception {
		EvaluateEntityStitchedUnionTables app = new EvaluateEntityStitchedUnionTables();
		
		if(app.parseCommandLine(EvaluateEntityStitchedUnionTables.class, args)) {
			app.run();
		}
	}
	
	public void run() throws Exception {
		Parallel.setReportIfStuck(false);

		// load created tables
		System.out.println("Loading extracted tables");
		WebTables extractedTables = WebTables.loadWebTables(new File(tablesLocation), true, false, false, serialise);
//		extractedTables.printDensityReport();
		boolean tableNamesChanged = false;
		for(Table t : extractedTables.getTables().values()) {
			if(t.getPath().contains("reference")) {
				t.setPath(t.getPath().replace("reference_",""));
				tableNamesChanged = true;
			}
			System.out.println(String.format("Table #%d '%s': {%s} / %d rows", t.getTableId(), t.getPath(), StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","), t.getRows().size()));
		}
		if(tableNamesChanged) {
			extractedTables.reloadSchema();
			extractedTables.reloadRecords();
		}
		
		// load reference tables
		System.out.println("Loading reference tables");
		WebTables referenceTables = WebTables.loadWebTables(new File(referenceLocation), true, false, false, serialise, Q.max(extractedTables.getTables().keySet())+1);
		for(Table t : referenceTables.getTables().values()) {
			System.out.println(String.format("Table #%d '%s': {%s} / %d rows", t.getTableId(), t.getPath(), StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","), t.getRows().size()));
		}

		// deduplicate all tables
		deduplicateEntities(extractedTables);
		deduplicateEntities(referenceTables);
		
		int extractedEntities = 0;
		for(Table t : Q.where(extractedTables.getTables().values(), (t)->!t.getPath().contains("_fd_"))) {
			extractedEntities += t.getSize();
		}
		int referenceEntities = 0;
		for(Table t : Q.where(referenceTables.getTables().values(), (t)->!t.getPath().contains("fd"))) {
			referenceEntities += t.getSize();
		}

		// join entity and relation tables
		joinEntityTablesToRelationTables(extractedTables, false);
		joinEntityTablesToRelationTables(referenceTables, true);
		
		// remove PK & FK columns from all tables
		System.out.println("removing PK & FK columns");
		removePKAndFKColumns(extractedTables);
		removePKAndFKColumns(referenceTables);
		
		int extractedTuples = 0;
		for(Table t : extractedTables.getTables().values()) {
			extractedTuples += t.getSize();
		}
		int referenceTuples = 0;
		for(Table t : referenceTables.getTables().values()) {
			referenceTuples += t.getSize();
		}

		// determine column uniqueness for blocking
		Map<String, Double> extractedColumnUniqueness = new HashMap<>();
		for(Table t: extractedTables.getTables().values()) {
			for(TableColumn c : t.getColumns()) {				
				Double uniq = t.getColumnUniqueness().get(c);

				extractedColumnUniqueness.put(c.getIdentifier(), uniq);
			}
		}
		
		Map<Integer, String> tableNameById = new HashMap<>();
		for(Table t : extractedTables.getTables().values()) {
			tableNameById.put(t.getTableId(), t.getPath());
		}
		for(Table t : referenceTables.getTables().values()) {
			tableNameById.put(t.getTableId(), t.getPath().replace("reference_", ""));
		}

		// determine value overlap between all tables
		// (1) determine exact overlap: block by sorted row contents (all columns concatenated, keep duplicate values), all blocked pairs are matches
		// (2) determine containment: block by cell values, match if all cells from reference table are contained, one reference row can only be mapped once
		
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences = null;
		
		// while matching, make sure that the similarity score is equal to the absolute cell overlap, i.e., a score of 2 means that two cells match
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCors = null;
		if(!measureContainment) {
			// (1) determine exact overlap
			
			System.out.println("Matching records - exact");
			// run row-based matching
			StandardRecordBlocker<MatchableTableRow, MatchableTableColumn> blocker = new StandardRecordBlocker<>(new FullRowBlockingKeyGenerator());
			// blocker.setMeasureBlockSizes(true);
			correspondences = engine.runIdentityResolution(extractedTables.getRecords(), referenceTables.getRecords(), null, new CellOverlapMatchingRule(0.0, evaluateNulls), blocker);
		} else {
			// (2) determine containment
			System.out.println("Matching records - containment");
			
			if(labelsMatch) {
				schemaCors = new ProcessableCollection<>();
				for(Table t : extractedTables.getTables().values()) {
					for(Table ref : referenceTables.getTables().values()) {
						String tName = t.getPath().replace("reference_", "");
						String refName = ref.getPath().replace("reference_", "");
						//if(!filenamesMatch || t.getPath().equals(ref.getPath().replace("reference_", ""))) {
						if(!filenamesMatch || tName.equals(refName)) {
							System.out.println(String.format("Mapping extracted table #%d %s to reference table #%d %s",
								t.getTableId(),
								t.getPath(),
								ref.getTableId(),
								ref.getPath()
							));
							for(TableColumn c : t.getColumns()) {
								for(TableColumn refCol : ref.getColumns()) {
									if(c.getHeader().replace("_label", "").equals(refCol.getHeader())) {
										System.out.println(String.format("\tMapping '%s' to '%s'",
											c.toString(),
											refCol.toString()
										));
										schemaCors.add(new Correspondence<>(extractedTables.getSchema().getRecord(c.getIdentifier()), referenceTables.getSchema().getRecord(refCol.getIdentifier()), 1.0));
									}
								}
							}
						}
					}
				}
				System.out.println(String.format("Created %d schema correspondences based on matching column headers", schemaCors.size()));
			} else {
				// first run a schema matching to find columns for blocking
				BlockingKeyIndexer<MatchableTableRow, MatchableTableColumn, MatchableTableColumn, MatchableTableRow> schemaMatcher 
					= new BlockingKeyIndexer<MatchableTableRow, MatchableTableColumn, MatchableTableColumn, MatchableTableRow>(
							new TableCellColumnBlockingKeyGenerator()
							, new TableCellColumnBlockingKeyGenerator()
							, new VectorSpaceJaccardSimilarity()
							, VectorCreationMethod.BinaryTermOccurrences
							, 0.0
							);
				
				schemaCors = schemaMatcher.runBlocking(extractedTables.getRecords(), referenceTables.getRecords(), null);
				
				
				// choose the most unique column for each table for blocking
	//			schemaCors = engine.getTopKSchemaCorrespondences(schemaCors, 3, 0.0);
				schemaCors = filterByDensity(schemaCors, extractedTables.getTables());	// don't use columns with NULL values for blocking!
				schemaCors = applyUniquenessWeights(schemaCors, extractedTables.getTables());	
				schemaCors = chooseBlockingKeys(schemaCors);

				System.out.println("Blocking by:");
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCors
						.sort((c)->c.getFirstRecord().getTableId())
						.get()) {
					System.out.println(String.format("\t%f\t%s <-> %s", cor.getSimilarityScore(), cor.getFirstRecord(), cor.getSecondRecord()));
				}
			}
			
			// run row-based matching
			StandardRecordBlocker<MatchableTableRow, MatchableTableColumn> blocker = new StandardRecordBlocker<>(new TableCellBlockingKeyGenerator(filenamesMatch ? tableNameById : null));
			// blocker.setMeasureBlockSizes(true);
			correspondences = engine.runIdentityResolution(extractedTables.getRecords(), referenceTables.getRecords(), schemaCors, new CellOverlapMatchingRule(0.0, evaluateNulls), blocker);
		}
		
		Distribution<Integer> lhs = Correspondence.getLHSFrequencyDistribution(correspondences.get());
		Distribution<Integer> rhs = Correspondence.getRHSFrequencyDistribution(correspondences.get());

		System.out.println(String.format("Frequency distributions for correspondences: \n\tLHS: %s\n\tRHS: %s",
			lhs.formatCompact(),
			rhs.formatCompact()
		));

		System.out.println("Creating global instance matching");

		if(useTop1) {
			// top-K global matching could assign multiple rows to the same reference row
			// - which means multiple extracted rows are counted as correct for one reference row, which is wrong

			// create a top1 matching for each combination of tables: only one row in an extracted table can be mapped to a row in a reference table
			correspondences = engine.getTopKInstanceCorrespondences(correspondences, 1, 0.0);
		
		} else {
			GreedyOneToOneMatchingAlgorithm<MatchableTableRow, MatchableTableColumn> greedyOneToOne = new GreedyOneToOneMatchingAlgorithm<>(correspondences);
			greedyOneToOne.setGroupByLeftDataSource(true);
			greedyOneToOne.setGroupByRightDataSource(true);
			greedyOneToOne.run();
			correspondences = greedyOneToOne.getResult();
		}
		System.out.println(String.format("\t%d correspondences in global instance matching", correspondences.size()));

		lhs = Correspondence.getLHSFrequencyDistribution(correspondences.get());
		rhs = Correspondence.getRHSFrequencyDistribution(correspondences.get());

		System.out.println(String.format("Frequency distributions for correspondences: \n\tLHS: %s\n\tRHS: %s",
			lhs.formatCompact(),
			rhs.formatCompact()
		));
		
		printUnmapped(extractedTables, referenceTables, correspondences, schemaCors);
		
		// create table-level maximum matching: only one extracted table can be considered as result for each reference table
		System.out.println("Aggregating correspondences");
		Processable<Correspondence<MatchableTable, Matchable>> tableLevelCorrespondences = createTableLevelCorrespondences(correspondences, extractedTables, referenceTables);
		
		// get the cell counts per reference table
		System.out.println("Counting cells in reference tables");
		Processable<Pair<Integer, Double>> referenceCellCounts = referenceTables.getRecords()
				.aggregate(
						(MatchableTableRow record,DataIterator<Pair<Integer, Double>> resultCollector) 
							-> {
								int nonNullCells = 0;
								for(MatchableTableColumn c : record.getSchema()) {
									if(record.hasValue(c) || evaluateNulls) {
										nonNullCells++;
									}
								}
								resultCollector.next(new Pair<>(record.getTableId(), (double)nonNullCells));
							}, 
						new SumDoubleAggregator<>());
		for(Pair<Integer, Double> p : referenceCellCounts.get()) {
			System.out.println(String.format("\t#%d\t%.0f", p.getFirst(), p.getSecond()));
		}
		
		// get the cell count per extracted table
		System.out.println("Counting cells in extracted tables");
		Processable<Pair<Integer, Double>> extractedCellCounts = extractedTables.getRecords()
				.aggregate(
						(MatchableTableRow record,DataIterator<Pair<Integer, Double>> resultCollector) 
							-> {
								int nonNullCells = 0;
								for(MatchableTableColumn c : record.getSchema()) {
									if(record.hasValue(c) || evaluateNulls) {
										nonNullCells++;
									}
								}
								resultCollector.next(new Pair<>(record.getTableId(), (double)nonNullCells));
							}, 
						new SumDoubleAggregator<>());
		for(Pair<Integer, Double> p : extractedCellCounts.get()) {
			System.out.println(String.format("\t#%d\t%.0f", p.getFirst(), p.getSecond()));
		}

		// join table-level correspondences to cell counts and calculate per-table performance
		// perform left join on referenceCellCounts to make sure that all tables from the reference appear in the evaluation, even if they were not matched
		Processable<Pair<Pair<Integer, Integer>, Performance>> performancePerTable = referenceCellCounts
			.leftJoin(
					tableLevelCorrespondences, 
					(p)->p.getFirst(),
					(c)->c.getSecondRecord().getId())
			.leftJoin(
					extractedCellCounts, 
					(p)->p.getSecond()==null ? -1 : p.getSecond().getFirstRecord().getId(),
					(p)->p.getFirst())
			.map((Pair<Pair<Pair<Integer, Double>, Correspondence<MatchableTable, Matchable>>, Pair<Integer, Double>> record, DataIterator<Pair<Pair<Integer, Integer>, Performance>> resultCollector) 
					-> {
						// table-level correspondence similarity score is the number of matching cells between extracted relation and reference relation
						int correctCells = 0;
						if(record.getFirst().getSecond()!=null) {
							correctCells = (int)record.getFirst().getSecond().getSimilarityScore();
						}

						// get the total number of cells in the reference relation as maximum number of correctly extracted cells
						int maxCells = record.getFirst().getFirst().getSecond().intValue();

						// get the total number of cells in the extracted relation
						int mappedCells = 0;
						if(record.getSecond()!=null) {
							mappedCells = record.getSecond().getSecond().intValue();
						}
						
						// calculate the performance
						Performance p = new Performance(correctCells, mappedCells, maxCells);
						int tableId = record.getFirst().getFirst().getFirst();
						int extractedTableId;
						
						if(record.getFirst().getSecond()!=null) {
							extractedTableId = record.getFirst().getSecond().getFirstRecord().getId();
						} else {
							extractedTableId = -1;
						}
						
						resultCollector.next(new Pair<>(new Pair<>(extractedTableId, tableId), p));
					});
		
		
		// run maximum weight matching
		tableLevelCorrespondences = createTableLevelCorrespondencesFromPerformance(performancePerTable);
		for(Correspondence<MatchableTable, Matchable> cor : tableLevelCorrespondences.get()) {
			System.out.println(String.format("%.6f\t[%d] %s <-> [%d] %s", 
					cor.getSimilarityScore(),
					cor.getFirstRecord().getId(),
					cor.getFirstRecord().getId() == -1 ? "-1" : extractedTables.getTables().get(cor.getFirstRecord().getId()).getPath(),
					cor.getSecondRecord().getId(),
					referenceTables.getTables().get(cor.getSecondRecord().getId()).getPath()
					));
		}
		
		System.out.println("Running maximum-weight matching");
		MaximumBipartiteMatchingAlgorithm<MatchableTable, Matchable> maximumMatching = new MaximumBipartiteMatchingAlgorithm<>(tableLevelCorrespondences);
		maximumMatching.run();
		tableLevelCorrespondences = maximumMatching.getResult();
		System.out.println(String.format("\t%d correspondences in maximum-weight matching", tableLevelCorrespondences.size()));
		
		Map<Integer, Integer> tableMappingRefToExtracted = new HashMap<>();
		for(Correspondence<MatchableTable, Matchable> cor : tableLevelCorrespondences.get()) {
			System.out.println(String.format("%.6f\t[%d] %s <-> [%d] %s", 
					cor.getSimilarityScore(),
					cor.getFirstRecord().getId(),
					cor.getFirstRecord().getId() == -1 ? "-1" : extractedTables.getTables().get(cor.getFirstRecord().getId()).getPath(),
					cor.getSecondRecord().getId(),
					referenceTables.getTables().get(cor.getSecondRecord().getId()).getPath()
					));
			tableMappingRefToExtracted.put(cor.getSecondRecord().getId(), cor.getFirstRecord().getId());
		}
		
		performancePerTable = referenceCellCounts
				.leftJoin(
						performancePerTable
							.join(tableLevelCorrespondences,
									(p)->p.getFirst(),
									(c)->new Pair<>(c.getFirstRecord().getId(), c.getSecondRecord().getId()))
						,
						(p)->p.getFirst(), 
						(p)->p.getFirst().getFirst().getSecond())
				.map(
					(Pair<Pair<Integer, Double>, Pair<Pair<Pair<Integer, Integer>, Performance>, Correspondence<MatchableTable, Matchable>>> record, DataIterator<Pair<Pair<Integer, Integer>, Performance>> resultCollector) 
					-> {
						if(record.getSecond()!=null ) {
							resultCollector.next(record.getSecond().getFirst());
						} else {
							resultCollector.next(
									new Pair<Pair<Integer, Integer>, Performance>(
											new Pair<Integer, Integer>(
													-1, 
													record.getFirst().getFirst()), 
											new Performance(0, 0, record.getFirst().getSecond().intValue())
									));
						}
					});
				
		
		// aggregate cell counts for all extracted tables
		Processable<Pair<Object, Double>> allExtractedCells = extractedCellCounts
				.aggregate(
						(record, collector)->collector.next(new Pair<>(0,record.getSecond())),
				new SumDoubleAggregator<>());
		
		int totalExtractedCells = allExtractedCells.firstOrNull().getSecond().intValue();
		
		// this code calculates the micro-average of all performances
		// - which is problematic, because the more errors are made during extraction (more extracted cells), the higher the weight in the overall evaluation
		// aggregate number of correctly mapped cells
		// Processable<Pair<Object, Performance>> aggregatedPerformances = performancePerTable
		// 	.aggregate(
		// 			(record, collector)->collector.next(new Pair<>(0, record.getSecond())),
		// 			new PerformanceAggregator<>());
		
		// Performance aggregatedPerformance = aggregatedPerformances.firstOrNull().getSecond();

		// aggregatedPerformance = new Performance(aggregatedPerformance.getNumberOfCorrectlyPredicted(), totalExtractedCells, aggregatedPerformance.getNumberOfCorrectTotal());

		// alternative: weighted macro average
		// - each table's performance is weighted by the size of the reference table, so the weight is independent of the extraction result
		double aggregatedPrecision=0.0, aggregatedRecall=0.0, totalWeightP=0.0, totalWeightR=0.0;
		for(Pair<Pair<Integer,Integer>,Performance> p : performancePerTable.get()) {
			if(p.getSecond().getNumberOfPredicted()>0) {
				aggregatedPrecision += p.getSecond().getPrecision() * p.getSecond().getNumberOfCorrectTotal();
				totalWeightP += p.getSecond().getNumberOfCorrectTotal();
			}
			aggregatedRecall += p.getSecond().getRecall() * p.getSecond().getNumberOfCorrectTotal();
			totalWeightR += p.getSecond().getNumberOfCorrectTotal();
		}
		Performance aggregatedPerformance = new IRPerformance(aggregatedPrecision/totalWeightP, aggregatedRecall/totalWeightR);

		// print performance
		Map<Integer, Table> refTables = referenceTables.getTables();
		
		System.out.println("*********************************************************");
		System.out.println("******* Evaluation Results ******************************");
		System.out.println("*********************************************************");
		
		System.out.println("Overall Performance:");
		// System.out.println(String.format("\tCreated cells: %,d\n\tCorrect cells: %,d\n\tTotal cells in gs: %,d",aggregatedPerformance.getNumberOfPredicted(), aggregatedPerformance.getNumberOfCorrectTotal(), aggregatedPerformance.getNumberOfCorrectTotal()));
		System.out.println(String.format("\tPrecision: %.6f\n\tRecall: %.6f\n\tF1-measure: %.6f", aggregatedPerformance.getPrecision(), aggregatedPerformance.getRecall(), aggregatedPerformance.getF1()));

		if(globalLogLocation!=null) {
			File tbls = new File(tablesLocation);
			BufferedWriter w = new BufferedWriter(new FileWriter(globalLogLocation, true));
			// w.write(String.format("%s\t%s\t%.6f\t%.6f\t%.6f\n", 
			w.write(String.format("%s\n", StringUtils.join(new String[] {
				tbls.getParentFile().getName(),
				tbls.getName(),
				Double.toString(aggregatedPerformance.getPrecision()),								// Precision
				Double.toString(aggregatedPerformance.getRecall()),									// Recall
				Double.toString(aggregatedPerformance.getF1()),										// F1-measure
				Integer.toString(extractedEntities),												// extracted entities
				Integer.toString(referenceEntities),												// reference entities
				Integer.toString(extractedTuples),													// extracted tuples
				Integer.toString(referenceTuples)													// reference tuples
				// Integer.toString(aggregatedPerformance.getNumberOfPredicted()),						// extracted cells
				// Integer.toString(aggregatedPerformance.getNumberOfCorrectTotal())					// reference cells
			}, "\t")));
			w.close();
		}
		
		if(performancePerTable!=null) {
			System.out.println("Performance per Table:");
			for(Pair<Pair<Integer, Integer>, Performance> p : performancePerTable.get()) {
				Table t = refTables.get(p.getFirst().getSecond());
				
				Table mapped = extractedTables.getTables().get(tableMappingRefToExtracted.get(t.getTableId()));
				
				if(mapped!=null) {
					System.out.println(String.format("%s (%s)", t.getPath(), mapped.getPath()));
				} else {
					System.out.println(t.getPath());
				}
				System.out.println(String.format("\t{%s}", StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
				System.out.println(String.format("\t%d cells", p.getSecond().getNumberOfCorrectTotal()));
				System.out.println(String.format("\tCreated cells: %,d\n\tCorrect cells: %,d\n\tTotal cells in gs: %,d",p.getSecond().getNumberOfPredicted(), p.getSecond().getNumberOfCorrectlyPredicted(), p.getSecond().getNumberOfCorrectTotal()));
				System.out.println(String.format("\tPrecision: %.6f\n\tRecall: %.6f\n\tF1-measure: %.6f", p.getSecond().getPrecision(), p.getSecond().getRecall(), p.getSecond().getF1()));
			}

			if(globalLogLocation!=null) {
				File tbls = new File(tablesLocation);
				BufferedWriter w = new BufferedWriter(new FileWriter(globalLogLocation + "_details.tsv", true));
				for(Pair<Pair<Integer, Integer>, Performance> p : performancePerTable.get()) {
					Table t = refTables.get(p.getFirst().getSecond());	
					Table mapped = extractedTables.getTables().get(tableMappingRefToExtracted.get(t.getTableId()));
					Performance perf = p.getSecond();

					w.write(String.format("%s\n", StringUtils.join(new String[] {
						tbls.getParentFile().getName(),
						tbls.getName(),
						t.getPath().contains("_fd_") ? "relation" : "entity",
						t.getPath(),															// reference relation name
						Integer.toString(t.getSize()),											// reference relation size
						Integer.toString(t.getColumns().size()),								// number of attributes in reference relation
						mapped==null ? "not mapped" : mapped.getPath(),							// extracted relation name
						mapped==null ? "0" : Integer.toString(mapped.getSize()),				// extracted relation size
						mapped==null ? "0" : Integer.toString(mapped.getColumns().size()),		// number of attributes in extracted relation
						Integer.toString(perf.getNumberOfCorrectlyPredicted()),					// correct 
						Integer.toString(perf.getNumberOfPredicted()),							// extracted
						Integer.toString(perf.getNumberOfCorrectTotal()),						// weight (number of cells in reference relation)
						Double.toString(perf.getPrecision()),									// precision
						Double.toString(perf.getRecall()),										// recall
						Double.toString(perf.getF1())											// F1
					}, "\t")));

				}
				w.close();
			}
		}

		System.out.println("done.");
	}
	
	protected Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> createOneToOneRowMapping(Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences) {

		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> result = new ParallelProcessableCollection<>();

		// remove all correspondences for rows which only have a single correspondence

		// group by the left rows
		Processable<Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> groupedByLeft = correspondences
			.group((cor,col)->col.next(new Pair<>(cor.getFirstRecord().getIdentifier(), cor)));
		// get all correspondences where the left row has only one correspondant
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> leftOne = groupedByLeft
			.map((group, col)->{
				if(group.getRecords().size()==1) {
					col.next(group.getRecords().firstOrNull());
				}
			});
		System.out.println(String.format("%d LHS rows with 1 match", leftOne.size()));

		// group by the right rows
		Processable<Group<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> groupedByRight = correspondences
			.group((cor,col)->col.next(new Pair<>(cor.getSecondRecord().getIdentifier(), cor)));
		// get all correspondences where the right row has only one correspondant
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> rightOne = groupedByRight
			.map((group, col)->{
				if(group.getRecords().size()==1) {
					col.next(group.getRecords().firstOrNull());
				}
			});
		System.out.println(String.format("%d RHS rows with 1 match", rightOne.size()));
		
		// join the correspondences to the identifier correspondences with only one correspondant
		Processable<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>,Boolean>> filtered = correspondences
			.leftJoin(leftOne, (c)->c)
			.leftJoin(rightOne, (p)->p.getFirst(), (c)->c)
			.map((join, col) -> {
				// if the correspondences is in both other correspondence sets, there is no alternative mapping for either of the rows
				if(join.getSecond()!=null && join.getFirst().getSecond()!=null) {
					col.next(new Pair<>(join.getFirst().getFirst(), true));
				} else {
					col.next(new Pair<>(join.getFirst().getFirst(), false));
				}
			});

		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> skipped = filtered.map((p,col)->{ if(p.getSecond()) col.next(p.getFirst()); });

		System.out.println(String.format("%d/%d correspondences assigned uniquely", skipped.size(), correspondences.size()));

		// add all correspondences without alternatives to the result
		result = result.append(skipped);

		// run a maximum weight matching on the remaining correspondences
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> multiple = filtered.map((p,col)->{ if(!p.getSecond()) col.next(p.getFirst()); });

		MaximumBipartiteMatchingAlgorithm<MatchableTableRow, MatchableTableColumn> maxWeight = new MaximumBipartiteMatchingAlgorithm<>(multiple);
		maxWeight.setGroupByLeftDataSource(true);
		maxWeight.setGroupByRightDataSource(true);
		maxWeight.run();
		correspondences = maxWeight.getResult();
		result = result.append(correspondences);

		System.out.println(String.format("\t%d correspondences in max. weight instance matching", result.size()));

		return result;
	}
	protected void printUnmapped(WebTables extractedTables, WebTables referenceTables, Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		Map<String, MatchableTableRow> mappedLeft = new HashMap<>();
		Map<String, MatchableTableRow> mappedRight = new HashMap<>();
		for(MatchableTableRow r : extractedTables.getRecords().get()) {
			mappedLeft.put(r.getIdentifier(), r);
		}
		for(MatchableTableRow r : referenceTables.getRecords().get()) {
			mappedRight.put(r.getIdentifier(), r);
		}
		for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : correspondences.get()) {
			mappedLeft.remove(cor.getFirstRecord().getIdentifier());
			mappedRight.remove(cor.getSecondRecord().getIdentifier());
		}
		
		if(mappedLeft.size()>0) {
			Map<Integer, Collection<MatchableTableRow>> groups = Q.group(mappedLeft.values(), (r)->r.getTableId());
			for(Integer tableId : groups.keySet()) {
				Collection<MatchableTableRow> groupRows = groups.get(tableId);
				System.out.println(String.format("\tTable #%d: %d unmapped records", tableId, groupRows.size()));
				for(MatchableTableRow r : Q.take(groupRows, 5)) {
					System.out.println(String.format("\t\t%s", r.format(20)));
				}
			}
		}
		if(mappedRight.size()>0) {
			Map<Integer, Collection<MatchableTableRow>> groups = Q.group(mappedRight.values(), (r)->r.getTableId());
			for(Integer tableId : groups.keySet()) {
				Collection<MatchableTableRow> groupRows = groups.get(tableId);
				System.out.println(String.format("\tTable #%d: %d unmapped records", tableId, groupRows.size()));
				for(MatchableTableRow r : Q.take(groupRows, 5)) {
					System.out.println(String.format("\t\t%s", r.format(20)));
				}
			}
		}

		System.out.println("Correspondence examples:");
		int idx = 0;
		// CellOverlapMatchingRule r = new CellOverlapMatchingRule(0.0, false);
		for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : correspondences.where(
			(c)->{
				// return c.getSimilarityScore()<c.getFirstRecord().getSchema().length;
				int valuesLeft = 0;
				for(MatchableTableColumn col : c.getFirstRecord().getSchema()) {
					if(c.getFirstRecord().hasValue(col)) {
						valuesLeft++;
					}
				}
				int valuesRight = 0;
				for(MatchableTableColumn col : c.getSecondRecord().getSchema()) {
					if(c.getSecondRecord().hasValue(col)) {
						valuesRight++;
					}
				}
				return c.getSimilarityScore() < (double)Math.min(valuesLeft, valuesRight);
			}).take(5).get()) {
			System.out.println(String.format("[%d]\t%f\t%s",idx, cor.getSimilarityScore(), cor.getFirstRecord().formatSchema(20)));
			System.out.println(String.format("[%d]\t%f\t%s",idx, cor.getSimilarityScore(), cor.getFirstRecord().format(20)));
			System.out.println(String.format("[%d]\t%f\t%s",idx, cor.getSimilarityScore(), cor.getSecondRecord().formatSchema(20)));
			System.out.println(String.format("[%d]\t%f\t%s",idx, cor.getSimilarityScore(), cor.getSecondRecord().format(20)));
			idx++;
			// Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCors = schemaCorrespondences.where((c)->c.getFirstRecord().getTableId()==cor.getFirstRecord().getTableId() && c.getSecondRecord().getTableId()==cor.getSecondRecord().getTableId());
			// r.apply(cor.getFirstRecord(), cor.getSecondRecord(), Correspondence.toMatchable(schemaCors));
		}
	}
	
	protected void joinEntityTablesToRelationTables(WebTables web, boolean isReference) throws Exception {
		Collection<Table> entityTables = new LinkedList<>();
		for(Table t : web.getTables().values()) {
			if(t.getPath().contains("_rel_") || t.getPath().contains("_fd_")) {
				String entityTableName = t.getPath().replaceAll("_rel_\\d+", "").replaceAll("_fd_\\d+", "").replace("reference_","");
				Integer entityTableId = web.getTableIndices().get(entityTableName);
				if(entityTableId==null) {
					System.out.println(String.format("Could not find entity table for '%s'", t.getPath()));
				} else {
					Table entityTable = web.getTables().get(entityTableId);
					
					TableColumn pk = Q.firstOrDefault(Q.where(entityTable.getColumns(), (c)->"PK".equalsIgnoreCase(c.getHeader())));
					TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equalsIgnoreCase(c.getHeader())));
					
					if(fk!=null) {
						Table joined = t.join(entityTable, Q.toList(new Pair<>(fk, pk)), Q.union(Q.without(entityTable.getColumns(), Q.toList(pk)), Q.without(t.getColumns(), Q.toList(fk))));
						joined.setTableId(t.getTableId());
						
						System.out.println(String.format("joining relation table #%d %s {%s} with entity table #%d %s to {%s} / %d rows", 
								t.getTableId(),
								t.getPath(),
								StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
								entityTable.getTableId(),
								entityTable.getPath(),
								StringUtils.join(Q.project(joined.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
								t.getRows().size()));
						
						web.getTables().put(t.getTableId(), joined);
						
						if(joinLocation!=null) {
							CSVTableWriter w = new CSVTableWriter();
							w.write(joined, new File(new File(joinLocation), String.format("%s_%s", entityTable.getPath(), t.getPath())));
						}
					} else {
						System.out.println(String.format("No foreign key found in table #%d %s", t.getTableId(), t.getPath()));
					}
				}
			} else {
				entityTables.add(t);
			}
		}

		if(isReference) {
			// rename the reference entity tables, otherwise their IDs will be the same as the extracted entity tables
			for(Table t : entityTables) {
				t.setPath("reference_" + t.getPath());
			}
		}
		
		web.reloadSchema();
		web.reloadRecords();
	}
	
	protected void removePKAndFKColumns(WebTables web) {
		for(Table t : web.getTables().values()) {
			Collection<TableColumn> toRemove = new LinkedList<>();
			for(TableColumn c: t.getColumns()) {
				if("pk".equals(c.getHeader()) || "fk".equals(c.getHeader())) {
					toRemove.add(c);
				}
			}
			for(TableColumn c : toRemove) {
				t.removeColumn(c);
			}
			t.getSchema().getCandidateKeys().clear();
		}
		web.reloadSchema();
		web.reloadRecords();
	}
	
	protected void deduplicateEntities(WebTables web) {
		for(Table t : web.getTables().values()) {
			int before = t.getRows().size();
			Collection<Pair<TableRow, TableRow>> duplicates = t.deduplicate(Q.without(t.getColumns(), Q.where(t.getColumns(), (c)->"PK".equalsIgnoreCase(c.getHeader()))), ConflictHandling.KeepBoth, false);
			if(duplicates.size()>0) {
				System.out.println(String.format("Table %s (%d rows) -> %d duplicates detected:", t.getPath(), before, duplicates.size()));
				int i = 1;
				for(Pair<TableRow, TableRow> duplicate : duplicates) {
					TableRow r1 = duplicate.getFirst();
					TableRow r2 = duplicate.getSecond();
					System.out.println(String.format("\t%d\t%s", i, r1.format(30)));
					System.out.println(String.format("\t%d\t%s", i, r2.format(30)));
					i++;
				}
				t.getSchema().getCandidateKeys().clear();
			}
		}
		web.reloadRecords();
	}
	
	protected void deduplicate(WebTables web) {
		for(Table t : web.getTables().values()) {
			int before = t.getRows().size();
			Collection<Pair<TableRow, TableRow>> duplicates = t.deduplicate(t.getColumns());
			if(duplicates.size()>0) {
				System.out.println(String.format("Table %s (%d rows) -> %d rows after deduplication", t.getPath(), before, t.getRows().size()));
				int i = 1;
				for(Pair<TableRow, TableRow> duplicate : duplicates) {
					System.out.println(String.format("\t%d\t%s", i, duplicate.getFirst().format(20)));
					System.out.println(String.format("\t%d\t%s", i, duplicate.getSecond().format(20)));
					i++;
				}
				t.getSchema().getCandidateKeys().clear();
			}
		}
		web.reloadRecords();
	}

	protected Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> filterByDensity(Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences, Map<Integer, Table> tables) {

		// multiply similarity values with column uniqueness to avoid having a high score for columns with only one single value
		// calculate the uniqueness for each column
		Processable<Pair<String, Double>> columnDensity = new ParallelProcessableCollection<>(tables.values())
			.map((Table record, DataIterator<Pair<String, Double>> resultCollector) 
				-> {
					Map<TableColumn, Double> den = record.getColumnDensities();
					for(TableColumn c : den.keySet()) {
						Double density = den.get(c);
						resultCollector.next(new Pair<>(c.getIdentifier(), density));
					}
				});
		
		correspondences = correspondences
			.join(columnDensity, (c)->c.getFirstRecord().getIdentifier(), (p)->p.getFirst())
			.map((Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<String, Double>> record, DataIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) 
				-> {
					Correspondence<MatchableTableColumn, MatchableTableRow> cor = record.getFirst();
					Double density = record.getSecond().getSecond();
					
					if(!(density<1.0)) {
						resultCollector.next(cor);
					}
				});
		
		return correspondences;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> applyUniquenessWeights(Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences, Map<Integer, Table> tables) {

		// multiply similarity values with column uniqueness to avoid having a high score for columns with only one single value
		// calculate the uniqueness for each column
		Processable<Pair<String, Double>> columnUniqueness = new ParallelProcessableCollection<>(tables.values())
			.map((Table record, DataIterator<Pair<String, Double>> resultCollector) 
				-> {
					Map<TableColumn, Double> uniq = record.getColumnUniqueness();
					for(TableColumn c : uniq.keySet()) {
						Double uniqueness = uniq.get(c);
						resultCollector.next(new Pair<>(c.getIdentifier(), uniqueness));
					}
				});
		
		correspondences = correspondences
			.join(columnUniqueness, (c)->c.getFirstRecord().getIdentifier(), (p)->p.getFirst())
			.map((Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<String, Double>> record, DataIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) 
				-> {
					Correspondence<MatchableTableColumn, MatchableTableRow> cor = record.getFirst();
					Double uniqueness = record.getSecond().getSecond();
					
					cor.setsimilarityScore(cor.getSimilarityScore() * uniqueness);
					resultCollector.next(cor);
				});
		
		return correspondences;
	}

	protected Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> chooseBlockingKeys(Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences) {

		return correspondences
			.aggregate(
				(Correspondence<MatchableTableColumn, MatchableTableRow> record, DataIterator<Pair<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(record.getFirstRecord().getTableId(), record));
				}
			, new TopKAggregator<>(1))
			.map(
				(Pair<Integer, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>>> record, DataIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) 
				-> {
					resultCollector.next(record.getSecond().firstOrNull());
				}
			);		
	}
	
	protected Processable<Correspondence<MatchableTable, Matchable>> createTableLevelCorrespondences(Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences, WebTables extractedTables, WebTables referenceTables) {
		Processable<Correspondence<MatchableTable, Matchable>> tableLevelCorrespondences = correspondences
		// map the row-level correspondences to table-level correspondences
		.map(
			(Correspondence<MatchableTableRow, MatchableTableColumn> record, DataIterator<Correspondence<MatchableTable, Matchable>> resultCollector) 
				-> {
					MatchableTable t1 = new MatchableTable(record.getFirstRecord().getTableId(), "");
					MatchableTable t2 = new MatchableTable(record.getSecondRecord().getTableId(), "");
					resultCollector.next(new Correspondence<>(t1, t2, record.getSimilarityScore()));
				})
		// aggregate the table-level correspondences by table combination
		.aggregate(
			(Correspondence<MatchableTable, Matchable> record,DataIterator<Pair<Pair<MatchableTable, MatchableTable>, Correspondence<MatchableTable, Matchable>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord(), record.getSecondRecord()), record));
				}
			, new CorrespondenceAggregator<>(0.0))
		// return the aggregated correspondences
		.map(
			(Pair<Pair<MatchableTable, MatchableTable>, Correspondence<MatchableTable, Matchable>> record,DataIterator<Correspondence<MatchableTable, Matchable>> resultCollector) 
			-> {
				resultCollector.next(record.getSecond());
			});
		
		System.out.println(String.format("\t%d table-level correspondences", tableLevelCorrespondences.size()));
		for(Correspondence<MatchableTable, Matchable> cor : tableLevelCorrespondences.sort((c)->c.getSecondRecord().getId()).get()) {
			System.out.println(String.format("%.6f\t[%d] %s <-> [%d] %s", 
					cor.getSimilarityScore(),
					cor.getFirstRecord().getId(),
					extractedTables.getTables().get(cor.getFirstRecord().getId()).getPath(),
					cor.getSecondRecord().getId(),
					referenceTables.getTables().get(cor.getSecondRecord().getId()).getPath()
					));
		}

		Processable<Correspondence<MatchableTable, Matchable>> tableLevelCorrespondencesCounts = correspondences
		// map the row-level correspondences to table-level correspondences
		.map(
			(Correspondence<MatchableTableRow, MatchableTableColumn> record, DataIterator<Correspondence<MatchableTable, Matchable>> resultCollector) 
				-> {
					MatchableTable t1 = new MatchableTable(record.getFirstRecord().getTableId(), "");
					MatchableTable t2 = new MatchableTable(record.getSecondRecord().getTableId(), "");
					resultCollector.next(new Correspondence<>(t1, t2, 1.0));
				})
		// aggregate the table-level correspondences by table combination
		.aggregate(
			(Correspondence<MatchableTable, Matchable> record,DataIterator<Pair<Pair<MatchableTable, MatchableTable>, Correspondence<MatchableTable, Matchable>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord(), record.getSecondRecord()), record));
				}
			, new CorrespondenceAggregator<>(0.0))
		// return the aggregated correspondences
		.map(
			(Pair<Pair<MatchableTable, MatchableTable>, Correspondence<MatchableTable, Matchable>> record,DataIterator<Correspondence<MatchableTable, Matchable>> resultCollector) 
			-> {
				resultCollector.next(record.getSecond());
			});
		
			System.out.println(String.format("\t%d table-level correspondences (Counts)", tableLevelCorrespondencesCounts.size()));
			for(Correspondence<MatchableTable, Matchable> cor : tableLevelCorrespondencesCounts.sort((c)->c.getSecondRecord().getId()).get()) {
				System.out.println(String.format("%.6f\t[%d] %s <-> [%d] %s", 
						cor.getSimilarityScore(),
						cor.getFirstRecord().getId(),
						extractedTables.getTables().get(cor.getFirstRecord().getId()).getPath(),
						cor.getSecondRecord().getId(),
						referenceTables.getTables().get(cor.getSecondRecord().getId()).getPath()
						));
			}

		return tableLevelCorrespondences;
	}
	
	protected Processable<Correspondence<MatchableTable, Matchable>> createTableLevelCorrespondencesFromPerformance(Processable<Pair<Pair<Integer, Integer>, Performance>> performancePerTable) {
		
		return performancePerTable
			.map(
				(Pair<Pair<Integer, Integer>, Performance> record, DataIterator<Correspondence<MatchableTable, Matchable>> resultCollector) 
				->{
					resultCollector.next(new Correspondence<MatchableTable, Matchable>(new MatchableTable(record.getFirst().getFirst(), ""), new MatchableTable(record.getFirst().getSecond(), ""), record.getSecond().getF1()));
				}
			);
		
	}
}
