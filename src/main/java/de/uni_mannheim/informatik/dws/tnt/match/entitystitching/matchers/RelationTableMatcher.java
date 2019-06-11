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
package de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.snow.ImprovedBlockingKeyIndexer;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableEntity;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableCell;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableDataSetLoader;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.CustomizableValueBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.ImprovedValueBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.TableToTableMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.preprocessing.FilenameFilter;
import de.uni_mannheim.informatik.dws.tnt.match.preprocessing.MinLengthFilter;
import de.uni_mannheim.informatik.dws.tnt.match.preprocessing.NumberFilter;
import de.uni_mannheim.informatik.dws.tnt.match.preprocessing.PatternCleaner;
import de.uni_mannheim.informatik.dws.tnt.match.preprocessing.TimeExtractor;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.CorrespondenceBasedCellComparator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.CorrespondenceBasedEntityComparator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.CorrespondenceBasedRowComparator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.RuleBasedMatchingAlgorithmExtended;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.LodTableRowLabelTokenGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.LodTableRowTokenGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.MatchableTableCellTokenisingBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.MatchableTableRowLabelTokenisingCellBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.MatchableTableRowTokenToEntityBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.TableRowTokenGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.blocking.TypedBasedBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.duplicatebased.RelationTableToKBVotingRule;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.duplicatebased.RelationTableVotingRule;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.CorrespondenceAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.TopKVotesAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.VotingAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.GreedyOneToOneMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.MaximumBipartiteMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.VectorSpaceInstanceBasedSchemaMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.InstanceBasedBlockingKeyIndexer;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardRecordBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardSchemaBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.DocumentFrequencyCounter;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.VectorCreationMethod;
import de.uni_mannheim.informatik.dws.winter.matching.rules.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.CountAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SumDoubleAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.UnadjustedDeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.MaximumOfTokenContainment;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceCosineSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceJaccardSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceMaximumOfContainmentSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.Table.ConflictHandling;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class RelationTableMatcher {

	private KnowledgeBase kb;

	public void loadKnowledgeBase(File entityDefinitionLocation, boolean useSurfaceForms) throws IOException {
		if(useSurfaceForms) {
			SurfaceForms sf = new SurfaceForms(new File(entityDefinitionLocation, "surfaceforms"), new File(entityDefinitionLocation, "redirects"));
			sf.loadSurfaceForms();
			kb = KnowledgeBase.loadKnowledgeBase(new File(entityDefinitionLocation, "tables/"), null, sf, true, true);
		} else {
			kb = KnowledgeBase.loadKnowledgeBase(new File(entityDefinitionLocation, "tables/"), null, null, true, true);
		}
		kb.loadCandidateKeys(new File(entityDefinitionLocation, "keys/"));	
	}

	public Processable<Pair<String, Double>> calculateIDF() {
		BlockingKeyIndexer<MatchableTableRow, MatchableTableColumn, MatchableTableColumn, MatchableValue> idx = new BlockingKeyIndexer<>(
				new LodTableRowLabelTokenGenerator(), new LodTableRowLabelTokenGenerator(),
				new VectorSpaceCosineSimilarity(), VectorCreationMethod.TFIDF, 0.0);

		Processable<Pair<String, Double>> idf = idx.calculateInverseDocumentFrequencies(kb.getRecords(),
				new LodTableRowLabelTokenGenerator());

		return idf;
	}

	public void writeIDF(Processable<Pair<String, Double>> idf, File file) throws IOException {
		BufferedWriter w = new BufferedWriter(new FileWriter(file));

		for (Pair<String, Double> p : idf.get()) {
			w.write(String.format("%s\t%f\n", p.getFirst(), p.getSecond()));
		}

		w.close();
	}

	public Processable<Pair<String, Double>> readIDF(File file) throws NumberFormatException, IOException {
		BufferedReader r = new BufferedReader(new FileReader(file));
		String line = null;
		Processable<Pair<String, Double>> idf = new ParallelProcessableCollection<>();

		while ((line = r.readLine()) != null) {
			String[] values = line.split("\t");

			if (values.length > 1) {
				Pair<String, Double> p = new Pair<>(values[0], Double.parseDouble(values[1]));
				idf.add(p);
			}
		}

		return idf;
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchRelationTableColumnsToClasses(
			Collection<Table> relationTables, Processable<Pair<String, Double>> idf) throws Exception {

		WebTableDataSetLoader loader = new WebTableDataSetLoader();

		DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(relationTables);

		BlockingKeyIndexer<MatchableTableRow, MatchableTableColumn, MatchableTableColumn, MatchableValue> blocker = new BlockingKeyIndexer<>(
				new TableRowTokenGenerator(), new LodTableRowLabelTokenGenerator(), new VectorSpaceCosineSimilarity(),
				VectorCreationMethod.TFIDF, 0.01);

		if (idf != null) {
			blocker.setDocumentFrequencyCounter(DocumentFrequencyCounter.Preset);
			blocker.setInverseDocumentFrequencies(idf);
		}

		Processable<Correspondence<MatchableTableColumn, MatchableValue>> correspondences = blocker.runBlocking(records,
				kb.getRecords(), null);

		// VectorSpaceInstanceBasedSchemaMatchingAlgorithm<MatchableTableRow,
		// MatchableTableColumn> algorithm = new
		// VectorSpaceInstanceBasedSchemaMatchingAlgorithm<>(
		// records,
		// kb.getRecords(),
		// new TableRowTokenGenerator(),
		// new LodTableRowLabelTokenGenerator(),
		// VectorCreationMethod.TFIDF,
		// new VectorSpaceCosineSimilarity(),
		// 0.01);

		// algorithm.run();

		// Processable<Correspondence<MatchableTableColumn, MatchableValue>>
		// correspondences = algorithm.getResult();

		// MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new
		// MatchingEngine<>();

		// Processable<Correspondence<MatchableTableColumn, MatchableValue>>
		// correspondences = engine.runInstanceBasedSchemaMatching(
		// records,
		// kb.getRecords(),
		// new TableRowTokenGenerator(),
		// new LodTableRowLabelTokenGenerator(),
		// // VectorCreationMethod.BinaryTermOccurrences,
		// VectorCreationMethod.TFIDF,
		// // new VectorSpaceMaximumOfContainmentSimilarity(),
		// // new VectorSpaceJaccardSimilarity(),
		// new VectorSpaceCosineSimilarity(),
		// // new GeneralisedMaximumOfContainmentSimilarity(),
		// 0.01);

		// MatchingEngine<MatchableTableColumn, MatchableValue> e2 = new
		// MatchingEngine<>();
		// correspondences = e2.getTopKInstanceCorrespondences(correspondences, 1, 0.0);

		return Correspondence.toMatchable(correspondences);
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchRelationTableColumnsToClassesByEntitiesViaIndexing(
			Collection<Table> relationTables, 
			Processable<Pair<String, Double>> idf,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			double indexingSimThreshold,
			double simThreshold,
			SimilarityMeasure<String> measure,
			String patternLocation,
			String logLocation) throws Exception {

		PatternCleaner cleaner = null;
		if(patternLocation!=null) {
			cleaner = new PatternCleaner();
			cleaner.loadPatterns(new File(patternLocation));
			for(Table t : relationTables) {
				cleaner.cleanTable(t);
			}
		}

		WebTableDataSetLoader loader = new WebTableDataSetLoader();

		DataSet<MatchableTableCell, MatchableTableColumn> cells = loader.loadCellDataSet(relationTables, true);

		
		ImprovedBlockingKeyIndexer<MatchableTableCell, MatchableTableColumn, MatchableTableCell, MatchableTableColumn> blocker = new ImprovedBlockingKeyIndexer<>(
				new MatchableTableCellTokenisingBlockingKeyGenerator(true, false), 
				new MatchableTableCellTokenisingBlockingKeyGenerator(false, true), 
				// new VectorSpaceCosineSimilarity(),
				new VectorSpaceJaccardSimilarity(),
				// ImprovedBlockingKeyIndexer.VectorCreationMethod.TFIDF, 
				ImprovedBlockingKeyIndexer.VectorCreationMethod.BinaryTermOccurrences, 
				indexingSimThreshold);
		// blocker.setRecalculateScores(true);
		blocker.setBlockFilterRatio(0.95);
		blocker.setMaxBlockPairSize(1000000);
		blocker.collectBlockSizeData(logLocation, -1);

		if (idf != null) {
			blocker.setDocumentFrequencyCounter(ImprovedBlockingKeyIndexer.DocumentFrequencyCounter.Preset);
			blocker.setInverseDocumentFrequencies(idf);
		}

		// Processable<Correspondence<MatchableTableCell, MatchableTableColumn>> correspondences = blocker.runBlocking(cells,
		// 		kb.getLabels(), null);

		LinearCombinationMatchingRule<MatchableTableCell, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(
			simThreshold);
		rule.addComparator(new CorrespondenceBasedCellComparator<String>(measure, kb), 1.0);

		RuleBasedMatchingAlgorithmExtended<MatchableTableCell, MatchableTableColumn, MatchableTableCell, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<>(
					cells, kb.getLabels(), schemaCorrespondences, rule, blocker);
			// RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
			// 		records, kb.getRecords(), schemaCorrespondences, rule, blocker);
			algorithm.setTaskName("Identity Resolution");
			algorithm.run();
			Processable<Correspondence<MatchableTableCell, MatchableTableColumn>> correspondences = algorithm
					.getResult();

		blocker.writeDebugBlockingResultsToFile();

		System.out.println(String.format("%d cell-level correspondences", correspondences.size()));

		GreedyOneToOneMatchingAlgorithm<MatchableTableCell, MatchableTableColumn> oneToOne = new GreedyOneToOneMatchingAlgorithm<>(
			correspondences);
		oneToOne.setGroupByLeftDataSource(true);
		oneToOne.setGroupByRightDataSource(true);
		oneToOne.run();
		correspondences = oneToOne.getResult();

		System.out.println(String.format("%d cell-level correspondences after 1:1", correspondences.size()));

		Processable<Pair<MatchableTableColumn, Integer>> aggregatedCells = cells
				.aggregate(new RecordKeyValueMapper<MatchableTableColumn, MatchableTableCell, Integer>() {

					@Override
					public void mapRecordToKey(MatchableTableCell record,
							DataIterator<Pair<MatchableTableColumn, Integer>> resultCollector) {
						resultCollector.next(new Pair<>(record.getColumn(), 1));
					}
				}, new CountAggregator<>());
		Map<MatchableTableColumn, Integer> entityCounts = Pair.toMap(aggregatedCells.get());

		Processable<Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Double>> aggregatedByColumns = correspondences
				.aggregate(
						new RecordKeyValueMapper<Pair<MatchableTableColumn, MatchableTableColumn>, Correspondence<MatchableTableCell, MatchableTableColumn>, Double>() {

							@Override
							public void mapRecordToKey(Correspondence<MatchableTableCell, MatchableTableColumn> record,
									DataIterator<Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Double>> resultCollector) {
								resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getColumn(),
										record.getSecondRecord().getColumn()), record.getSimilarityScore()));
										// record.getSecondRecord().getColumn()), 1.0));
							}
						}, new SumDoubleAggregator<>());

		// logging only
		Processable<Group<MatchableTableColumn, Correspondence<MatchableTableCell, MatchableTableColumn>>> groupedByColumn = correspondences
				.group(new RecordKeyValueMapper<MatchableTableColumn, Correspondence<MatchableTableCell, MatchableTableColumn>, Correspondence<MatchableTableCell, MatchableTableColumn>>() {

					@Override
					public void mapRecordToKey(Correspondence<MatchableTableCell, MatchableTableColumn> record,
							DataIterator<Pair<MatchableTableColumn, Correspondence<MatchableTableCell, MatchableTableColumn>>> resultCollector) {
						resultCollector.next(new Pair<>(record.getFirstRecord().getColumn(), record));
					}
				});

		for(Group<MatchableTableColumn, Correspondence<MatchableTableCell, MatchableTableColumn>> colGrp : groupedByColumn.get()) {

			Processable<Correspondence<MatchableTableCell, MatchableTableColumn>> columnCors = colGrp.getRecords();
			int entities = entityCounts.get(colGrp.getKey());

			System.out.println(String.format("Found %d correspondences for %s '%s' with %d entities", columnCors.size(), colGrp.getKey().getIdentifier(), colGrp.getKey().getHeader(), entities));
			Processable<Pair<Integer, Double>> classDistribution = columnCors.aggregate((cor,col)->col.next(new Pair<>(cor.getSecondRecord().getDataSourceIdentifier(), cor.getSimilarityScore())), new SumDoubleAggregator<>());

			for(Pair<Integer, Double> p : classDistribution.get()) {
				Processable<Correspondence<MatchableTableCell, MatchableTableColumn>> cors = columnCors
						.where((c0) -> c0.getSecondRecord().getDataSourceIdentifier() == p.getFirst());
					
				System.out.println(String.format("\t%s\t%f\tcor: %d\tent: %d", kb.getClassIndices().get(p.getFirst()), p.getSecond(), cors.size(), entityCounts.get(colGrp.getKey())));

				for(Correspondence<MatchableTableCell, MatchableTableColumn> cor : cors.take(10).get()) {
					System.out.println(String.format("\t\t%s\t<->\t%s '%s' (%f)", cor.getFirstRecord().formatValue(), cor.getSecondRecord().getIdentifier(), cor.getSecondRecord().formatValue(), cor.getSimilarityScore()));
				}
			}

			Processable<Correspondence<MatchableTableColumn, Matchable>> aggregatedCors = aggregatedByColumns.map((p)-> {
				return new Correspondence<>(p.getFirst().getFirst(), p.getFirst().getSecond(), p.getSecond().intValue() / (double)entities);
			});
			for(Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Double> p : aggregatedByColumns.where((p0)->p0.getFirst().getFirst().getIdentifier().equals(colGrp.getKey().getIdentifier())).sort((p0)->p0.getSecond(), false).get()) {
				
				System.out.println(String.format("\t==> %s sum: %f normalised: %f", p.getFirst().getSecond().getIdentifier(), p.getSecond(), p.getSecond() / (double)entities));
			}
		}

		System.out.println(String.format("Discovered %d schema correspondences", aggregatedByColumns.size()));

		Processable<Correspondence<MatchableTableColumn, Matchable>> columnCors = aggregatedByColumns.map((p)-> {
			int entities = entityCounts.get(p.getFirst().getFirst());
			return new Correspondence<>(p.getFirst().getFirst(), p.getFirst().getSecond(), p.getSecond().intValue() / (double)entities);
		});

		return columnCors;
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchRelationTableColumnsToClassesByEntitiesViaIndexingPerTable(
			Collection<Table> relationTables, 
			Processable<Pair<String, Double>> idf,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			double indexingSimThreshold,
			double simThreshold,
			SimilarityMeasure<String> measure,
			String patternLocation,
			String logLocation) throws Exception {

		PatternCleaner cleaner = null;
		if(patternLocation!=null) {
			cleaner = new PatternCleaner();
			cleaner.loadPatterns(new File(patternLocation));
			for(Table t : relationTables) {
				cleaner.cleanTable(t);
			}
		}

		Processable<Correspondence<MatchableTableColumn, Matchable>> result = new ParallelProcessableCollection<>();

		ImprovedBlockingKeyIndexer<MatchableTableCell, MatchableTableColumn, MatchableTableCell, MatchableTableColumn> blocker = new ImprovedBlockingKeyIndexer<>(
			new MatchableTableCellTokenisingBlockingKeyGenerator(true, false), 
			new MatchableTableCellTokenisingBlockingKeyGenerator(false, true), 
			// new VectorSpaceCosineSimilarity(),
			new VectorSpaceJaccardSimilarity(),
			// ImprovedBlockingKeyIndexer.VectorCreationMethod.TFIDF, 
			ImprovedBlockingKeyIndexer.VectorCreationMethod.BinaryTermOccurrences, 
			indexingSimThreshold);
		// blocker.setRecalculateScores(true);
		blocker.setBlockFilterRatio(0.95);
		blocker.setMaxBlockPairSize(1000000);
		// blocker.collectBlockSizeData(logLocation, -1);
		blocker.setCacheBlocks(true);
		if (idf != null) {
			blocker.setDocumentFrequencyCounter(ImprovedBlockingKeyIndexer.DocumentFrequencyCounter.Preset);
			blocker.setInverseDocumentFrequencies(idf);
		}

		for(Table t : relationTables) {
			System.out.println(String.format("Refining table %s", t.getPath()));

			WebTableDataSetLoader loader = new WebTableDataSetLoader();

			DataSet<MatchableTableCell, MatchableTableColumn> cells = loader.loadCellDataSet(Q.toList(t), true);

			blocker.resetCachedBlocks(true, false);

			Processable<Correspondence<MatchableTableCell, MatchableTableColumn>> correspondences = blocker.runBlocking(cells,
					kb.getLabels(), null);

			// LinearCombinationMatchingRule<MatchableTableCell, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(
			// 	simThreshold);
			// rule.addComparator(new CorrespondenceBasedCellComparator<String>(measure, kb), 1.0);

			// RuleBasedMatchingAlgorithmExtended<MatchableTableCell, MatchableTableColumn, MatchableTableCell, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<>(
			// 			cells, kb.getLabels(), schemaCorrespondences, rule, blocker);
			// 	algorithm.setTaskName("Identity Resolution");
			// 	algorithm.run();
			// 	Processable<Correspondence<MatchableTableCell, MatchableTableColumn>> correspondences = algorithm
			// 			.getResult();

			// blocker.writeDebugBlockingResultsToFile();

			System.out.println(String.format("%d cell-level correspondences", correspondences.size()));

			GreedyOneToOneMatchingAlgorithm<MatchableTableCell, MatchableTableColumn> oneToOne = new GreedyOneToOneMatchingAlgorithm<>(
				correspondences);
			oneToOne.setGroupByLeftDataSource(true);
			oneToOne.setGroupByRightDataSource(true);
			oneToOne.run();
			correspondences = oneToOne.getResult();

			System.out.println(String.format("%d cell-level correspondences after 1:1", correspondences.size()));

			Processable<Pair<MatchableTableColumn, Integer>> aggregatedCells = cells
					.aggregate(new RecordKeyValueMapper<MatchableTableColumn, MatchableTableCell, Integer>() {

						@Override
						public void mapRecordToKey(MatchableTableCell record,
								DataIterator<Pair<MatchableTableColumn, Integer>> resultCollector) {
							resultCollector.next(new Pair<>(record.getColumn(), 1));
						}
					}, new CountAggregator<>());
			Map<MatchableTableColumn, Integer> entityCounts = Pair.toMap(aggregatedCells.get());

			Processable<Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Double>> aggregatedByColumns = correspondences
					.aggregate(
							new RecordKeyValueMapper<Pair<MatchableTableColumn, MatchableTableColumn>, Correspondence<MatchableTableCell, MatchableTableColumn>, Double>() {

								@Override
								public void mapRecordToKey(Correspondence<MatchableTableCell, MatchableTableColumn> record,
										DataIterator<Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Double>> resultCollector) {
									resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getColumn(),
											record.getSecondRecord().getColumn()), record.getSimilarityScore()));
											// record.getSecondRecord().getColumn()), 1.0));
								}
							}, new SumDoubleAggregator<>());

			// logging only
			Processable<Group<MatchableTableColumn, Correspondence<MatchableTableCell, MatchableTableColumn>>> groupedByColumn = correspondences
					.group(new RecordKeyValueMapper<MatchableTableColumn, Correspondence<MatchableTableCell, MatchableTableColumn>, Correspondence<MatchableTableCell, MatchableTableColumn>>() {

						@Override
						public void mapRecordToKey(Correspondence<MatchableTableCell, MatchableTableColumn> record,
								DataIterator<Pair<MatchableTableColumn, Correspondence<MatchableTableCell, MatchableTableColumn>>> resultCollector) {
							resultCollector.next(new Pair<>(record.getFirstRecord().getColumn(), record));
						}
					});

			for(Group<MatchableTableColumn, Correspondence<MatchableTableCell, MatchableTableColumn>> colGrp : groupedByColumn.get()) {

				Processable<Correspondence<MatchableTableCell, MatchableTableColumn>> columnCors = colGrp.getRecords();
				int entities = entityCounts.get(colGrp.getKey());

				System.out.println(String.format("Found %d correspondences for %s '%s' with %d entities", columnCors.size(), colGrp.getKey().getIdentifier(), colGrp.getKey().getHeader(), entities));
				Processable<Pair<Integer, Double>> classDistribution = columnCors.aggregate((cor,col)->col.next(new Pair<>(cor.getSecondRecord().getDataSourceIdentifier(), cor.getSimilarityScore())), new SumDoubleAggregator<>());

				for(Pair<Integer, Double> p : classDistribution.get()) {
					Processable<Correspondence<MatchableTableCell, MatchableTableColumn>> cors = columnCors
							.where((c0) -> c0.getSecondRecord().getDataSourceIdentifier() == p.getFirst());
						
					System.out.println(String.format("\t%s\t%f\tcor: %d\tent: %d", kb.getClassIndices().get(p.getFirst()), p.getSecond(), cors.size(), entityCounts.get(colGrp.getKey())));

					for(Correspondence<MatchableTableCell, MatchableTableColumn> cor : cors.take(10).get()) {
						System.out.println(String.format("\t\t%s\t<->\t%s '%s' (%f)", cor.getFirstRecord().formatValue(), cor.getSecondRecord().getIdentifier(), cor.getSecondRecord().formatValue(), cor.getSimilarityScore()));
					}
				}

				Processable<Correspondence<MatchableTableColumn, Matchable>> aggregatedCors = aggregatedByColumns.map((p)-> {
					return new Correspondence<>(p.getFirst().getFirst(), p.getFirst().getSecond(), p.getSecond().intValue() / (double)entities);
				});
				for(Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Double> p : aggregatedByColumns.where((p0)->p0.getFirst().getFirst().getIdentifier().equals(colGrp.getKey().getIdentifier())).sort((p0)->p0.getSecond(), false).get()) {
					
					System.out.println(String.format("\t==> %s sum: %f normalised: %f", p.getFirst().getSecond().getIdentifier(), p.getSecond(), p.getSecond() / (double)entities));
				}
			}

			System.out.println(String.format("Discovered %d schema correspondences", aggregatedByColumns.size()));

			Processable<Correspondence<MatchableTableColumn, Matchable>> columnCors = aggregatedByColumns.map((p)-> {
				int entities = entityCounts.get(p.getFirst().getFirst());
				return new Correspondence<>(p.getFirst().getFirst(), p.getFirst().getSecond(), p.getSecond().intValue() / (double)entities);
			});

			result = result.append(columnCors);
		}

		return result;
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> tuneMatchRelationTableColumnsToClassesByEntities(
			Collection<Table> relationTables,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			MatchingGoldStandard trainingSet,
			String logLocation,
			String patternLocation) throws Exception {

		Processable<Correspondence<MatchableTableColumn, Matchable>> best_result = null;

		if(trainingSet!=null) {

			Collection<SimilarityMeasure<String>> measures = new LinkedList<>();
			measures.add(new TokenizingJaccardSimilarity());
			measures.add(new MaximumOfTokenContainment());
			StringBuilder sb = new StringBuilder();
			BufferedWriter w = new BufferedWriter(new FileWriter(new File(logLocation + "_configurations.tsv")));

			String best_config = "";
			double best_f1 = 0.0;
			// for(SimilarityMeasure<String> measure : measures) {
				SimilarityMeasure<String> measure = new TokenizingJaccardSimilarity();
				// for(int i = 0; i < 10; i++) {
					// double simThreshold = i / 10.0;
					double simThreshold = 0.4;
					// for(boolean usePatterns : new boolean[] { true, false }) {
						boolean usePatterns = true;
						// for(int minEnt = 0; minEnt < 6; minEnt++) {
						for(int minEntities : new int[] { 3, 5, 10 }) {
							for(double finalThreshold : new double[] { 0.0, 0.1, 0.15, 0.2, 0.25, 0.3}) {
								// double minMappedEntityPercentage = minEnt / 10.0;
								double minMappedEntityPercentage = 0.0;
								
								Processable<Correspondence<MatchableTableColumn, Matchable>> result = matchRelationTableColumnsToClassesByEntities(relationTables, schemaCorrespondences, simThreshold, measure, usePatterns ? patternLocation : null, minMappedEntityPercentage, minEntities, finalThreshold, null, null);

								MatchingEvaluator<MatchableTableColumn, Matchable> evaluator = new MatchingEvaluator<>();
								Set<String> mappedColumns = new HashSet<>();
								for(Pair<String, String> p : trainingSet.getPositiveExamples()) {
									mappedColumns.add(p.getFirst());
								}

								Set<String> containsCorrect = new HashSet<>();
								for(Correspondence<MatchableTableColumn, Matchable> cor : result.get()) {
									if(trainingSet.containsPositive(cor.getFirstRecord(), cor.getSecondRecord())) {
										containsCorrect.add(cor.getFirstRecord().getIdentifier());
									}
								}

								String config = String.format("t:%.1f-%s-pat:%b-ent:%.1f-ent_abs:%d-final_t:%.2f", 
									simThreshold, 
									measure.getClass().getSimpleName(), 
									usePatterns, 
									minMappedEntityPercentage,
									minEntities,
									finalThreshold);

								evaluator.writeEvaluation(new File(logLocation + "_evaluation_all_" + config + ".csv"), result, trainingSet);
								MatchingEngine<MatchableTableColumn, Matchable> engine = new MatchingEngine<>();
								result = engine.getTopKInstanceCorrespondences(result, 1, 0.0);            
								evaluator.writeEvaluation(new File(logLocation + "_evaluation_" + config + ".csv"), result, trainingSet);

								Performance perf = evaluator.evaluateMatching(result, trainingSet);
								double recall = perf.getNumberOfCorrectlyPredicted() / (double)mappedColumns.size();
								double f1 = (2*perf.getPrecision()*recall)/(perf.getPrecision()+recall);
								sb.append("\nConfig " + config + "\n");
								sb.append(String.format("Precision: %f\nRecall: %f\nF1: %f\n", perf.getPrecision(), recall, f1));
								sb.append(String.format("Max Recall is %f\n", containsCorrect.size() / (double)mappedColumns.size()));
								w.write(String.format("%s\n", StringUtils.join(new String[] {
									config,
									Double.toString(perf.getPrecision()),
									Double.toString(recall),
									Double.toString(f1)
								}, "\t")));

								if(f1>best_f1) {
									best_f1 = f1;
									best_result = result;
									best_config = config;
								}
							}
						}
					// }
				// }
			// }
		
			sb.append(String.format("\nBest configuration is '%s' with F1: %f\n", best_config, best_f1));
			System.out.println(sb.toString());
		} else {
			best_result = matchRelationTableColumnsToClassesByEntities(relationTables, schemaCorrespondences, 0.4, new TokenizingJaccardSimilarity(), patternLocation, 0.0, 0, 0.0, null, null);
		}
		
		return best_result;
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchRelationTableColumnsToClassesByEntities(
			Collection<Table> relationTables,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			double simThreshold,
			SimilarityMeasure<String> measure,
			String patternLocation,
			double minMappedEntityPercentage,
			int minMappedEntities,
			double finalThreshold,
			File resultLocation,
			String gsLocation) throws Exception {

		// matches once per table (for multiple columns)
		// -> so far the fastest working solution...
		BufferedWriter w = null;
		BufferedWriter wAll = null;
		BufferedWriter wTop = null;
		BufferedWriter wCombination = null;
		if(resultLocation!=null) {
			w = new BufferedWriter(new FileWriter(resultLocation));
			wAll = new BufferedWriter(new FileWriter(resultLocation + "_all.tsv"));
			wTop = new BufferedWriter(new FileWriter(resultLocation + "_top1.tsv"));
			wCombination = new BufferedWriter(new FileWriter(resultLocation + "_class_combinations.tsv"));

			String header = String.format("%s\n", StringUtils.join(new String[] {
				"Column Id",
				"Class Id",
				"Similarity",
				"Table Rows",
				"Table Columns",
				"Table Class",
				"Column Header",
				"Is Context",
				"Key Size",
				"Mapped Entities",
				"Total Entities"
			}, "\t"));
			w.write(header);
			wAll.write(header);
			wTop.write(header);
		}

		
		MatchingGoldStandard gs = null;
		if(gsLocation!=null) {
			gs = new MatchingGoldStandard();
			gs.loadFromCSVFile(new File(gsLocation));
		}

		Processable<Correspondence<MatchableTableColumn, Matchable>> result = new ParallelProcessableCollection<>();
		WebTableDataSetLoader loader = new WebTableDataSetLoader();

		PatternCleaner cleaner = null;
		if(patternLocation!=null) {
			cleaner = new PatternCleaner();
			cleaner.loadPatterns(new File(patternLocation));
		}
		// TimeExtractor timeExtractor = new TimeExtractor();
		MinLengthFilter minLengthFilter = new MinLengthFilter(3);
		NumberFilter numberFilter = new NumberFilter();
		FilenameFilter filenameFilter = new FilenameFilter();


		LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(
			simThreshold);
		// rule.addComparator(new CorrespondenceBasedEntityComparator<String>(new
		// TokenizingJaccardSimilarity()), 1.0);
		// rule.addComparator(new CorrespondenceBasedEntityComparator<String>(new MaximumOfTokenContainment()), 1.0);
		rule.addComparator(new CorrespondenceBasedEntityComparator<String>(measure, kb), 1.0);

		Set<Pair<String, MatchableTableColumn>> blockedColumsRight = new HashSet<>();
		for (MatchableTableColumn c : kb.getSchema().get()) {
			if(KnowledgeBase.RDFS_LABEL.equals(c.getHeader())) {
				blockedColumsRight.add(
					new Pair<>("class_" + c.getTableId() + "-", c));
			}
		}

		MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator bkg = new MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator(
			kb.getCandidateKeys().get(), new HashMap<>());
		bkg.setMatchPartialKeys(true);
		MatchableTableRowTokenToEntityBlockingKeyGenerator bkg2 = new MatchableTableRowTokenToEntityBlockingKeyGenerator(
				blockedColumsRight);
		StandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> blocker = new StandardBlocker<>(
				bkg, bkg2);
		blocker.setBlockFilterRatio(0.95);
		blocker.setMaxBlockPairSize(1000000);
		blocker.setCacheBlocks(true);
		// blocker.setMeasureBlockSizes(true);

		int tblIdx = 1;
		for (Table t : relationTables) {

			System.out.println(String.format("Processing table %d/%d %s", tblIdx++, relationTables.size(), t.getPath()));

			if(cleaner!=null) {
				cleaner.cleanTable(t);
			}
			// Map<TableColumn, Integer> timeOccurrences = timeExtractor.cleanTable(t);
			// for(TableColumn c : timeOccurrences.keySet()) {
			// 	Integer cnt = timeOccurrences.get(c);
			// 	if(cnt>0) {
			// 		System.out.println(String.format("%d time mentions in column %s '%s'", cnt, c.getIdentifier(), c.getHeader()));
			// 	}
			// }	
			// filenameFilter.cleanTable(t);
			// numberFilter.cleanTable(t);
			minLengthFilter.cleanTable(t);
			// }
			DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(Q.toList(t));
			// DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(relationTables);

			// run identity resolution for the table

			// blocker.resetCache(true, false);

			// RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
			// 		records, kb.getRecords(), corsForColumn, rule, blocker);
			// RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
			// 		records, kb.getRecords(), schemaCorrespondences, rule, blocker);
			// algorithm.setTaskName("Identity Resolution");
			// algorithm.run();
			// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> entityCors = algorithm
			// 		.getResult();

			// for (TableColumn c : t.getColumns()) {

			// 	// aggregate entity scores for each column
			// 	// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCors = entityCors.where((cor)-> Q.any(cor.getFirstRecord().getEntityKey(), (mc)->mc.getIdentifier().equals(c.getIdentifier())));

			// 	Processable<Correspondence<MatchableTableColumn, Matchable>> corsForColumn = schemaCorrespondences
			// 			.where((cor) -> cor.getFirstRecord().getIdentifier().equals(c.getIdentifier()));

			// 	if (corsForColumn.size() > 0) {

					// 		System.out.println(String.format("Refining %s", c.getIdentifier()));
					// 		for (Correspondence<MatchableTableColumn, Matchable> cor : corsForColumn.get()) {
					// 			System.out.println(String.format("->%s\t%f", cor.getSecondRecord().getIdentifier(),
					// 					cor.getSimilarityScore()));
					// 		}

					// Set<Pair<String, MatchableTableColumn>> blockedColumsLeft = new HashSet<>();
					// // Set<Pair<String, MatchableTableColumn>> blockedColumsRight = new HashSet<>();
					// for (Correspondence<MatchableTableColumn, Matchable> cor : corsForColumn.get()) {
					// 	blockedColumsLeft.add(new Pair<>(null, cor.getFirstRecord()));
					// 	// blockedColumsRight.add(
					// 	// 		new Pair<>("class_" + cor.getSecondRecord().getTableId() + "-", cor.getSecondRecord()));
					// }

					// System.out.println(String.format("Blocking by %d candidate keys", kb.getCandidateKeys().size()));
					// for(MatchableTableDeterminant d : kb.getCandidateKeys().get()) {
					// 	System.out.println(
					// 		String.format("\t%s: {%s}", 
					// 			kb.getClassIndices().get(d.getTableId()), 
					// 			StringUtils.join(Q.project(d.getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ",")
					// 	));
					// }

					// MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator bkg = new MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator(
					// 	kb.getCandidateKeys().get(), new HashMap<>());
					// bkg.setMatchPartialKeys(true);
					// MatchableTableRowTokenToEntityBlockingKeyGenerator bkg2 = new MatchableTableRowTokenToEntityBlockingKeyGenerator(
					// 		blockedColumsRight);
					// StandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> blocker = new StandardBlocker<>(
					// 		bkg, bkg2);
					// blocker.setBlockFilterRatio(0.95);
					// blocker.setMaxBlockPairSize(1000000);
					// // blocker.setMeasureBlockSizes(true);


					bkg.getCreatedEntitiesPerClass().clear();
					blocker.resetCache(true, false);

					// RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
					// 		records, kb.getRecords(), corsForColumn, rule, blocker);
					RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
							records, kb.getRecords(), schemaCorrespondences, rule, blocker);
					algorithm.setTaskName("Identity Resolution");
					algorithm.run();
					Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCors = algorithm
							.getResult();

					// // create class distribution
					// Distribution<String> classDist = Distribution.fromCollection(instanceCors.map((cor)->kb.getClassIndices().get(cor.getSecondRecord().getTableId())).get());
					// System.out.println(classDist.formatCompact());

					// group instance correspondences by entity
					Processable<Group<MatchableEntity, MatchableEntity>> grouped = instanceCors.group(
							new RecordKeyValueMapper<MatchableEntity, Correspondence<MatchableEntity, MatchableTableColumn>, MatchableEntity>() {

								@Override
								public void mapRecordToKey(Correspondence<MatchableEntity, MatchableTableColumn> record,
										DataIterator<Pair<MatchableEntity, MatchableEntity>> resultCollector) {
									resultCollector.next(new Pair<MatchableEntity, MatchableEntity>(
											record.getFirstRecord(), record.getSecondRecord()));
								}
							});

					System.out.println(String.format("Found correspondences for %d entities", grouped.size()));
					

					// for(Group<MatchableEntity, MatchableEntity> g : grouped.get()) {
					// 	System.out.println(String.format("Found %d matches for entity '%s'", g.getRecords().size(), g.getKey().getIdentifier()));
					// }

					Processable<Group<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>>> groupedByColumn = instanceCors.group(
							new RecordKeyValueMapper<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>, Correspondence<MatchableEntity, MatchableTableColumn>>() {

								@Override
								public void mapRecordToKey(Correspondence<MatchableEntity, MatchableTableColumn> record,
										DataIterator<Pair<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>>> resultCollector) {
									for(MatchableTableColumn col : record.getFirstRecord().getEntityKey()) {
										resultCollector.next(new Pair<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>>(
											col, record));
									}
								}
							});
					for(Group<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>> g : groupedByColumn.get()) {
						System.out.println(String.format("Found %d correspondences for column %s", g.getRecords().size(), g.getKey().getIdentifier()));
					}

					// for (TableColumn c : t.getColumns()) {
					for(Group<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>> g : groupedByColumn.get()) {

						MatchableTableColumn c = g.getKey();
					// 	// aggregate entity scores for each column
					// 	// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCors = entityCors.where((cor)-> Q.any(cor.getFirstRecord().getEntityKey(), (mc)->mc.getIdentifier().equals(c.getIdentifier())));
		
						Processable<Correspondence<MatchableTableColumn, Matchable>> corsForColumn = schemaCorrespondences
								.where((cor) -> cor.getFirstRecord().getIdentifier().equals(c.getIdentifier()));
		
						if (corsForColumn.size() > 0) {
		
							System.out.println(String.format("Refining %s '%s'", c.getIdentifier(), c.getHeader()));
							for (Correspondence<MatchableTableColumn, Matchable> cor : corsForColumn.get()) {
								System.out.println(String.format("->%s\t%f", cor.getSecondRecord().getIdentifier(),
										cor.getSimilarityScore()));
							}

							Processable<Correspondence<MatchableEntity, MatchableTableColumn>> columnCors = g.getRecords();
							// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> columnCors = instanceCors;
							// create class distribution
							Distribution<String> classDist = Distribution.fromCollection(columnCors.map((cor)->kb.getClassIndices().get(cor.getSecondRecord().getTableId())).get());
							System.out.println(classDist.formatCompact());
							// create entity distribution
							// Distribution<String> entityDist = Distribution.fromCollection(columnCors.map((cor)->cor.getSecondRecord().getIdentifier()).get());
							// System.out.println(entityDist.formatCompact());

							GreedyOneToOneMatchingAlgorithm<MatchableEntity, MatchableTableColumn> oneToOne = new GreedyOneToOneMatchingAlgorithm<>(columnCors);
							oneToOne.setGroupByLeftDataSource(true);
							oneToOne.setGroupByRightDataSource(true);
							oneToOne.run();
							columnCors = oneToOne.getResult();
							// MatchingEngine<MatchableEntity, MatchableTableColumn> engine = new MatchingEngine<>();
							// instanceCors = engine.getTopKInstanceCorrespondences(instanceCors, 1, 0.0);
							System.out.println(String.format("%d instance correspondences after 1:1", columnCors.size()));

							List<String> classCombination = new LinkedList<>();
							String tblClass = null;
							Pair<String, Double> cls = t.getMapping().getMappedClass();
							if(cls!=null) {
								tblClass = cls.getFirst();
							} else {
								tblClass = t.getPath().split("\\_")[0];
							}
							// classCombination.add(tblClass);

							if(columnCors.size()>0) {

								Processable<Pair<Integer, Double>> classDistribution = columnCors.aggregate((cor,col)->col.next(new Pair<>(cor.getSecondRecord().getDataSourceIdentifier(), cor.getSimilarityScore())), new SumDoubleAggregator<>());

								for(Pair<Integer, Double> p : classDistribution.get()) {
									Processable<Correspondence<MatchableEntity, MatchableTableColumn>> cors = columnCors
											.where((c0) -> c0.getSecondRecord().getDataSourceIdentifier() == p.getFirst());
									Processable<Group<MatchableEntity, MatchableEntity>> entities = cors.group(
											new RecordKeyValueMapper<MatchableEntity, Correspondence<MatchableEntity, MatchableTableColumn>, MatchableEntity>() {

												@Override
												public void mapRecordToKey(
														Correspondence<MatchableEntity, MatchableTableColumn> record,
														DataIterator<Pair<MatchableEntity, MatchableEntity>> resultCollector) {
													resultCollector.next(new Pair<MatchableEntity, MatchableEntity>(
															record.getFirstRecord(), record.getSecondRecord()));
												}
											});
										
									System.out.println(String.format("\t%s\t%f\tcor: %d\tent: %d", kb.getClassIndices().get(p.getFirst()), p.getSecond(), cors.size(), entities.size()));

									for(Correspondence<MatchableEntity, MatchableTableColumn> cor : cors.take(10).get()) {
										System.out.println(String.format("\t\t%s\t<->\t%s (%f)", cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier(), cor.getSimilarityScore()));
									}
								}

								// Pair<Integer, Double> maxClass = Q.max(classDistribution.get(), (p)->p.getSecond());
								// MatchableTableColumn matchableKBCol = schemaCorrespondences.where((cor)->cor.getSecondRecord().getDataSourceIdentifier()==maxClass.getFirst()).firstOrNull().getSecondRecord();

								String best_details = null;
								double best_score = Double.MIN_VALUE;
								String best_class = null;
								for(Pair<Integer, Double> p : classDistribution.get()) {
									MatchableTableColumn matchableWTCol = records.getSchema().getRecord(c.getIdentifier());
									MatchableTableColumn matchableKBCol = schemaCorrespondences.where((cor)->cor.getSecondRecord().getDataSourceIdentifier()==p.getFirst()).firstOrNull().getSecondRecord();

									Processable<Correspondence<MatchableEntity, MatchableTableColumn>> cors = columnCors
											.where((c0) -> c0.getSecondRecord().getDataSourceIdentifier() == p.getFirst());
									Processable<Group<MatchableEntity, MatchableEntity>> entities = cors.group(
											new RecordKeyValueMapper<MatchableEntity, Correspondence<MatchableEntity, MatchableTableColumn>, MatchableEntity>() {

												@Override
												public void mapRecordToKey(
														Correspondence<MatchableEntity, MatchableTableColumn> record,
														DataIterator<Pair<MatchableEntity, MatchableEntity>> resultCollector) {
													resultCollector.next(new Pair<MatchableEntity, MatchableEntity>(
															record.getFirstRecord(), record.getSecondRecord()));
												}
											});

									// normalise score
									// divide by number of unique entities for the corresponding class (in the web table)
									int correspondences = cors.size();
									int mappedEntities = entities.size();
									// int entityCount = bkg.getCreatedEntitiesPerClass().get(p.getFirst()).size();
									int entityCount = bkg.getCreatedEntitiesPerColumn().get(String.format("%d-%s",p.getFirst(), c.getIdentifier())).size();
									double summedScore = (double)p.getSecond();
									// double score = (double)p.getSecond() / (double)entityCount;
									double score = summedScore / (double)entityCount;
									double mappedPercentage = mappedEntities / (double)entityCount;
									System.out.println(String.format("%s: %d/%d entities mapped (%.2f%%) by %d correspondences with total score of %f -> normalised: %f", 
										matchableKBCol.getIdentifier(), 
										mappedEntities,
										entityCount, 
										mappedPercentage * 100, 
										correspondences,
										summedScore, 
										score));

									Set<TableColumn> key = Q.firstOrDefault(t.getSchema().getCandidateKeys());
									int keySize = 0;
									if(key!=null) {
										keySize = key.size();
									}

									Boolean correct = null;
									if(gs!=null) {
										if(gs.containsPositive(matchableWTCol, matchableKBCol)) {
											correct = true;
										} else {
											correct = false;
										}
									}

									String details = String.format("%s\n", StringUtils.join(new String[] {
										matchableWTCol.getIdentifier(),
										matchableKBCol.getIdentifier(),
										Double.toString(score),
										Integer.toString(t.getSize()),
										Integer.toString(t.getColumns().size()),
										tblClass,
										matchableWTCol.getHeader(),
										Boolean.toString(ContextColumns.isContextColumn(matchableWTCol)),
										Integer.toString(keySize),
										Integer.toString(mappedEntities),
										Integer.toString(entityCount),
										correct == null ? "" : correct.toString()
									}, "\t"));

									if(mappedPercentage >= minMappedEntityPercentage && score >= finalThreshold && mappedEntities >= minMappedEntities) {
										result.add(new Correspondence<MatchableTableColumn, Matchable>(matchableWTCol, matchableKBCol, score));

										if(w!=null) {
											w.write(details);
										}

										if(score>best_score) {
											best_details = details;
											best_score = score;
											best_class = matchableKBCol.getIdentifier();
										}
									}

									if(wAll!=null) {
										wAll.write(details);
									}
								}

								if(best_details!=null && wTop!=null) {
									wTop.write(best_details);
									classCombination.add(best_class);
								}
							}

							if(wCombination!=null) {
								wCombination.write(String.format("%s\t%s\t%s\n", t.getPath(), tblClass, StringUtils.join(classCombination, "\t")));
							}
						// }
					// }
				}

			}

			if(w!=null) {
				w.flush();
				wAll.flush();
				wTop.flush();
				wCombination.flush();
			}
		}

		if(w!=null) {
			w.close();
			wAll.close();
			wTop.close();
			wCombination.close();
		}

		return result;
	}

	protected void writeDetailedResults(Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences, WebTables web, File f) throws IOException {
        BufferedWriter w = new BufferedWriter(new FileWriter(f));

        try {

            w.write(String.format("%s\n", StringUtils.join(new String[] {
                "Column",
                "Class",
                "Score",
                "Table rows",
                "Table class",
                "Column header"
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

                w.write(String.format("%s\n", StringUtils.join(new String[] {
                    cor.getFirstRecord().getIdentifier(),
                    cor.getSecondRecord().getIdentifier(),
                    Double.toString(cor.getSimilarityScore()),
                    Integer.toString(t.getSize()),
                    tblClass,
                    cor.getFirstRecord().getHeader()
                }, "\t")));
            }

        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            w.close();
        }
    }

	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchRelationTableColumnsToClassesByEntitiesPerColumn(
			Collection<Table> relationTables,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			double simThreshold,
			SimilarityMeasure<String> measure,
			String patternLocation,
			double minMappedEntityPercentage) throws Exception {

		Processable<Correspondence<MatchableTableColumn, Matchable>> result = new ParallelProcessableCollection<>();
		WebTableDataSetLoader loader = new WebTableDataSetLoader();

		PatternCleaner cleaner = null;
		if(patternLocation!=null) {
			cleaner = new PatternCleaner();
			cleaner.loadPatterns(new File(patternLocation));
		}


		LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(
			simThreshold);
		// rule.addComparator(new CorrespondenceBasedEntityComparator<String>(new
		// TokenizingJaccardSimilarity()), 1.0);
		// rule.addComparator(new CorrespondenceBasedEntityComparator<String>(new MaximumOfTokenContainment()), 1.0);
		rule.addComparator(new CorrespondenceBasedEntityComparator<String>(measure, kb), 1.0);

		Set<Pair<String, MatchableTableColumn>> blockedColumsRight = new HashSet<>();
		for (MatchableTableColumn c : kb.getSchema().get()) {
			if(KnowledgeBase.RDFS_LABEL.equals(c.getHeader())) {
				blockedColumsRight.add(
					new Pair<>("class_" + c.getTableId() + "-", c));
			}
		}

		MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator bkg = new MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator(
			kb.getCandidateKeys().get(), new HashMap<>());
		bkg.setMatchPartialKeys(true);
		MatchableTableRowTokenToEntityBlockingKeyGenerator bkg2 = new MatchableTableRowTokenToEntityBlockingKeyGenerator(
				blockedColumsRight);
		StandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> blocker = new StandardBlocker<>(
				bkg, bkg2);
		blocker.setBlockFilterRatio(0.95);
		blocker.setMaxBlockPairSize(1000000);
		blocker.setCacheBlocks(true);
		// blocker.setMeasureBlockSizes(true);

		for (Table t : relationTables) {

			if(cleaner!=null) {
				cleaner.cleanTable(t);
			}
			// }
			DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(Q.toList(t));
			// DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(relationTables);

			// run identity resolution for the table

			// blocker.resetCache(true, false);

			// RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
			// 		records, kb.getRecords(), corsForColumn, rule, blocker);
			// RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
			// 		records, kb.getRecords(), schemaCorrespondences, rule, blocker);
			// algorithm.setTaskName("Identity Resolution");
			// algorithm.run();
			// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> entityCors = algorithm
			// 		.getResult();

			for (TableColumn c : t.getColumns()) {

				// aggregate entity scores for each column
				// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCors = entityCors.where((cor)-> Q.any(cor.getFirstRecord().getEntityKey(), (mc)->mc.getIdentifier().equals(c.getIdentifier())));

				Processable<Correspondence<MatchableTableColumn, Matchable>> corsForColumn = schemaCorrespondences
						.where((cor) -> cor.getFirstRecord().getIdentifier().equals(c.getIdentifier()));

				if (corsForColumn.size() > 0) {

					// 		System.out.println(String.format("Refining %s", c.getIdentifier()));
					// 		for (Correspondence<MatchableTableColumn, Matchable> cor : corsForColumn.get()) {
					// 			System.out.println(String.format("->%s\t%f", cor.getSecondRecord().getIdentifier(),
					// 					cor.getSimilarityScore()));
					// 		}

					// Set<Pair<String, MatchableTableColumn>> blockedColumsLeft = new HashSet<>();
					// // Set<Pair<String, MatchableTableColumn>> blockedColumsRight = new HashSet<>();
					// for (Correspondence<MatchableTableColumn, Matchable> cor : corsForColumn.get()) {
					// 	blockedColumsLeft.add(new Pair<>(null, cor.getFirstRecord()));
					// 	// blockedColumsRight.add(
					// 	// 		new Pair<>("class_" + cor.getSecondRecord().getTableId() + "-", cor.getSecondRecord()));
					// }

					// System.out.println(String.format("Blocking by %d candidate keys", kb.getCandidateKeys().size()));
					// for(MatchableTableDeterminant d : kb.getCandidateKeys().get()) {
					// 	System.out.println(
					// 		String.format("\t%s: {%s}", 
					// 			kb.getClassIndices().get(d.getTableId()), 
					// 			StringUtils.join(Q.project(d.getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ",")
					// 	));
					// }

					// MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator bkg = new MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator(
					// 	kb.getCandidateKeys().get(), new HashMap<>());
					// bkg.setMatchPartialKeys(true);
					// MatchableTableRowTokenToEntityBlockingKeyGenerator bkg2 = new MatchableTableRowTokenToEntityBlockingKeyGenerator(
					// 		blockedColumsRight);
					// StandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> blocker = new StandardBlocker<>(
					// 		bkg, bkg2);
					// blocker.setBlockFilterRatio(0.95);
					// blocker.setMaxBlockPairSize(1000000);
					// // blocker.setMeasureBlockSizes(true);


					bkg.getCreatedEntitiesPerClass().clear();
					blocker.resetCache(true, false);

					RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
							records, kb.getRecords(), corsForColumn, rule, blocker);
					// RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
					// 		records, kb.getRecords(), schemaCorrespondences, rule, blocker);
					algorithm.setTaskName("Identity Resolution");
					algorithm.run();
					Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCors = algorithm
							.getResult();

					// // create class distribution
					// Distribution<String> classDist = Distribution.fromCollection(instanceCors.map((cor)->kb.getClassIndices().get(cor.getSecondRecord().getTableId())).get());
					// System.out.println(classDist.formatCompact());

					// group instance correspondences by entity
					Processable<Group<MatchableEntity, MatchableEntity>> grouped = instanceCors.group(
							new RecordKeyValueMapper<MatchableEntity, Correspondence<MatchableEntity, MatchableTableColumn>, MatchableEntity>() {

								@Override
								public void mapRecordToKey(Correspondence<MatchableEntity, MatchableTableColumn> record,
										DataIterator<Pair<MatchableEntity, MatchableEntity>> resultCollector) {
									resultCollector.next(new Pair<MatchableEntity, MatchableEntity>(
											record.getFirstRecord(), record.getSecondRecord()));
								}
							});

					System.out.println(String.format("Found correspondences for %d entities", grouped.size()));
					

					// for(Group<MatchableEntity, MatchableEntity> g : grouped.get()) {
					// 	System.out.println(String.format("Found %d matches for entity '%s'", g.getRecords().size(), g.getKey().getIdentifier()));
					// }

					Processable<Group<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>>> groupedByColumn = instanceCors.group(
							new RecordKeyValueMapper<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>, Correspondence<MatchableEntity, MatchableTableColumn>>() {

								@Override
								public void mapRecordToKey(Correspondence<MatchableEntity, MatchableTableColumn> record,
										DataIterator<Pair<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>>> resultCollector) {
									for(MatchableTableColumn col : record.getFirstRecord().getEntityKey()) {
										resultCollector.next(new Pair<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>>(
											col, record));
									}
								}
							});
					for(Group<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>> g : groupedByColumn.get()) {
						System.out.println(String.format("Found %d correspondences for column %s", g.getRecords().size(), g.getKey().getIdentifier()));
					}

					// for (TableColumn c : t.getColumns()) {
					// for(Group<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>> g : groupedByColumn.get()) {

					// 	MatchableTableColumn c = g.getKey();
					// 	// aggregate entity scores for each column
					// 	// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCors = entityCors.where((cor)-> Q.any(cor.getFirstRecord().getEntityKey(), (mc)->mc.getIdentifier().equals(c.getIdentifier())));
		
					// 	Processable<Correspondence<MatchableTableColumn, Matchable>> corsForColumn = schemaCorrespondences
					// 			.where((cor) -> cor.getFirstRecord().getIdentifier().equals(c.getIdentifier()));
		
					// 	if (corsForColumn.size() > 0) {
		
							System.out.println(String.format("Refining %s", c.getIdentifier()));
							for (Correspondence<MatchableTableColumn, Matchable> cor : corsForColumn.get()) {
								System.out.println(String.format("->%s\t%f", cor.getSecondRecord().getIdentifier(),
										cor.getSimilarityScore()));
							}

							// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> columnCors = g.getRecords();
							Processable<Correspondence<MatchableEntity, MatchableTableColumn>> columnCors = instanceCors;
							// create class distribution
							Distribution<String> classDist = Distribution.fromCollection(columnCors.map((cor)->kb.getClassIndices().get(cor.getSecondRecord().getTableId())).get());
							System.out.println(classDist.formatCompact());
							// create entity distribution
							Distribution<String> entityDist = Distribution.fromCollection(columnCors.map((cor)->cor.getSecondRecord().getIdentifier()).get());
							System.out.println(entityDist.formatCompact());

							GreedyOneToOneMatchingAlgorithm<MatchableEntity, MatchableTableColumn> oneToOne = new GreedyOneToOneMatchingAlgorithm<>(columnCors);
							oneToOne.setGroupByLeftDataSource(true);
							oneToOne.setGroupByRightDataSource(true);
							oneToOne.run();
							columnCors = oneToOne.getResult();
							// MatchingEngine<MatchableEntity, MatchableTableColumn> engine = new MatchingEngine<>();
							// instanceCors = engine.getTopKInstanceCorrespondences(instanceCors, 1, 0.0);
							System.out.println(String.format("%d instance correspondences after 1:1", columnCors.size()));

							if(columnCors.size()>0) {

								//TODO: group by left column, then aggregate by class
								// cor.getSecondRecord().getDataSourceIdentifier() can group instance cors by dbp class
								// what can group them by wt column?

								Processable<Pair<Integer, Double>> classDistribution = columnCors.aggregate((cor,col)->col.next(new Pair<>(cor.getSecondRecord().getDataSourceIdentifier(), cor.getSimilarityScore())), new SumDoubleAggregator<>());

								for(Pair<Integer, Double> p : classDistribution.get()) {
									Processable<Correspondence<MatchableEntity, MatchableTableColumn>> cors = columnCors
											.where((c0) -> c0.getSecondRecord().getDataSourceIdentifier() == p.getFirst());
									Processable<Group<MatchableEntity, MatchableEntity>> entities = cors.group(
											new RecordKeyValueMapper<MatchableEntity, Correspondence<MatchableEntity, MatchableTableColumn>, MatchableEntity>() {

												@Override
												public void mapRecordToKey(
														Correspondence<MatchableEntity, MatchableTableColumn> record,
														DataIterator<Pair<MatchableEntity, MatchableEntity>> resultCollector) {
													resultCollector.next(new Pair<MatchableEntity, MatchableEntity>(
															record.getFirstRecord(), record.getSecondRecord()));
												}
											});
										
									System.out.println(String.format("\t%s\t%f\tcor: %d\tent: %d", kb.getClassIndices().get(p.getFirst()), p.getSecond(), cors.size(), entities.size()));

									for(Correspondence<MatchableEntity, MatchableTableColumn> cor : cors.take(10).get()) {
										System.out.println(String.format("\t\t%s\t<->\t%s (%f)", cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier(), cor.getSimilarityScore()));
									}
								}

								// Pair<Integer, Double> maxClass = Q.max(classDistribution.get(), (p)->p.getSecond());
								// MatchableTableColumn matchableKBCol = schemaCorrespondences.where((cor)->cor.getSecondRecord().getDataSourceIdentifier()==maxClass.getFirst()).firstOrNull().getSecondRecord();

								for(Pair<Integer, Double> p : classDistribution.get()) {
									MatchableTableColumn matchableWTCol = records.getSchema().getRecord(c.getIdentifier());
									MatchableTableColumn matchableKBCol = schemaCorrespondences.where((cor)->cor.getSecondRecord().getDataSourceIdentifier()==p.getFirst()).firstOrNull().getSecondRecord();

									Processable<Correspondence<MatchableEntity, MatchableTableColumn>> cors = columnCors
											.where((c0) -> c0.getSecondRecord().getDataSourceIdentifier() == p.getFirst());
									Processable<Group<MatchableEntity, MatchableEntity>> entities = cors.group(
											new RecordKeyValueMapper<MatchableEntity, Correspondence<MatchableEntity, MatchableTableColumn>, MatchableEntity>() {

												@Override
												public void mapRecordToKey(
														Correspondence<MatchableEntity, MatchableTableColumn> record,
														DataIterator<Pair<MatchableEntity, MatchableEntity>> resultCollector) {
													resultCollector.next(new Pair<MatchableEntity, MatchableEntity>(
															record.getFirstRecord(), record.getSecondRecord()));
												}
											});

									// normalise score
									// divide by number of unique entities for the corresponding class (in the web table)
									int correspondences = cors.size();
									int mappedEntities = entities.size();
									// int entityCount = bkg.getCreatedEntitiesPerClass().get(p.getFirst()).size();
									int entityCount = bkg.getCreatedEntitiesPerColumn().get(String.format("%d-%s",p.getFirst(), c.getIdentifier())).size();
									double summedScore = (double)p.getSecond();
									// double score = (double)p.getSecond() / (double)entityCount;
									double score = summedScore / (double)entityCount;
									double mappedPercentage = mappedEntities / (double)entityCount;
									System.out.println(String.format("%s: %d/%d entities mapped (%.2f%%) by %d correspondences with total score of %f -> normalised: %f", 
										matchableKBCol.getIdentifier(), 
										mappedEntities,
										entityCount, 
										mappedPercentage * 100, 
										correspondences,
										summedScore, 
										score));

									if(mappedPercentage >= minMappedEntityPercentage) {
										result.add(new Correspondence<MatchableTableColumn, Matchable>(matchableWTCol, matchableKBCol, score));
									}
								}
							}
						// }
					// }
				}

			}
		}

		return result;
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchRelationTableColumnsToClassesByEntitiesSingleBatch(
		Collection<Table> relationTables,
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
		double simThreshold,
		SimilarityMeasure<String> measure,
		String patternLocation,
		double minMappedEntityPercentage) throws Exception {

		Processable<Correspondence<MatchableTableColumn, Matchable>> result = new ParallelProcessableCollection<>();
		WebTableDataSetLoader loader = new WebTableDataSetLoader();

		PatternCleaner cleaner = null;
		if(patternLocation!=null) {
			cleaner = new PatternCleaner();
			cleaner.loadPatterns(new File(patternLocation));
		}


		LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(
			simThreshold);
		// rule.addComparator(new CorrespondenceBasedEntityComparator<String>(new
		// TokenizingJaccardSimilarity()), 1.0);
		// rule.addComparator(new CorrespondenceBasedEntityComparator<String>(new MaximumOfTokenContainment()), 1.0);
		rule.addComparator(new CorrespondenceBasedEntityComparator<String>(measure, kb), 1.0);

		Set<Pair<String, MatchableTableColumn>> blockedColumsRight = new HashSet<>();
		for (MatchableTableColumn c : kb.getSchema().get()) {
			if(KnowledgeBase.RDFS_LABEL.equals(c.getHeader())) {
				blockedColumsRight.add(
					new Pair<>("class_" + c.getTableId() + "-", c));
			}
		}

		MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator bkg = new MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator(
			kb.getCandidateKeys().get(), new HashMap<>());
		bkg.setMatchPartialKeys(true);
		MatchableTableRowTokenToEntityBlockingKeyGenerator bkg2 = new MatchableTableRowTokenToEntityBlockingKeyGenerator(
				blockedColumsRight);
		StandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> blocker = new StandardBlocker<>(
				bkg, bkg2);
		// blocker.setBlockFilterRatio(0.95);
		blocker.setMaxBlockPairSize(1000000);
		// blocker.setCacheBlocks(true);
		// blocker.setMeasureBlockSizes(true);

		for (Table t : relationTables) {

			if(cleaner!=null) {
				cleaner.cleanTable(t);
			}
		}
			// DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(Q.toList(t));
			DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(relationTables);

			// run identity resolution for the table

			// blocker.resetCache(true, false);

			// RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
			// 		records, kb.getRecords(), corsForColumn, rule, blocker);
			// RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
			// 		records, kb.getRecords(), schemaCorrespondences, rule, blocker);
			// algorithm.setTaskName("Identity Resolution");
			// algorithm.run();
			// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> entityCors = algorithm
			// 		.getResult();

			// for (TableColumn c : t.getColumns()) {

			// 	// aggregate entity scores for each column
			// 	// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCors = entityCors.where((cor)-> Q.any(cor.getFirstRecord().getEntityKey(), (mc)->mc.getIdentifier().equals(c.getIdentifier())));

			// 	Processable<Correspondence<MatchableTableColumn, Matchable>> corsForColumn = schemaCorrespondences
			// 			.where((cor) -> cor.getFirstRecord().getIdentifier().equals(c.getIdentifier()));

			// 	if (corsForColumn.size() > 0) {

			// 		System.out.println(String.format("Refining %s", c.getIdentifier()));
			// 		for (Correspondence<MatchableTableColumn, Matchable> cor : corsForColumn.get()) {
			// 			System.out.println(String.format("->%s\t%f", cor.getSecondRecord().getIdentifier(),
			// 					cor.getSimilarityScore()));
			// 		}

					// Set<Pair<String, MatchableTableColumn>> blockedColumsLeft = new HashSet<>();
					// // Set<Pair<String, MatchableTableColumn>> blockedColumsRight = new HashSet<>();
					// for (Correspondence<MatchableTableColumn, Matchable> cor : corsForColumn.get()) {
					// 	blockedColumsLeft.add(new Pair<>(null, cor.getFirstRecord()));
					// 	// blockedColumsRight.add(
					// 	// 		new Pair<>("class_" + cor.getSecondRecord().getTableId() + "-", cor.getSecondRecord()));
					// }

					// System.out.println(String.format("Blocking by %d candidate keys", kb.getCandidateKeys().size()));
					// for(MatchableTableDeterminant d : kb.getCandidateKeys().get()) {
					// 	System.out.println(
					// 		String.format("\t%s: {%s}", 
					// 			kb.getClassIndices().get(d.getTableId()), 
					// 			StringUtils.join(Q.project(d.getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ",")
					// 	));
					// }

					// MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator bkg = new MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator(
					// 	kb.getCandidateKeys().get(), new HashMap<>());
					// bkg.setMatchPartialKeys(true);
					// MatchableTableRowTokenToEntityBlockingKeyGenerator bkg2 = new MatchableTableRowTokenToEntityBlockingKeyGenerator(
					// 		blockedColumsRight);
					// StandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> blocker = new StandardBlocker<>(
					// 		bkg, bkg2);
					// blocker.setBlockFilterRatio(0.95);
					// blocker.setMaxBlockPairSize(1000000);
					// // blocker.setMeasureBlockSizes(true);


					// bkg.getCreatedEntitiesPerClass().clear();
					// blocker.resetCache(true, false);

					// RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
					// 		records, kb.getRecords(), corsForColumn, rule, blocker);
					RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(
							records, kb.getRecords(), schemaCorrespondences, rule, blocker);
					algorithm.setTaskName("Identity Resolution");
					algorithm.run();
					Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCors = algorithm
							.getResult();

					// // create class distribution
					// Distribution<String> classDist = Distribution.fromCollection(instanceCors.map((cor)->kb.getClassIndices().get(cor.getSecondRecord().getTableId())).get());
					// System.out.println(classDist.formatCompact());

					// group instance correspondences by entity
					Processable<Group<MatchableEntity, MatchableEntity>> grouped = instanceCors.group(
							new RecordKeyValueMapper<MatchableEntity, Correspondence<MatchableEntity, MatchableTableColumn>, MatchableEntity>() {

								@Override
								public void mapRecordToKey(Correspondence<MatchableEntity, MatchableTableColumn> record,
										DataIterator<Pair<MatchableEntity, MatchableEntity>> resultCollector) {
									resultCollector.next(new Pair<MatchableEntity, MatchableEntity>(
											record.getFirstRecord(), record.getSecondRecord()));
								}
							});

					System.out.println(String.format("Found correspondences for %d entities", grouped.size()));
					

					// for(Group<MatchableEntity, MatchableEntity> g : grouped.get()) {
					// 	System.out.println(String.format("Found %d matches for entity '%s'", g.getRecords().size(), g.getKey().getIdentifier()));
					// }

					Processable<Group<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>>> groupedByColumn = instanceCors.group(
							new RecordKeyValueMapper<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>, Correspondence<MatchableEntity, MatchableTableColumn>>() {

								@Override
								public void mapRecordToKey(Correspondence<MatchableEntity, MatchableTableColumn> record,
										DataIterator<Pair<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>>> resultCollector) {
									for(MatchableTableColumn col : record.getFirstRecord().getEntityKey()) {
										resultCollector.next(new Pair<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>>(
											col, record));
									}
								}
							});
					for(Group<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>> g : groupedByColumn.get()) {
						System.out.println(String.format("Found %d correspondences for column %s", g.getRecords().size(), g.getKey().getIdentifier()));
					}

					// for (TableColumn c : t.getColumns()) {
					for(Group<MatchableTableColumn, Correspondence<MatchableEntity, MatchableTableColumn>> g : groupedByColumn.get()) {

						MatchableTableColumn c = g.getKey();
						// aggregate entity scores for each column
						// Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCors = entityCors.where((cor)-> Q.any(cor.getFirstRecord().getEntityKey(), (mc)->mc.getIdentifier().equals(c.getIdentifier())));
		
						Processable<Correspondence<MatchableTableColumn, Matchable>> corsForColumn = schemaCorrespondences
								.where((cor) -> cor.getFirstRecord().getIdentifier().equals(c.getIdentifier()));
		
						if (corsForColumn.size() > 0) {
		
							System.out.println(String.format("Refining %s", c.getIdentifier()));
							for (Correspondence<MatchableTableColumn, Matchable> cor : corsForColumn.get()) {
								System.out.println(String.format("->%s\t%f", cor.getSecondRecord().getIdentifier(),
										cor.getSimilarityScore()));
							}

							Processable<Correspondence<MatchableEntity, MatchableTableColumn>> columnCors = g.getRecords();
							// create class distribution
							Distribution<String> classDist = Distribution.fromCollection(columnCors.map((cor)->kb.getClassIndices().get(cor.getSecondRecord().getTableId())).get());
							System.out.println(classDist.formatCompact());
							// create entity distribution
							Distribution<String> entityDist = Distribution.fromCollection(columnCors.map((cor)->cor.getSecondRecord().getIdentifier()).get());
							System.out.println(entityDist.formatCompact());

							GreedyOneToOneMatchingAlgorithm<MatchableEntity, MatchableTableColumn> oneToOne = new GreedyOneToOneMatchingAlgorithm<>(columnCors);
							oneToOne.setGroupByLeftDataSource(true);
							oneToOne.setGroupByRightDataSource(true);
							oneToOne.run();
							columnCors = oneToOne.getResult();
							// MatchingEngine<MatchableEntity, MatchableTableColumn> engine = new MatchingEngine<>();
							// instanceCors = engine.getTopKInstanceCorrespondences(instanceCors, 1, 0.0);
							System.out.println(String.format("%d instance correspondences after 1:1", columnCors.size()));

							if(columnCors.size()>0) {

								//TODO: group by left column, then aggregate by class
								// cor.getSecondRecord().getDataSourceIdentifier() can group instance cors by dbp class
								// what can group them by wt column?

								Processable<Pair<Integer, Double>> classDistribution = columnCors.aggregate((cor,col)->col.next(new Pair<>(cor.getSecondRecord().getDataSourceIdentifier(), cor.getSimilarityScore())), new SumDoubleAggregator<>());

								for(Pair<Integer, Double> p : classDistribution.get()) {
									Processable<Correspondence<MatchableEntity, MatchableTableColumn>> cors = columnCors
											.where((c0) -> c0.getSecondRecord().getDataSourceIdentifier() == p.getFirst());
									Processable<Group<MatchableEntity, MatchableEntity>> entities = cors.group(
											new RecordKeyValueMapper<MatchableEntity, Correspondence<MatchableEntity, MatchableTableColumn>, MatchableEntity>() {

												@Override
												public void mapRecordToKey(
														Correspondence<MatchableEntity, MatchableTableColumn> record,
														DataIterator<Pair<MatchableEntity, MatchableEntity>> resultCollector) {
													resultCollector.next(new Pair<MatchableEntity, MatchableEntity>(
															record.getFirstRecord(), record.getSecondRecord()));
												}
											});
										
									System.out.println(String.format("\t%s\t%f\tcor: %d\tent: %d", kb.getClassIndices().get(p.getFirst()), p.getSecond(), cors.size(), entities.size()));

									for(Correspondence<MatchableEntity, MatchableTableColumn> cor : cors.take(10).get()) {
										System.out.println(String.format("\t\t%s\t<->\t%s (%f)", cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier(), cor.getSimilarityScore()));
									}
								}

								// Pair<Integer, Double> maxClass = Q.max(classDistribution.get(), (p)->p.getSecond());
								// MatchableTableColumn matchableKBCol = schemaCorrespondences.where((cor)->cor.getSecondRecord().getDataSourceIdentifier()==maxClass.getFirst()).firstOrNull().getSecondRecord();

								for(Pair<Integer, Double> p : classDistribution.get()) {
									MatchableTableColumn matchableWTCol = records.getSchema().getRecord(c.getIdentifier());
									MatchableTableColumn matchableKBCol = schemaCorrespondences.where((cor)->cor.getSecondRecord().getDataSourceIdentifier()==p.getFirst()).firstOrNull().getSecondRecord();

									Processable<Correspondence<MatchableEntity, MatchableTableColumn>> cors = columnCors
											.where((c0) -> c0.getSecondRecord().getDataSourceIdentifier() == p.getFirst());
									Processable<Group<MatchableEntity, MatchableEntity>> entities = cors.group(
											new RecordKeyValueMapper<MatchableEntity, Correspondence<MatchableEntity, MatchableTableColumn>, MatchableEntity>() {

												@Override
												public void mapRecordToKey(
														Correspondence<MatchableEntity, MatchableTableColumn> record,
														DataIterator<Pair<MatchableEntity, MatchableEntity>> resultCollector) {
													resultCollector.next(new Pair<MatchableEntity, MatchableEntity>(
															record.getFirstRecord(), record.getSecondRecord()));
												}
											});

									// normalise score
									// divide by number of unique entities for the corresponding class (in the web table)
									int correspondences = cors.size();
									int mappedEntities = entities.size();
									// int entityCount = bkg.getCreatedEntitiesPerClass().get(p.getFirst()).size();
									int entityCount = bkg.getCreatedEntitiesPerColumn().get(String.format("%d-%s",p.getFirst(), c.getIdentifier())).size();
									double summedScore = (double)p.getSecond();
									// double score = (double)p.getSecond() / (double)entityCount;
									double score = summedScore / (double)entityCount;
									double mappedPercentage = mappedEntities / (double)entityCount;
									System.out.println(String.format("%s: %d/%d entities mapped (%.2f%%) by %d correspondences with total score of %f -> normalised: %f", 
										matchableKBCol.getIdentifier(), 
										mappedEntities,
										entityCount, 
										mappedPercentage * 100, 
										correspondences,
										summedScore, 
										score));

									if(mappedPercentage >= minMappedEntityPercentage) {
										result.add(new Correspondence<MatchableTableColumn, Matchable>(matchableWTCol, matchableKBCol, score));
									}
								}
							}
						}
					}
				// }

		// 	}
		// }

		return result;
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchRelationTablesToKnowledgeBase(Collection<Table> relationTables) throws Exception {
	
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences = new ParallelProcessableCollection<>();
		
		WebTableDataSetLoader loader = new WebTableDataSetLoader();

		for(Table rt : relationTables) {
			
			// copy the table
			Table t = rt.project(rt.getColumns());
			// get the URI column
			TableColumn uriCol = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"URI".equals(c.getHeader())));
			// remove all rows without uri (unlinked entities)
			Iterator<TableRow> rowIt = t.getRows().iterator();
			while(rowIt.hasNext()) {
				if(rowIt.next().get(uriCol.getColumnIndex())==null) {
					rowIt.remove();
				}
			}
			t.reorganiseRowNumbers();
			// deduplicate the copy based on URI values
			t.deduplicate(Q.toList(uriCol), ConflictHandling.CreateSet);
			
			// create dataset
			DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(Q.toList(t));
			
			// get URI column
			MatchableTableColumn uriColumn = null;
			for(MatchableTableColumn c : records.getSchema().get()) {
				if("URI".equals(c.getHeader())) {
					uriColumn = c;
				}
			}
			
			if(uriColumn!=null) {
				Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = new ParallelProcessableCollection<>();
				
				// create instance correspondences
				for(MatchableTableRow row : records.get()) {
					
					if(row.hasValue(uriColumn)) {
						String uri = row.get(uriColumn.getColumnIndex()).toString();
						MatchableTableRow otherRow = kb.getRecords().getRecord(uri);
						
						if(otherRow!=null) {
							Correspondence<MatchableTableRow, MatchableTableColumn> cor = new Correspondence<MatchableTableRow, MatchableTableColumn>(row, otherRow, 1.0);
							
							instanceCorrespondences.add(cor);
						}
					}
					
				}
				
				// run matching
				MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();

				// block by data type
				StandardSchemaBlocker<MatchableTableColumn, MatchableTableRow> blocker = new StandardSchemaBlocker<>(new TypedBasedBlockingKeyGenerator());
				// blocker.setMeasureBlockSizes(true);		

				WeightedDateSimilarity dateSim = new WeightedDateSimilarity(1, 3, 5);
				dateSim.setYearRange(10);
				RelationTableToKBVotingRule rule = new RelationTableToKBVotingRule(0.1, new UnadjustedDeviationSimilarity(), dateSim, new TokenizingJaccardSimilarity());
				
				// TopKVotesAggregator<MatchableTableColumn, MatchableTableRow> filter = new TopKVotesAggregator<>(1);
				TopKVotesAggregator<MatchableTableColumn, MatchableTableRow> filter = null;
				
				// normalise votes by number of instance correspondences
				// CorrespondenceAggregator<MatchableTableColumn, MatchableTableRow> voteAggregator = new VotingAggregator<>(true, instanceCorrespondences.size(), 0.0);
				CorrespondenceAggregator<MatchableTableColumn, MatchableTableRow> voteAggregator = new VotingAggregator<>(true, 0.1);
				
				int clsId = kb.getClassIds().get(rt.getMapping().getMappedClass().getFirst());
				ParallelHashedDataSet<MatchableTableColumn, MatchableTableColumn> classSchema = new ParallelHashedDataSet<>(kb.getSchema().where((c)->c.getTableId()==clsId).get());

				Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = engine.runDuplicateBasedSchemaMatching(
					records.getSchema(), 
					classSchema, 
					instanceCorrespondences, 
					rule, 
					filter, 
					voteAggregator, 
					blocker);

				System.out.println(String.format("Matching '%s' to KB", rt.getPath()));
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.sort((c)->c.getSimilarityScore()).get()) {
					System.out.println(String.format("\t%.6f\t%d\t%s <-> %s", cor.getSimilarityScore(), cor.getCausalCorrespondences().size(), cor.getFirstRecord(), cor.getSecondRecord()));
				}
		
				// create 1:1 mapping for correspondences
				MaximumBipartiteMatchingAlgorithm<MatchableTableColumn,MatchableTableRow> max = new MaximumBipartiteMatchingAlgorithm<>(schemaCorrespondences);
				max.run();
				schemaCorrespondences = max.getResult();
				
				System.out.println(String.format("Global Matching for '%s'", rt.getPath()));
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.sort((c)->c.getSimilarityScore()).get()) {
					System.out.println(String.format("\t%.6f\t%s <-> %s", cor.getSimilarityScore(), cor.getFirstRecord(), cor.getSecondRecord()));
				}

				// update table columns with properties
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
					TableColumn c = rt.getSchema().get(cor.getFirstRecord().getColumnIndex());
					
					c.setHeader(cor.getSecondRecord().getHeader());
					
					rt.getMapping().setMappedProperty(c.getColumnIndex(), new Pair<String, Double>(cor.getSecondRecord().getIdentifier(), cor.getSimilarityScore()));
				}
				
				correspondences = correspondences.append(schemaCorrespondences);
			}
		}
		
		return Correspondence.toMatchable(correspondences);
	}
	
	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchRelationTables(
		Collection<Table> relationTables
	) {
		WebTableDataSetLoader loader = new WebTableDataSetLoader();

		Processable<Correspondence<MatchableTableColumn, MatchableValue>> allCors = new ParallelProcessableCollection<>();

		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();

		Map<String, Collection<Table>> byClass = Q.group(relationTables, (t)->t.getMapping().getMappedClass().getFirst());

		for(String cls : byClass.keySet()) {

			Collection<Table> tables = byClass.get(cls);

			System.out.println(String.format("Matching %d tables for class %s", tables.size(), cls));

			DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(tables);

			Set<String> excludedHeaders = Q.toSet("fk");
			Set<DataType> typeFilter = Q.toSet(DataType.string);

			Processable<Correspondence<MatchableTableColumn, MatchableValue>> correspondences = engine.runInstanceBasedSchemaMatching(
				records, 
				records, 
				new TableRowTokenGenerator(excludedHeaders, typeFilter), 
				new TableRowTokenGenerator(excludedHeaders, typeFilter), 
				VectorCreationMethod.BinaryTermOccurrences, 
				// VectorCreationMethod.TFIDF,
				// new VectorSpaceMaximumOfContainmentSimilarity(), 
				new VectorSpaceJaccardSimilarity(),
				// new VectorSpaceCosineSimilarity(),
				0.5);
		
			correspondences = correspondences.where((cor)->cor.getFirstRecord().getDataSourceIdentifier()!=cor.getSecondRecord().getDataSourceIdentifier());

			allCors = allCors.append(correspondences);

			System.out.println(String.format("- Found %d correspondences", correspondences.size()));
		}

		return Correspondence.toMatchable(allCors);
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchRelationTablesWithRefinement(
		Collection<Table> relationTables, DisjointHeaders dh, File logDirectory,
		VectorCreationMethod vectorCreation, VectorSpaceSimilarity similarity, double threshold
	) throws Exception {
		WebTableDataSetLoader loader = new WebTableDataSetLoader();

		Processable<Correspondence<MatchableTableColumn, Matchable>> allCors = new ParallelProcessableCollection<>();

		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();

		Map<String, Collection<Table>> byClass = Q.group(relationTables, (t)->t.getMapping().getMappedClass().getFirst());

		for(String cls : byClass.keySet()) {

			Collection<Table> tables = byClass.get(cls);

			System.out.println(String.format("Matching %d tables for class %s", tables.size(), cls));

			FusibleDataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(tables);

			WebTables web = new WebTables();
			web.setRecords(records);

			TableToTableMatcher matcher = null;

			Set<String> excludedHeaders = Q.toSet("fk");
			Set<DataType> typeFilter = Q.toSet(DataType.string);

			// matcher = new LabelBasedMatcher();
			// matcher = new ImprovedValueBasedMatcher(0.2);
			matcher = new CustomizableValueBasedMatcher(
				// 0.2, 
				threshold,
				new TableRowTokenGenerator(excludedHeaders, typeFilter), 
				// VectorCreationMethod.BinaryTermOccurrences, 
				// VectorCreationMethod.TFIDF, 
				vectorCreation,
				// new VectorSpaceJaccardSimilarity());
				// new VectorSpaceCosineSimilarity()
				similarity
				);
			
			Map<String, Set<String>> disjointHeaders = dh.getAllDisjointHeaders();
			
			matcher.setWebTables(web);
			matcher.setMatchingEngine(new MatchingEngine<>());
			matcher.setDisjointHeaders(disjointHeaders);
			matcher.setVerbose(true);
			matcher.setLogDirectory(logDirectory);
			matcher.setMatchContextColumnsByHeader(false);
			
			matcher.initialise();
			matcher.match();
			
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = matcher.getSchemaCorrespondences();

			allCors = allCors.append(schemaCorrespondences);

			System.out.println(String.format("- Found %d correspondences", schemaCorrespondences.size()));
		}

		return Correspondence.toMatchable(allCors);
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchRelationTables(
		Collection<Table> relationTables, 
		DataSet<MatchableTableRow, MatchableTableColumn> relationTableRecords,
		Collection<String> clusteredPKs
		) throws Exception {
			WebTableDataSetLoader loader = new WebTableDataSetLoader();
			Collection<Table> tables = new LinkedList<>();

			for(Table rt : relationTables) {
				System.out.println(String.format("[RelationTableMatcher] Creating merged view for %s", rt.getPath()));

				// copy the table
				Table t = rt.project(rt.getColumns());
				t.setTableId(rt.getTableId());
				// get the FK column
				TableColumn fkCol = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"fk".equals(c.getHeader())));
				// remove all rows with unclustered FKs (entities that only appear in one data source)
				Processable<TableRow> rows = new ParallelProcessableCollection<>(t.getRows());
				rows = rows.map(
					(r) -> {
						String fkValue = r.get(fkCol.getColumnIndex()).toString();
						if(clusteredPKs.contains(fkValue)) {
							return r;
						} else {
							return null;
						}
					});

				t.setRows(new ArrayList<>(rows.get()));
				t.reorganiseRowNumbers();
				// deduplicate the copy based on FK values
				t.deduplicate(Q.toList(fkCol), ConflictHandling.CreateSet);
				
				tables.add(t);
			}

			// create dataset
			DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(tables);

			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = new ParallelProcessableCollection<>();
				
			System.out.println("Creating instance correspondences");
			// get all record clusters (cannot be done in the previous loop, as the row identifiers are modified)
			Map<String, Collection<String>> recordClusters = new HashMap<>();
			for(Table t : tables) {
				// get the FK column
				TableColumn fkCol = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"fk".equals(c.getHeader())));
				for(TableRow r : t.getRows()) {
					String fkValue = r.get(fkCol.getColumnIndex()).toString();
					Collection<String> cluster = MapUtils.get(recordClusters, fkValue, new HashSet<>());
					cluster.add(r.getIdentifier());
				}
			}

			// create instance correspondences
			for(Collection<String> cluster : recordClusters.values()) {
				List<String> l = new ArrayList<>(cluster);
				for(int i = 0; i < l.size(); i++) {
					for(int j = i+1; j < l.size(); j++) {
						instanceCorrespondences.add(new Correspondence<MatchableTableRow,MatchableTableColumn>(records.getRecord(l.get(i)), records.getRecord(l.get(j)), 1.0));
					}
				}
			}
			
			// run matching
			MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();

			// block by data type
			StandardSchemaBlocker<MatchableTableColumn, MatchableTableRow> blocker = new StandardSchemaBlocker<>(new TypedBasedBlockingKeyGenerator());
			// blocker.setMeasureBlockSizes(true);		

			WeightedDateSimilarity dateSim = new WeightedDateSimilarity(1, 3, 5);
			dateSim.setYearRange(10);
			RelationTableVotingRule rule = new RelationTableVotingRule(0.1, new UnadjustedDeviationSimilarity(), dateSim, new TokenizingJaccardSimilarity());
			
			// TopKVotesAggregator<MatchableTableColumn, MatchableTableRow> filter = new TopKVotesAggregator<>(1);
			TopKVotesAggregator<MatchableTableColumn, MatchableTableRow> filter = null;
			
			// normalise votes by number of instance correspondences
			// CorrespondenceAggregator<MatchableTableColumn, MatchableTableRow> voteAggregator = new VotingAggregator<>(true, instanceCorrespondences.size(), 0.0);
			CorrespondenceAggregator<MatchableTableColumn, MatchableTableRow> voteAggregator = new VotingAggregator<>(true, 0.1);

			Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = engine.runDuplicateBasedSchemaMatching(
				records.getSchema(), 
				records.getSchema(), 
				// Does not work: the schema and the instance correspondences are created from different sets of tables
				// relationTableRecords.getSchema(),
				// relationTableRecords.getSchema(),
				instanceCorrespondences, 
				rule, 
				filter, 
				voteAggregator, 
				blocker);

			System.out.println(String.format("Schema matching of %d relation tables", tables.size()));
			for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.sort((c)->c.getSimilarityScore()).get()) {
				System.out.println(String.format("\t%.6f\t%d\t%s <-> %s", cor.getSimilarityScore(), cor.getCausalCorrespondences().size(), cor.getFirstRecord(), cor.getSecondRecord()));
			}

			GreedyOneToOneMatchingAlgorithm<MatchableTableColumn,MatchableTableRow> oneToOne = new GreedyOneToOneMatchingAlgorithm<>(schemaCorrespondences);
			oneToOne.run();
			schemaCorrespondences = oneToOne.getResult();
			
			System.out.println(String.format("Global Matching for %d relation tables", tables.size()));
			for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.sort((c)->c.getSimilarityScore()).get()) {
				System.out.println(String.format("\t%.6f\t%s <-> %s", cor.getSimilarityScore(), cor.getFirstRecord(), cor.getSecondRecord()));
			}
			
			return Correspondence.toMatchable(schemaCorrespondences);
		}
}
