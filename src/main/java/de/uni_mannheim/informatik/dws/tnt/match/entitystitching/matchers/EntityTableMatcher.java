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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.EntityTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableEntity;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableLodColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableDataSetLoader;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.MatchableTableRowToKBComparator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.RuleBasedMatchingAlgorithmExtended;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.LodTableRowTokenGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.MatchableTableRowTokenBlockingKeyGenerator2;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.MatchableTableRowTokenToEntityBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.TableRowTokenGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.blocking.TypedBasedEntityBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.duplicatebased.TableEntityToKBVotingRule;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.FullClusterBasedMaximumMatching;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.GraphBasedRefinement;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.KeyContainmentRule;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.LabelMappedConstraint;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.TransitivityMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.similarity.TFIDF;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.VotingAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.GreedyOneToOneMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.MaximumBipartiteMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.RuleLearner;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.TransitiveCorrespondencesCreator;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.VectorCreationMethod;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.InstanceBasedBlockingKeyIndexer;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardRecordBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardSchemaBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.rules.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.MatchingRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.MaxScoreMatchingRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.WekaMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.FeatureVectorDataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.RecordCSVFormatter;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.CountAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.DistributionAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.MaxAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SetAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SumDoubleAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.DeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.UnadjustedDeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceCosineSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EntityTableMatcher {

	// load entity definitions
	private KnowledgeBase kb;
	private File entityDefinitionLocation;
	
	private boolean logCorrespondences = false;
	private File logDirectory;
	private String taskName;
	private String configuration = "";
//	private IIndex kbIndex;
	private int minNumberOfInstanceCorrespondences = 0;
	private double minMatchRatio = 0.0;
	private boolean useGraphBasedRefinement = true;
	
	private Collection<EntityTable> referenceEntityTables = null;
	
	/**
	 * @param configuration the configuration to set
	 */
	public void setConfiguration(String configuration) {
		this.configuration = configuration;
	}

	private boolean doNotDeduplicateInstanceCandidates = false;
	/**
	 * @param doNotDeduplicateInstanceCandidates the doNotDeduplicateInstanceCandidates to set
	 */
	public void setDoNotDeduplicateInstanceCandidates(boolean doNotDeduplicateInstanceCandidates) {
		this.doNotDeduplicateInstanceCandidates = doNotDeduplicateInstanceCandidates;
	}

	public void setReferenceEntityTables(Collection<EntityTable> referenceEntityTables) {
		this.referenceEntityTables = referenceEntityTables;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}
	
	public EntityTableMatcher(int minNumberOfInstanceCorrespondences) {
		this.minNumberOfInstanceCorrespondences = minNumberOfInstanceCorrespondences;
	}
	
	/**
	 * @param logCorrespondences the logCorrespondences to set
	 */
	public void setLogCorrespondences(boolean logCorrespondences) {
		this.logCorrespondences = logCorrespondences;
	}
	
	public void setLogDirectory(File logDirectory, String taskName) {
		this.logDirectory = logDirectory;
	}

	/**
	 * @param useGraphBasedRefinement the useGraphBasedRefinement to set
	 */
	public void setUseGraphBasedRefinement(boolean useGraphBasedRefinement) {
		this.useGraphBasedRefinement = useGraphBasedRefinement;
	}

	/**
	 * @param minMatchRatio the minMatchRatio to set
	 */
	public void setMinMatchRatio(double minMatchRatio) {
		this.minMatchRatio = minMatchRatio;
	}
	
	public void loadEntityDefinitions(File entityDefinitionLocation, boolean loadSurfaceForms) throws IOException {
		this.entityDefinitionLocation = entityDefinitionLocation;
		KnowledgeBase.loadClassHierarchy(new File(entityDefinitionLocation, "class_hierarchy").getAbsolutePath());
		if(loadSurfaceForms) {
			SurfaceForms sf = new SurfaceForms(new File(entityDefinitionLocation, "surfaceforms"), new File(entityDefinitionLocation, "redirects"));
			// sf.loadSurfaceForms();
			// File indexLocation = new File(entityDefinitionLocation, "index/");
			// kbIndex = new DefaultIndex(indexLocation.getAbsolutePath());
			kb = KnowledgeBase.loadKnowledgeBase(new File(entityDefinitionLocation, "tables/"), null, sf, true, true);
		} else {
			kb = KnowledgeBase.loadKnowledgeBase(new File(entityDefinitionLocation, "tables/"), null, null, true, true);
		}
		kb.loadCandidateKeys(new File(entityDefinitionLocation, "keys/"));	
		kb.loadInclusionDependencies(new File(entityDefinitionLocation, "inclusion_dependencies"));
		
		if(logCorrespondences) {
			for(String cls : kb.getClassIds().keySet()) {
				int tblId = kb.getClassIds().get(cls);
				System.out.println(String.format("Class '%s' (#%d / %d entities / %d records): {%s}",
						cls, 
						tblId, 
						kb.getTablesSize().get(tblId),
						kb.getRecords().where((r)->r.getTableId()==tblId).size(),
						StringUtils.join(Q.project(kb.getSchema().where((c)->c.getTableId()==tblId).get(), new MatchableTableColumn.ColumnHeaderProjection()), ",")
						));
			}
		}
	}
	
	public KnowledgeBase getKnowledgeBase() {
		return kb;
	}
	
	/**
	 * 
	 * Matches the tables to the entity definitions and creates entity groups from the matching result.
	 * 
	 * @param records
	 * @param attributes
	 * @param candidateKeys
	 * @param tableToTableCorrespondences
	 * @param entityDefinitionLocation
	 * @param tables
	 * @param disjointHeaders
	 * @return
	 * @throws IOException 
	 */
	public Processable<Correspondence<MatchableTableColumn, Matchable>> detectEntityColumns(DataSet<MatchableTableRow, MatchableTableColumn> records,
			Processable<MatchableTableColumn> attributes, 
			Processable<MatchableTableDeterminant> candidateKeys,
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToTableCorrespondences,
			Map<Integer, Table> tables,
			DisjointHeaders disjointHeaders) throws IOException {
		
		System.out.println(String.format("[EntityTableMatcher.detectEntityColumns] creating row counts for %d tables", tables.size()));
		// create row counts for union tables
		Map<Integer, Integer> rowCounts = new HashMap<>();
		for(Integer i : tables.keySet()) {
			rowCounts.put(i, tables.get(i).getRows().size());
		}
		
		// *******************************************************************************************
		// (1) match table schema to entity definition schema
		// *******************************************************************************************
		System.out.println(String.format("[EntityTableMatcher.detectEntityColumns] running instance-based schema matching for %d web table records and %d knowledge base records", records.size(), kb.getRecords().size()));
		BlockingKeyIndexer<MatchableTableRow, MatchableTableColumn, MatchableTableColumn, MatchableValue> blocker
			= new InstanceBasedBlockingKeyIndexer<>(new TableRowTokenGenerator(), new LodTableRowTokenGenerator(), new VectorSpaceCosineSimilarity(), VectorCreationMethod.TFIDF, 0.0);		
		// blocker.setMeasureBlockSizes(logCorrespondences);
		Processable<Correspondence<MatchableTableColumn, MatchableValue>> tableToKBCorrespondences = blocker.runBlocking(records, kb.getRecords(), null);
		if(logCorrespondences) {
			System.out.println(String.format("%d correspondences before similarity threshold", tableToKBCorrespondences.size()));
			// printCorrespondences(Correspondence.toMatchable(tableToKBCorrespondences), tables);
		}
		
		// *******************************************************************************************
		// (2) apply similarity threshold
		// *******************************************************************************************
		tableToKBCorrespondences = tableToKBCorrespondences.where((c)->c.getSimilarityScore()>=0.1);
		if(logCorrespondences) {
			System.out.println(String.format("%d correspondences after similarity threshold", tableToKBCorrespondences.size()));
			printCorrespondences(Correspondence.toMatchable(tableToKBCorrespondences), tables);
		}
		
		// *******************************************************************************************
		// (3) Filter: only original columns can be mapped to rdfs:label
		// *******************************************************************************************
		// the main entity's name must be in one of the original columns: remove all correspondences from context columns to rdfs:label
		tableToKBCorrespondences = tableToKBCorrespondences
				.where((c)->!(ContextColumns.isContextColumn(c.getFirstRecord()) && "rdf-schema#label".equals(c.getSecondRecord().getHeader())));
		
		if(logCorrespondences) {
			System.out.println(String.format("%d correspondences after context-to-label filter", tableToKBCorrespondences.size()));
			printCorrespondences(Correspondence.toMatchable(tableToKBCorrespondences), tables);
		}

		System.out.println(String.format("Instance-based schema matching found %d correspondences", tableToKBCorrespondences.size()));
		
		// *******************************************************************************************
		// (4) refine schema correspondences
		// *******************************************************************************************
		Processable<Correspondence<MatchableTableColumn, Matchable>> refinedTableToKBCorrespondences = runDuplicatBasedSchemaMatching(
			// only link records to classes if a complete candidate key is mapped
			applyKeyCompletelyMappedConstraint(Correspondence.toMatchable(tableToKBCorrespondences)), 
			records, 
			attributes, 
			true, 
			tables);

		// the main entity's name must be in one of the original columns: remove all correspondences from context columns to rdfs:label
		refinedTableToKBCorrespondences = refinedTableToKBCorrespondences
				.where((c)->!(ContextColumns.isContextColumn(c.getFirstRecord()) && "rdf-schema#label".equals(c.getSecondRecord().getHeader())));

		// weight schema correspondences by uniqueness and union table size
		// removed: table size already done by normalisation of duplicate-based
		// removed: uniqueness prefers smaller tables, biases because instance correspondences are created on deduplicated data and 1:1 ensures unique counts! 
//		refinedTableToKBCorrespondences = applyCorrespondenceWeights(refinedTableToKBCorrespondences, tables);
		
		// *******************************************************************************************
		// (5) holistic global matching 
		// *******************************************************************************************
		// make sure each column cluster is consistently mapped to the same class, then decide for a single class per table
		FullClusterBasedMaximumMatching clusterMaximum = new FullClusterBasedMaximumMatching(kb);
		clusterMaximum.setLog(logCorrespondences);
		clusterMaximum.setLogDirectory(logDirectory);
		Processable<Correspondence<MatchableTableColumn, Matchable>> clusteredTableToKBCorrespondences = clusterMaximum.run(attributes, tableToTableCorrespondences, refinedTableToKBCorrespondences);
		System.out.println(String.format("Clustered Maximum Matching has %d correspondences", clusteredTableToKBCorrespondences.size()));
		printCorrespondences(Correspondence.toMatchable(clusteredTableToKBCorrespondences), tables);
		
//		clusteredTableToKBCorrespondences = applyKeyCompletelyMappedConstraint(clusteredTableToKBCorrespondences);
		
		// *******************************************************************************************
		// (6) remove correspondences between tables that are mapped to different classes
		// *******************************************************************************************
		tableToTableCorrespondences = runClassFiltering(tableToTableCorrespondences, clusteredTableToKBCorrespondences);

		// *******************************************************************************************
		// (7) merge column-to-column and column-to-property correspondences for holistic refinement
		// *******************************************************************************************
		// combine table-to-kb correspondences with table-to-table correspondences
		Processable<Correspondence<MatchableTableColumn, Matchable>> combinedCorrespondences = tableToTableCorrespondences.append(Correspondence.toMatchable(clusteredTableToKBCorrespondences));
		System.out.println(String.format("Combining resulted in %d correspondences ", combinedCorrespondences.size()));
		System.out.println("Refining Correspondences");

		if(useGraphBasedRefinement) {
			// run graph-based refinement to remove inconsistencies
			// after the holistic global matching, there can still be inconsistencies among columns in tables which are mapped to the same class
			// - if a column was incorrectly matched to a property (for example because there is no better match and the similarity is still high)
			// - we can detect the error if this column co-occurred with other columns which are also mapped to this property
			GraphBasedRefinement refiner = new GraphBasedRefinement(false);
			refiner.setReturnInvalidCorrespondences(true);
			refiner.setVerbose(true);
			Processable<Correspondence<MatchableTableColumn, Matchable>> invalidCorrespondences = refiner.match(combinedCorrespondences, disjointHeaders);
			System.out.println(String.format("%d table to kb correspondences before refinement", clusteredTableToKBCorrespondences.size()));
			clusteredTableToKBCorrespondences.remove(invalidCorrespondences.get());
			System.out.println(String.format("%d table to kb correspondences after refinement", clusteredTableToKBCorrespondences.size()));
			System.out.println(String.format("%d table to table correspondences before refinement", tableToTableCorrespondences.size()));
			tableToTableCorrespondences.remove(invalidCorrespondences.get());
			System.out.println(String.format("%d table to table correspondences after refinement", tableToTableCorrespondences.size()));
			System.out.println(String.format("Graph-based refinement removed %d correspondences", invalidCorrespondences.size()));
		}
		
		// we might have removed a mapping to rdfs:label in the graph based refinement
		// - so we have to make sure that each mapping to a class still contains it, otherwise we remove the mapping
		LabelMappedConstraint labelMapped = new LabelMappedConstraint(true);
		clusteredTableToKBCorrespondences = labelMapped.applyLabelMappedConstraint(clusteredTableToKBCorrespondences);

		tableToTableCorrespondences = runClassFiltering(tableToTableCorrespondences, clusteredTableToKBCorrespondences);
		combinedCorrespondences = tableToTableCorrespondences.append(clusteredTableToKBCorrespondences);
		
		System.out.println(String.format("Calculating correspondence closure for %d correspondences", combinedCorrespondences.size()));
		TransitivityMatcher<MatchableTableColumn> transitivity = new TransitivityMatcher<>();
		combinedCorrespondences = transitivity.run(combinedCorrespondences);
		System.out.println(String.format("%d correspondences in transitive closure", combinedCorrespondences.size()));
		
		// (9) remove correspondences that were created between KB columns
		System.out.println("Cleaning up correspondence graph");
		combinedCorrespondences = combinedCorrespondences.where((c)->!(c.getFirstRecord() instanceof MatchableLodColumn));
		
//		printCorrespondences(combinedCorrespondences, tables);
		
		return combinedCorrespondences;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> applyCorrespondenceWeights(
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKbCorrespondences,
			Map<Integer, Table> tables) {
		
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

		// create a second weight based on the number of rows in the table
		// determine the size of each column
		Processable<Pair<String, Double>> columnSize = new ParallelProcessableCollection<>(tables.values())
		.map((Table record, DataIterator<Pair<String, Double>> resultCollector) 
			-> {
				for(TableColumn c : record.getColumns()) {
					resultCollector.next(new Pair<>(c.getIdentifier(), (double)record.getRows().size()));
				}
			});
		// normalise by the size of the largest column
		Pair<String, Double> maxSize = columnSize.sort((p)->p.getSecond(),false).firstOrNull();
		Processable<Pair<String, Double>> columnWeight = columnSize.map((Pair<String, Double> record, DataIterator<Pair<String, Double>> resultCollector) -> {
			resultCollector.next(new Pair<>(record.getFirst(), record.getSecond() / maxSize.getSecond()));
		});
		
		tableToKbCorrespondences = tableToKbCorrespondences
			.join(columnUniqueness, (c)->c.getFirstRecord().getIdentifier(), (p)->p.getFirst())
			.map((Pair<Correspondence<MatchableTableColumn, Matchable>, Pair<String, Double>> record, DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) 
				-> {
					Correspondence<MatchableTableColumn, Matchable> cor = record.getFirst();
					Double uniqueness = record.getSecond().getSecond();
					
					cor.setsimilarityScore(cor.getSimilarityScore() * uniqueness);
					resultCollector.next(cor);
				});

		// do not weight by column size, already done in duplicate-based schema matching
//		tableToKbCorrespondences = tableToKbCorrespondences
//				.join(columnWeight, (c)->c.getFirstRecord().getIdentifier(), (p)->p.getFirst())
//				.map((Pair<Correspondence<MatchableTableColumn, Matchable>, Pair<String, Double>> record, DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) 
//					-> {
//						Correspondence<MatchableTableColumn, Matchable> cor = record.getFirst();
//						Double weight = record.getSecond().getSecond();
//						
//						cor.setsimilarityScore(cor.getSimilarityScore() * weight);
//						resultCollector.next(cor);
//					});
		
		System.out.println(String.format("Applied correspondence weights to %d correspondences", tableToKbCorrespondences.size()));
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : tableToKbCorrespondences
				.sort((c)->c.getFirstRecord().getTableId())
				.get()) {
			System.out.println(String.format("\t%f\t%s <-> %s", cor.getSimilarityScore(), cor.getFirstRecord(), cor.getSecondRecord()));
		}
		
		return tableToKbCorrespondences;
	}

	public Processable<Correspondence<MatchableEntity, MatchableTableColumn>>  createInstanceCorrespondencesForDuplicateBasedSchemaMatching(
		Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBCorrespondences,
		DataSet<MatchableTableRow, MatchableTableColumn> webRecords,
		boolean createOneToOneMapping,
		Map<Integer, Table> tables
	) throws IOException {

		MaximumBipartiteMatchingAlgorithm<MatchableTableColumn, Matchable> max = new MaximumBipartiteMatchingAlgorithm<>(tableToKBCorrespondences);
		max.setGroupByLeftDataSource(true);
		max.setGroupByRightDataSource(true);
		max.run();
		tableToKBCorrespondences = max.getResult();

		// *******************************************************************************************
		// create the matching rules
		// *******************************************************************************************
		MatchingRuleGenerator ruleGenerator = new MatchingRuleGenerator(kb, 0.6);
		ruleGenerator.setVerbose(logCorrespondences);
		Processable<LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn>> rules = ruleGenerator.generateMatchingRules(tableToKBCorrespondences);
		// add all matching rules to a max-score matching rule
		MaxScoreMatchingRule<MatchableEntity, MatchableTableColumn> matchingRule = new MaxScoreMatchingRule<>(0.0);
		System.out.println(String.format("Generated %d matching rules", rules.size()));
		for(LinearCombinationMatchingRule<MatchableEntity, MatchableTableColumn> rule : rules.get()) {
			matchingRule.addMatchingRule(rule);
		}
		
		// *******************************************************************************************
		// create the blocker
		// *******************************************************************************************
		// get all rdfs:label correspondences
		Processable<Correspondence<MatchableTableColumn, Matchable>> labelCorrespondences = Correspondence.toMatchable(tableToKBCorrespondences.where((cor)->"rdf-schema#label".equals(cor.getSecondRecord().getHeader())));
		// create a blocker based on the rdfs:label correspondences
		Set<Pair<String, MatchableTableColumn>> blockedColumsLeft = new HashSet<>();
		Set<Pair<String, MatchableTableColumn>> blockedColumsRight = new HashSet<>();
		for(Correspondence<MatchableTableColumn, Matchable> cor : labelCorrespondences.get()) {
			// add an identifier for each correspondence to prevent that blocks for different correspondences are merged (would influence block filtering)
			// - but now what is the advantage over running each table individually? - takes forever now
			// alternative: merge blocks by class - much faster, but still slower than before. ca. 3x more pairs generated than with merging all blocks
			// blockedColumsLeft.add(new Pair<>(String.format("%d|", cor.getSecondRecord().getTableId()), cor.getFirstRecord()));
			// blockedColumsRight.add(new Pair<>(String.format("%d|", cor.getSecondRecord().getTableId()), cor.getSecondRecord()));
			
			// merge all blocks, but no block filtering - surprisingly fast, ca. 10x more pairs than with block filtering
			// - with 0.99 block filtering, still ca. 5x more pairs and good results
			blockedColumsLeft.add(new Pair<>(null, cor.getFirstRecord()));
			blockedColumsRight.add(new Pair<>("class_" + cor.getSecondRecord().getTableId() + "-", cor.getSecondRecord()));
		}

		KeyContainmentRule containmentRule = new KeyContainmentRule(kb);
		Processable<Group<Integer, MatchableTableDeterminant>> excludedKeys = containmentRule.getExludedKeys(tableToKBCorrespondences);
		Map<Integer, Collection<MatchableTableDeterminant>> excludedKeysMap = new HashMap<>();
		for(Group<Integer, MatchableTableDeterminant> g : excludedKeys.get()) {
			excludedKeysMap.put(g.getKey(), g.getRecords().get());
		}


		System.out.println(String.format("Generating blocking keys from %d columns", blockedColumsLeft.size()));
		MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator bkg = new MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator(kb.getCandidateKeys().get(), excludedKeysMap);
		bkg.setDoNotDeduplicate(doNotDeduplicateInstanceCandidates);

		MatchableTableRowTokenToEntityBlockingKeyGenerator bkg2 = new MatchableTableRowTokenToEntityBlockingKeyGenerator(blockedColumsRight);
		StandardBlocker<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> blocker = new StandardBlocker<>(bkg, bkg2);
		blocker.setBlockFilterRatio(0.95);
		blocker.setMaxBlockPairSize(1000000);
		// blocker.setMeasureBlockSizes(true);
		
		// *******************************************************************************************
		// run identity resolution
		// *******************************************************************************************
		RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithmExtended<MatchableTableRow, MatchableTableColumn, MatchableEntity, MatchableTableColumn>(webRecords, kb.getRecords(), tableToKBCorrespondences, matchingRule, blocker);
		algorithm.setTaskName("Identity Resolution");
		algorithm.run();
		Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCorrespondences = algorithm.getResult();
		
		if(createOneToOneMapping) {
			// *******************************************************************************************
			// create a 1:1 mapping
			// *******************************************************************************************
			// run greedy global matching on instance correspondences per class
			GreedyOneToOneMatchingAlgorithm<MatchableEntity, MatchableTableColumn> greedyOneToOne = new GreedyOneToOneMatchingAlgorithm<>(instanceCorrespondences);
			greedyOneToOne.setGroupByRightDataSource(true);
			greedyOneToOne.setGroupByLeftDataSource(true);
			greedyOneToOne.run();
			instanceCorrespondences = greedyOneToOne.getResult();
			System.out.println(String.format("%d instance correspondences after greedy 1:1 matching per class", instanceCorrespondences.size()));
		}
		
		
		if(logCorrespondences) {
			// log the first 10 correspondences for every class
			Processable<Group<String, Correspondence<MatchableEntity, MatchableTableColumn>>> corsByClass = instanceCorrespondences
				.group(
				(Correspondence<MatchableEntity, MatchableTableColumn> record, DataIterator<Pair<String, Correspondence<MatchableEntity, MatchableTableColumn>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(kb.getClassIndices().get(record.getSecondRecord().getTableId()), record));
				});
			for(Group<String, Correspondence<MatchableEntity, MatchableTableColumn>> group : corsByClass.get()) {
				System.out.println(String.format("%d correspondences to %s", group.getRecords().size(), group.getKey()));
				for(Correspondence<MatchableEntity, MatchableTableColumn> cor : group.getRecords().sort((c)->c.getSimilarityScore(),false).take(10).get()) {
					System.out.println(String.format("%s\n%f\t%s\n\t%s", cor.getProvenance(), cor.getSimilarityScore(), cor.getFirstRecord().format(30), cor.getSecondRecord().format(30)));
				}
			}
		}
		
		
		// generate class distribution for each table
		Processable<Pair<Integer, Distribution<String>>> classCounts = instanceCorrespondences
			.aggregate(
				(Correspondence<MatchableEntity, MatchableTableColumn> record, DataIterator<Pair<Integer, Correspondence<MatchableEntity, MatchableTableColumn>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(record.getFirstRecord().getTableId(), record));
				}
				, new DistributionAggregator<Integer, Correspondence<MatchableEntity, MatchableTableColumn>, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String getInnerKey(Correspondence<MatchableEntity, MatchableTableColumn> record) {
						return kb.getClassIndices().get(record.getSecondRecord().getTableId());
					}
				});
		
		if(logCorrespondences) {
			// log detailed results, class distribution per table
			logIdentityResolutionResults(tables, webRecords, labelCorrespondences, classCounts);
		}

		// *******************************************************************************************
		// min match filter: remove correspondences to classes which are too infrequent
		// *******************************************************************************************
		int minInstanceMatchesPerClass = minNumberOfInstanceCorrespondences;
		Map<Integer, Collection<Integer>> classesToRemoveByTable = new HashMap<>();
		for(Pair<Integer, Distribution<String>> p : classCounts.sort((p)->p.getFirst()).get()) {
			Table t = tables.get(p.getFirst());
			Set<String> classesToRemove = new HashSet<>();

			for(String cls : p.getSecond().getElements()) {
				double frequency = p.getSecond().getFrequency(cls);
				
				int entitiesForClass = 0;
				int clsId = kb.getClassIds().get(cls);
				if(bkg.getCreatedEntitiesPerClass().containsKey(clsId)) {
					entitiesForClass = Q.where(bkg.getCreatedEntitiesPerClass().get(clsId), (e)->e.getTableId()==t.getTableId()).size();
				}
				double matchRatio = frequency/(double)entitiesForClass;
				System.out.println(String.format("[MinMatchFilter] Class %s has %d matches (%f)",
					cls,
					(int)frequency,
					matchRatio
				));
				if(frequency<minInstanceMatchesPerClass || matchRatio < minMatchRatio) {
					classesToRemove.add(cls);
				}
			}
			
			if(classesToRemove.size()>0) {
				EntityTable ref = getReferenceEntityTable(t);
				System.out.println(String.format("[MinMatchFilter] removing classes from mapping for table #%d %s [%s]: %s",
					t.getTableId(),
					t.getPath(),
					ref == null ? "" : ref.getEntityName(),
					StringUtils.join(classesToRemove, ",")
					));
			}
			
			classesToRemoveByTable.put(t.getTableId(), Q.project(classesToRemove, (s)->kb.getClassIds().get(s)));
		}
		instanceCorrespondences = instanceCorrespondences
				.where((c) -> {
					if(classesToRemoveByTable.containsKey(c.getFirstRecord().getTableId())) {
						return !classesToRemoveByTable.get(c.getFirstRecord().getTableId()).contains(c.getSecondRecord().getTableId());
					} else {
						return true;
					}
				});
		System.out.println(String.format("[MinMatchFilter] %d instance correspondences after filtering", instanceCorrespondences.size()));

		return instanceCorrespondences;
	}

	protected void logIdentityResolutionResults(
		Map<Integer, Table> tables,
		DataSet<MatchableTableRow, MatchableTableColumn> webRecords,
		Processable<Correspondence<MatchableTableColumn, Matchable>> labelCorrespondences,
		Processable<Pair<Integer, Distribution<String>>> classCounts
	) throws IOException {
		// get row counts for web tables
		Processable<Pair<Integer, Integer>> rowCounts = webRecords
		.aggregate(
			(MatchableTableRow record, DataIterator<Pair<Integer, MatchableTableRow>> resultCollector) 
			-> {
				resultCollector.next(new Pair<>(record.getTableId(), record));
			}
			, new CountAggregator<>());
		Map<Integer, Integer> webTableSizes = Pair.toMap(rowCounts.get());

		// get approximate entity count for web tables using distinct values of rdfs:label column
		// join records with label correspondences
		// - note: at this point, a table can have multiple rdfs:label correspondences (to different classes or even to the same class)
		// then, aggregate label values into set
		Processable<Pair<Integer, Map<Correspondence<MatchableTableColumn, Matchable>, Set<Object>>>> labels = webRecords
		.join(labelCorrespondences, (r)->r.getTableId(), (c)->c.getFirstRecord().getTableId())
		// aggregate by schema correspondence
		.aggregate(
			(Pair<MatchableTableRow, Correspondence<MatchableTableColumn, Matchable>> record, DataIterator<Pair<Correspondence<MatchableTableColumn, Matchable>, Object>> resultCollector) 
			-> {
				resultCollector.next(new Pair<>(
						record.getSecond(),
						record.getFirst().get(record.getSecond().getFirstRecord().getColumnIndex())
						));
			}
			, new SetAggregator<>())
		// then group by table
		.group(
			(Pair<Correspondence<MatchableTableColumn, Matchable>, Set<Object>> record, DataIterator<Pair<Integer, Pair<Correspondence<MatchableTableColumn, Matchable>, Set<Object>>>> resultCollector) 
			-> {
				resultCollector.next(new Pair<>(
					record.getFirst().getFirstRecord().getTableId(),
					record
					));					
			}
		)
		.map(
			(Group<Integer,Pair<Correspondence<MatchableTableColumn,Matchable>,Set<Object>>> record, DataIterator<Pair<Integer, Map<Correspondence<MatchableTableColumn, Matchable>, Set<Object>>>> resultCollector) 
			-> {
				resultCollector.next(new Pair<>(
						record.getKey(),
						Pair.toMap(record.getRecords().get())
						));
			}
		);

		Map<Integer, Map<Correspondence<MatchableTableColumn, Matchable>, Set<Object>>> webTableLabels = Pair.toMap(labels.get());

		BufferedWriter w = new BufferedWriter(new FileWriter(new File(logDirectory, "entity_class_distributions.tsv")));
		for(Pair<Integer, Distribution<String>> p : classCounts.sort((p)->p.getFirst()).get()) {
		Table t = tables.get(p.getFirst());
		EntityTable ref = getReferenceEntityTable(t);
		boolean majorityMatchesRef = ref!=null && p.getSecond().getMode().equals(ref.getEntityName());

		Map<Correspondence<MatchableTableColumn, Matchable>, Set<Object>> labelsByCor = webTableLabels.get(p.getFirst());
		Distribution<String> labelByCorDistribution = new Distribution<>();

		for(Correspondence<MatchableTableColumn, Matchable> lblCor : labelsByCor.keySet()) {
			Set<Object> labelValues = labelsByCor.get(lblCor);
			labelByCorDistribution.add(
					String.format("%s/%s", 
							lblCor.getFirstRecord().getIdentifier(),
							kb.getClassIndices().get(lblCor.getSecondRecord().getTableId())
							),  
					labelValues.size()
					);
		}


		System.out.println(String.format("[runDuplicatBasedSchemaMatching] Class distribution for table #%d %s %s(%d rows / %d labels / %d matches): %s\t\tlabel distribution (%d label correspondences): %s", 
				p.getFirst(),
				t.getPath(),
				ref == null ? "" : String.format("[%s]%s ", ref.getEntityName(), majorityMatchesRef ? "+" : "-"),
				webTableSizes.get(p.getFirst()),
				labelByCorDistribution.getMaxFrequency(),
				p.getSecond().getPopulationSize(),
				p.getSecond().formatCompact(),
				labelsByCor.keySet().size(),
				labelByCorDistribution.formatCompact()
				));

		w.write(StringUtils.join(new String[] {
				taskName,
				configuration,
				t.getPath(),
				ref == null ? "" : ref.getEntityName(),
				Integer.toString(webTableSizes.get(p.getFirst())),
				Integer.toString(labelByCorDistribution.getMaxFrequency()),
				Integer.toString(p.getSecond().getMaxFrequency()),
				p.getSecond().formatCompact(),
				Boolean.toString(majorityMatchesRef),
				Integer.toString(labelsByCor.keySet().size()),
				labelByCorDistribution.formatCompact()
		}, "\t"));
		w.write("\n");
		}

		w.close();
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> runDuplicatBasedSchemaMatching(
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBCorrespondences,
			DataSet<MatchableTableRow, MatchableTableColumn> webRecords,
			Processable<MatchableTableColumn> webAttributes,
			boolean createOneToOneMapping,
			Map<Integer, Table> tables
			) throws IOException {
		
		// *******************************************************************************************
		// create instance correspondences
		// *******************************************************************************************
		Processable<Correspondence<MatchableEntity, MatchableTableColumn>> instanceCorrespondences = createInstanceCorrespondencesForDuplicateBasedSchemaMatching(
			tableToKBCorrespondences,
			webRecords,
			createOneToOneMapping,
			tables
		);

		// *******************************************************************************************
		// run duplicate-based schema matching using the instance correspondences
		// *******************************************************************************************
		// block by data type
		StandardSchemaBlocker<MatchableTableColumn, MatchableEntity> schemaBlocker = new StandardSchemaBlocker<>(new TypedBasedEntityBlockingKeyGenerator());
		// schemaBlocker.setMeasureBlockSizes(true);		
	
		// set-up similarity measures
		WeightedDateSimilarity dateSimilarity = new WeightedDateSimilarity(1, 3, 5);
		dateSimilarity.setYearRange(10);

		// create voting rule
		// use different similarity measures per data type, vote if the similarity value is >= 0.1
		TableEntityToKBVotingRule rule = new TableEntityToKBVotingRule(0.1, new UnadjustedDeviationSimilarity(), dateSimilarity, new TokenizingJaccardSimilarity());
		// rule.setLogVotes(true);
	
		// define vote aggregation
		// use weighted voting and normalise with 1 without any similarity threshold
		// = use the sum of all similarity scores
		VotingAggregator<MatchableTableColumn, MatchableEntity> voteAggregator = new VotingAggregator<>(true,1,0.0);
		
		// run the matcher
		MatchingEngine<MatchableEntity, MatchableTableColumn> engine = new MatchingEngine<>();
		ParallelHashedDataSet<MatchableTableColumn, MatchableTableColumn> attributeDS = new ParallelHashedDataSet<>(webAttributes.get());
		Processable<Correspondence<MatchableTableColumn, MatchableEntity>> schemaCorrespondences = engine.runDuplicateBasedSchemaMatching(attributeDS, kb.getSchema(), instanceCorrespondences, rule, null, voteAggregator, schemaBlocker);

		// *******************************************************************************************
		// post-process similarity scores
		// *******************************************************************************************

		// hierarchy-based score propagation
		// ClassHierarchyBasedScorePropagation scorePropagation = new ClassHierarchyBasedScorePropagation(kb);
		// schemaCorrespondences = scorePropagation.runScorePropagation(schemaCorrespondences);
		
		// score normalisation
		// schemaCorrespondences = normaliseSchemaCorrespondenceScores(schemaCorrespondences, tables);
		
		if(logCorrespondences) {
			for(Correspondence<MatchableTableColumn, MatchableEntity> cor : schemaCorrespondences
					.sort((c)->c.getSimilarityScore())
					.sort((c)->c.getFirstRecord().getTableId())
					.get()) {
				
				System.out.println(String.format("\t%.6f (%d)\t%s <-> %s", cor.getSimilarityScore(), cor.getCausalCorrespondences().size(), cor.getFirstRecord(), cor.getSecondRecord()));
			}
		}
		
		// return all discovered schema correspondences
		return Correspondence.toMatchable(schemaCorrespondences);
		
	}

	public Processable<Correspondence<MatchableTableColumn, MatchableEntity>> normaliseSchemaCorrespondenceScores(
		Processable<Correspondence<MatchableTableColumn, MatchableEntity>> schemaCorrespondences,
		Map<Integer, Table> tables
	) {
		// get the max number of matched rows per table = classCounts
		Map<Integer, Integer> matchedRowCounts = Pair.toMap(schemaCorrespondences
				.aggregate(
					(Correspondence<MatchableTableColumn, MatchableEntity> record, DataIterator<Pair<Integer, Integer>> resultCollector) 
					-> {
						resultCollector.next(new Pair<>(record.getFirstRecord().getTableId(), record.getCausalCorrespondences().size()));
					}
					, new MaxAggregator<>())
					.get());
		
		// get the number of matched cells per column (= the 'known' part of the table)
		Map<String, Double> cellCounts = Pair.toMap(schemaCorrespondences
			.aggregate(
				(Correspondence<MatchableTableColumn, MatchableEntity> record, DataIterator<Pair<String, Double>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(record.getFirstRecord().getIdentifier(), (double)record.getCausalCorrespondences().size()));
				}
				, new SumDoubleAggregator<>())
				.get());
		// get the maximal number of matched cells per table/class combination
		// = the amount of cells we can understand if we map the table to this class
		MaximumBipartiteMatchingAlgorithm<MatchableTableColumn, MatchableEntity> maxSchemaForCells = new MaximumBipartiteMatchingAlgorithm<>(schemaCorrespondences);
		maxSchemaForCells.setGroupByLeftDataSource(true);
		maxSchemaForCells.run();
		Map<Integer, Double> maxMatchedCellsPerTable = Pair.toMap(maxSchemaForCells.getResult()
			// sum the number of instance correspondences over all schema correspondences by table/class pair
			// = the number of cells for which have an instance and a schema correspondence
			.aggregate(
				(Correspondence<MatchableTableColumn, MatchableEntity> record, DataIterator<Pair<Pair<Integer,Integer>, Double>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), (double)record.getCausalCorrespondences().size()));
				}
				, new SumDoubleAggregator<>())
			// get the maximum value per table
			// = the maximum number of cells we can understand with any of the possible classes
			.aggregate(
				(Pair<Pair<Integer, Integer>, Double> record, DataIterator<Pair<Integer, Double>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(record.getFirst().getFirst(), record.getSecond()));
				}
				, new MaxAggregator<>())
			.get());
		if(logCorrespondences) {
			for(Integer tblId : maxMatchedCellsPerTable.keySet()) {
				Table t = tables.get(tblId);
				System.out.println(String.format("[EntityTableMatcher] Max number of matched cells for table #%d %s = %d", t.getTableId(), t.getPath(), maxMatchedCellsPerTable.get(tblId).intValue()));
			}
		}
		// get the total number of matched cells in all tables
		Double totalCellCount = Q.sum(maxMatchedCellsPerTable.values());
		System.out.println(String.format("[EntityTableMatcher] Total number of matched cells: %d", totalCellCount.intValue()));
		
		double scoreBiasTowardsColumns = 1.0;
		
		// score normalisation
		schemaCorrespondences = schemaCorrespondences
				.map(
					(Correspondence<MatchableTableColumn, MatchableEntity> record, DataIterator<Correspondence<MatchableTableColumn, MatchableEntity>> resultCollector) 
					-> {
						Integer matchedRowsForTable = matchedRowCounts.get(record.getFirstRecord().getTableId());
						
						Double cellCount = cellCounts.get(record.getFirstRecord().getIdentifier());
						
						double rowScore = record.getCausalCorrespondences().size() / (double) matchedRowsForTable;
						
						// normalising by cell count per column
//						double columnScore = record.getSimilarityScore() / (double) cellCount;
						
						// normalising by total matched cell count
						double columnScore = record.getSimilarityScore() / totalCellCount;
						
//						double similarityScore = record.getSimilarityScore();
						double similarityScore = scoreBiasTowardsColumns * columnScore + (1-scoreBiasTowardsColumns) * rowScore; 
				
						resultCollector.next(new Correspondence<MatchableTableColumn, MatchableEntity>(
								record.getFirstRecord(), 
								record.getSecondRecord(), 
								similarityScore,
								record.getCausalCorrespondences()));
						
					});

		return schemaCorrespondences;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> runClassFiltering(
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToTableCorrespondences,
			Processable<Correspondence<MatchableTableColumn, Matchable>> clusteredTableToKBCorrespondences
			) {
		System.out.println(String.format("%d table-to-table correspondences before class filtering", tableToTableCorrespondences.size()));
		Processable<Pair<Integer, Integer>> tableToClassMapping = clusteredTableToKBCorrespondences.map((Correspondence<MatchableTableColumn, Matchable> record, DataIterator<Pair<Integer, Integer>> resultCollector) 
			-> {
				resultCollector.next(new Pair<Integer, Integer>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()));
			})
			.distinct();

		tableToTableCorrespondences = tableToTableCorrespondences
			// join left column in each correspondence to its class mapping
			.join(tableToClassMapping, (c)->c.getFirstRecord().getTableId(), (p)->p.getFirst())
			// join right column in each correspondence to its class mapping
			.join(tableToClassMapping, (c)->c.getFirst().getSecondRecord().getTableId(), (p)->p.getFirst())
			// keep a correspondence if both columns are mapped to the same class
			.map((Pair<Pair<Correspondence<MatchableTableColumn, Matchable>, Pair<Integer, Integer>>, Pair<Integer, Integer>> record, DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) 
				-> {
					Correspondence<MatchableTableColumn, Matchable> cor = record.getFirst().getFirst();
					int leftClass = record.getFirst().getSecond().getSecond();
					int rightClass = record.getSecond().getSecond();
					
					if(leftClass==rightClass) {
						resultCollector.next(cor);
					} else {
//						System.out.println(String.format("Removing correspondence with inconsistent class mapping: %s [#%d] <-> %s [#%d]", cor.getFirstRecord(), leftClass, cor.getSecondRecord(), rightClass));
					}
				});
		System.out.println(String.format("%d table-to-table correspondences after class filtering", tableToTableCorrespondences.size()));
		
		return tableToTableCorrespondences;
	}
	
	public Processable<Correspondence<MatchableTableColumn, Matchable>> applyKeyCompletelyMappedConstraint(Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		// constraint that all columns of at least one key must be mapped if a class mapping is assigned
		// group correspondences by table/class combination, check for each class if a full key was mapped. if not, remove correspondences 
		
		// group the keys by class
		Processable<Group<Integer, MatchableTableDeterminant>> groupedCandidateKeys = kb.getCandidateKeys()
				.group((MatchableTableDeterminant record, DataIterator<Pair<Integer, MatchableTableDeterminant>> resultCollector)  
				-> {
					resultCollector.next(new Pair<>(record.getTableId(), record));
				});
		correspondences = correspondences
			// group correspondences by table/class combination
			.group((Correspondence<MatchableTableColumn, Matchable> record,
					DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				})
			// join candidate keys, grouped by class
			.leftJoin(
					groupedCandidateKeys, 
					(Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>> g)->g.getKey().getSecond(),
					(Group<Integer, MatchableTableDeterminant> g)->g.getKey())
			// map correspondences to output if full candidate key is mapped
			.map((Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, Group<Integer, MatchableTableDeterminant>> record, DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) 
				-> {
					Collection<MatchableTableColumn> mappedColumns = new LinkedList<>();
					
					for(Correspondence<MatchableTableColumn, Matchable> cor : record.getFirst().getRecords().get()) {
						mappedColumns.add(cor.getSecondRecord());
					}
					
					boolean isAnyKeyMapped = false;
					if(record.getSecond()!=null) {
						for(MatchableTableDeterminant key : record.getSecond().getRecords().get()) {
							if(mappedColumns.containsAll(key.getColumns())) {
								isAnyKeyMapped = true;
								break;
							}
						}
					} else {
						// if there are no key definitions, then the current group is between web tables only, in which case we do not filter out anything
						isAnyKeyMapped = true;
					}
					
					if(isAnyKeyMapped) {
						for(Correspondence<MatchableTableColumn, Matchable> cor : record.getFirst().getRecords().get()) {
							resultCollector.next(cor);
						}
					}
				});
		
		System.out.println(String.format("Key completely mapped constraint resulted in %d correspondences", correspondences.size()));
		
		return correspondences;
	}
	
	protected void printCorrespondences(Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences, Map<Integer, Table> tables) {
		if(logCorrespondences) {
			for(Correspondence<MatchableTableColumn, Matchable> cor : Q.sort(correspondences.get(), (c1,c2)->Integer.compare(c1.getFirstRecord().getTableId(), c2.getFirstRecord().getTableId()))) {
				if(cor.getSecondRecord() instanceof MatchableLodColumn) {
					System.out.println(String.format("%.6f\t%s <-> {%d}%s\t(%d/%d)", 
						cor.getSimilarityScore(), 
						cor.getFirstRecord(), 
						cor.getSecondRecord().getDataSourceIdentifier(),
						cor.getSecondRecord(), 
						cor.getCausalCorrespondences()==null ? 0 : cor.getCausalCorrespondences().size(), 
						tables.get(cor.getFirstRecord().getTableId()).getSize()));
				}
			}
		}
	}
	
	protected void printCorrespondencesWithValues(Processable<Correspondence<MatchableTableColumn, MatchableValue>> correspondences, Map<Integer, Table> tables) {
		if(logCorrespondences) {
			for(Correspondence<MatchableTableColumn, MatchableValue> cor : Q.sort(correspondences.get(), (c1,c2)->Integer.compare(c1.getFirstRecord().getTableId(), c2.getFirstRecord().getTableId()))) {
				System.out.println(String.format("%.6f\t%s <-> {%d}%s\t(%d/%d)", 
					cor.getSimilarityScore(), 
					cor.getFirstRecord(), 
					cor.getSecondRecord().getDataSourceIdentifier(),
					cor.getSecondRecord(), 
					cor.getCausalCorrespondences().size(), 
					tables.get(cor.getFirstRecord().getTableId()).getSize()));
				
				for(Correspondence<MatchableValue, Matchable> cause : cor.getCausalCorrespondences().sort((c)->c.getSimilarityScore(),false).get()) {
					System.out.println(String.format("\t%.6f\t%s", cause.getSimilarityScore(), cause.getFirstRecord().getValue()));
				}
			}
		}
	}

	public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> linkEntitiesForAnnotation(Table entityTable, boolean addValuesFromKB, double blockFilterRatio, Map<String, String> pkToEntity) throws Exception {
		
		WebTableDataSetLoader loader = new WebTableDataSetLoader();
		
		// create a dataset of records
		DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadDataSet(entityTable);
		
		Integer classId = kb.getClassIds().get(entityTable.getMapping().getMappedClass().getFirst());
		
		// get the schema mapping
		ProcessableCollection<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = new ProcessableCollection<>();
		Map<MatchableTableColumn, MatchableTableColumn> webToEntitySchemaMapping = new HashMap<>();
		for(TableColumn c : entityTable.getColumns()) {

			MatchableTableColumn webColumn = records.getSchema().getRecord(c.getIdentifier());
			
			Pair<String, Double> mapping = entityTable.getMapping().getMappedProperty(c.getColumnIndex());
			if(mapping!=null && mapping.getFirst()!=null) {
				String property = mapping.getFirst();
				MatchableTableColumn kbColumn = kb.getSchema().getRecord(property);
				
				if(kbColumn==null) {
					System.out.println(String.format("Schema mapping: %s -> %s (%s)", webColumn, kbColumn, mapping.getFirst()));
				} else {
					System.out.println(String.format("Schema mapping: %s -> %s", webColumn, kbColumn));
					webToEntitySchemaMapping.put(webColumn, kbColumn);
					schemaCorrespondences.add(new Correspondence<MatchableTableColumn, Matchable>(webColumn, kbColumn, 1.0));
				}
				
			}
		}
		
		BlockingKeyCreatorByUniqueness blockingKeyCreator = new BlockingKeyCreatorByUniqueness();
		Map<TableColumn, Double> uniqueness = blockingKeyCreator.calculateColumnUniqueness(entityTable);
		
		MaxScoreMatchingRule<MatchableTableRow, MatchableTableColumn> matchingRule = new MaxScoreMatchingRule<>(0.0);
		Collection<TableColumn> blockingKeys = new HashSet<>();
		
		// create blocking keys and matching rules for each candidate key
		// if records have missing values, not all records can be matched by every candidate key
		for(Collection<TableColumn> candidateKey : entityTable.getSchema().getCandidateKeys()) {
			blockingKeys.addAll(blockingKeyCreator.createBlockingKey(candidateKey, uniqueness));

			// create the matching rule
			LinearCombinationMatchingRule<MatchableTableRow, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(0.3);
				
			for(TableColumn c : candidateKey) {
				
				MatchableTableColumn webColumn = records.getSchema().getRecord(c.getIdentifier());
				MatchableTableColumn kbColumn = webToEntitySchemaMapping.get(webColumn);
				
				SimilarityMeasure<?> measure = null;
				
				if(kbColumn!=null) {
					switch (kbColumn.getType()) {
					case numeric:
						measure = new DeviationSimilarity();
						break;
					case date:
						WeightedDateSimilarity dateSim = new WeightedDateSimilarity(1, 3, 5);
						dateSim.setYearRange(10);
						measure = dateSim;
						break;
					default:
	//						measure = new MaximumOfTokenContainment();
						measure = new TokenizingJaccardSimilarity();
						break;
					}
					
					MatchableTableRowToKBComparator<?> comparator = new MatchableTableRowToKBComparator<>(kbColumn, measure,0.0);
					rule.addComparator(comparator, 1.0);
				} else {
					System.out.println(String.format("No corresponding column for '%s'", webColumn));
				}
				
			}
			
			rule.normalizeWeights();
			
			System.out.println(String.format("Matching rule uses {%s}", StringUtils.join(Q.project(candidateKey, new TableColumn.ColumnHeaderProjection()), ",")));
			
			matchingRule.addMatchingRule(rule);
		}
		
		Set<Pair<String, MatchableTableColumn>> blockedLeft = new HashSet<>();
		for(MatchableTableColumn col : webToEntitySchemaMapping.keySet()) {
			blockedLeft.add(new Pair<>(null, col));
		}
		Set<Pair<String, MatchableTableColumn>> blockedRight = new HashSet<>();
		for(MatchableTableColumn col : webToEntitySchemaMapping.values()) {
			blockedRight.add(new Pair<>(null, col));
		}
		
		// create the blocker
		System.out.println(String.format("Blocking via {%s}", StringUtils.join(Q.project(webToEntitySchemaMapping.keySet(), new MatchableTableColumn.ColumnHeaderProjection()), ",")));
		MatchableTableRowTokenBlockingKeyGenerator2 bkg  = new MatchableTableRowTokenBlockingKeyGenerator2(blockedLeft);
		MatchableTableRowTokenBlockingKeyGenerator2 bkg2  = new MatchableTableRowTokenBlockingKeyGenerator2(blockedRight);
		StandardRecordBlocker<MatchableTableRow, MatchableTableColumn> blocker = new StandardRecordBlocker<>(bkg,bkg2);
//			blocker.setMeasureBlockSizes(true);
		blocker.setBlockFilterRatio(blockFilterRatio);
		blocker.setMaxBlockPairSize(1000000);

		// run the matching
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();
		DataSet<MatchableTableRow, MatchableTableColumn> kbRecords = new ParallelHashedDataSet<>(kb.getRecords().where((r)->r.getTableId()==classId).get());
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences = engine.runIdentityResolution(records, kbRecords, schemaCorrespondences, matchingRule, blocker);
		
		return correspondences;
	}

	public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> testBlocking(Collection<Table> entityTables, boolean addValuesFromKB, double blockFilterRatio, Map<String, String> pkToEntity, boolean verbose) throws Exception {
		
		WebTableDataSetLoader loader = new WebTableDataSetLoader();
		
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> result = new ParallelProcessableCollection<>();
		
		for(Table t : entityTables) {
			
			// create a dataset of records
			DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadDataSet(t);
			
			Integer classId = kb.getClassIds().get(t.getMapping().getMappedClass().getFirst());
			
			Collection<TableColumn> potentialBlockingKeys = new LinkedList<>();
			
			// get the schema mapping
			Map<MatchableTableColumn, MatchableTableColumn> webToEntitySchemaMapping = new HashMap<>();
			for(TableColumn c : t.getColumns()) {

				MatchableTableColumn webColumn = records.getSchema().getRecord(c.getIdentifier());
				
				Pair<String, Double> mapping = t.getMapping().getMappedProperty(c.getColumnIndex());
				if(mapping!=null) {
					String property = mapping.getFirst();
					MatchableTableColumn kbColumn = kb.getSchema().getRecord(property);
					
					if(kbColumn!=null) {
						webToEntitySchemaMapping.put(webColumn, kbColumn);
						potentialBlockingKeys.add(c);
					}
				}
			}
			
			BlockingKeyCreatorByUniqueness blockingKeyCreator = new BlockingKeyCreatorByUniqueness();
			Map<TableColumn, Double> uniqueness = blockingKeyCreator.calculateColumnUniqueness(t);
			Collection<TableColumn> blockingKeys = new HashSet<>();
			
			for(Collection<TableColumn> candidateKey : t.getSchema().getCandidateKeys()) {
				blockingKeys.addAll(blockingKeyCreator.createBlockingKey(potentialBlockingKeys, uniqueness));
			}
			
			Set<Pair<String, MatchableTableColumn>> blockedLeft = new HashSet<>();
			for(MatchableTableColumn col : webToEntitySchemaMapping.keySet()) {
				blockedLeft.add(new Pair<>("", col));
			}
			Set<Pair<String, MatchableTableColumn>> blockedRight = new HashSet<>();
			for(MatchableTableColumn col : webToEntitySchemaMapping.values()) {
				blockedRight.add(new Pair<>("", col));
			}
			
			// create the blocker
			System.out.println(String.format("Blocking via {%s}", StringUtils.join(Q.project(blockingKeys, new TableColumn.ColumnHeaderProjection()), ",")));
			MatchableTableRowTokenBlockingKeyGenerator2 bkg  = new MatchableTableRowTokenBlockingKeyGenerator2(blockedLeft);
			MatchableTableRowTokenBlockingKeyGenerator2 bkg2  = new MatchableTableRowTokenBlockingKeyGenerator2(blockedRight);
			StandardRecordBlocker<MatchableTableRow, MatchableTableColumn> blocker = new StandardRecordBlocker<>(bkg,bkg2);
			blocker.setMeasureBlockSizes(true);
			blocker.setDeduplicatePairs(false);
			blocker.setBlockFilterRatio(blockFilterRatio);

			DataSet<MatchableTableRow, MatchableTableColumn> kbRecords = new ParallelHashedDataSet<>(kb.getRecords().where((r)->r.getTableId()==classId).get());
			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences = blocker.runBlocking(records, kbRecords, null);
			
			result = result.append(correspondences);
		}
		
		return result;
	}
	
	public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> linkEntities(Collection<Table> entityTables, boolean addValuesFromKB, double blockFilterRatio, Map<String, String> pkToEntity, boolean verbose, boolean createOneToOneMapping, boolean useLearnedLinkageRule) throws Exception {
		
		WebTableDataSetLoader loader = new WebTableDataSetLoader();
		
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> result = new ParallelProcessableCollection<>();
		
		ProcessableCollection<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = new ProcessableCollection<>();
		
		for(Table t : entityTables) {
			
			System.out.println("***************************************************");
			System.out.println(String.format("*** EntityTableMatcher.linkEntities: %s", t.getPath()));
			System.out.println("***************************************************");

			// create a dataset of records
			DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadDataSet(t);
			
			Integer classId = kb.getClassIds().get(t.getMapping().getMappedClass().getFirst());
			
			// get the schema mapping
			Map<MatchableTableColumn, MatchableTableColumn> webToEntitySchemaMapping = new HashMap<>();
			for(TableColumn c : t.getColumns()) {

				MatchableTableColumn webColumn = records.getSchema().getRecord(c.getIdentifier());
				
				Pair<String, Double> mapping = t.getMapping().getMappedProperty(c.getColumnIndex());
				if(mapping!=null) {
					String property = mapping.getFirst();
					MatchableTableColumn kbColumn = kb.getSchema().getRecord(property);
					
					if(kbColumn!=null) {
						webToEntitySchemaMapping.put(webColumn, kbColumn);
						schemaCorrespondences.add(new Correspondence<MatchableTableColumn, Matchable>(webColumn, kbColumn, 1.0));

						System.out.println(String.format("\t%s -> %s", webColumn.toString(), kbColumn.toString()));
					}
				}
			}
			
			BlockingKeyCreatorByUniqueness blockingKeyCreator = new BlockingKeyCreatorByUniqueness();
			Map<TableColumn, Double> uniqueness = blockingKeyCreator.calculateColumnUniqueness(t);
			
			MaxScoreMatchingRule<MatchableTableRow, MatchableTableColumn> matchingRule = new MaxScoreMatchingRule<>(0.0);
			Collection<TableColumn> blockingKeys = new HashSet<>();
			
			// create blocking keys and matching rules for each candidate key
			// if records have missing values, not all records can be matched by every candidate key
			for(Collection<TableColumn> candidateKey : t.getSchema().getCandidateKeys()) {
				blockingKeys.addAll(blockingKeyCreator.createBlockingKey(candidateKey, uniqueness));

				// create the matching rule
				LinearCombinationMatchingRule<MatchableTableRow, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(0.6);
					
				for(TableColumn c : candidateKey) {
					
					MatchableTableColumn webColumn = records.getSchema().getRecord(c.getIdentifier());
					MatchableTableColumn kbColumn = webToEntitySchemaMapping.get(webColumn);
					
					SimilarityMeasure<?> measure = null;
					
					if(kbColumn!=null) {
						switch (kbColumn.getType()) {
						case numeric:
							measure = new DeviationSimilarity();
							break;
						case date:
							WeightedDateSimilarity dateSim = new WeightedDateSimilarity(1, 3, 5);
							dateSim.setYearRange(10);
							measure = dateSim;
							break;
						default:
	//						measure = new MaximumOfTokenContainment();
							measure = new TokenizingJaccardSimilarity();
							break;
						}
						
						MatchableTableRowToKBComparator<?> comparator = new MatchableTableRowToKBComparator<>(kbColumn, measure,0.0);
						rule.addComparator(comparator, 1.0);
					}
				}
				
				rule.normalizeWeights();
				
				System.out.println(String.format("\tMatching rule uses {%s}", StringUtils.join(Q.project(candidateKey, new TableColumn.ColumnHeaderProjection()), ",")));
				
				matchingRule.addMatchingRule(rule);
			}
			
			Set<Pair<String, MatchableTableColumn>> blockedLeft = new HashSet<>();
			for(MatchableTableColumn col : webToEntitySchemaMapping.keySet()) {
				blockedLeft.add(new Pair<>("", col));
			}
			Set<Pair<String, MatchableTableColumn>> blockedRight = new HashSet<>();
			for(MatchableTableColumn col : webToEntitySchemaMapping.values()) {
				blockedRight.add(new Pair<>("", col));
			}
			
			DataSet<MatchableTableRow, MatchableTableColumn> kbRecords = new ParallelHashedDataSet<>(kb.getRecords().where((r)->r.getTableId()==classId).get());
			Processable<MatchableTableColumn> targetSchema = kb.getSchema().where((c)->c.getTableId()==classId);
			MatchableTableColumn labelColumn = targetSchema.where((c)->KnowledgeBase.RDFS_LABEL.equals(c.getHeader())).firstOrNull();

			TFIDF<String> weights = new TFIDF<>(
				(s)->s.split(" "),
				VectorCreationMethod.TFIDF);
			
			weights.prepare(kbRecords, (record,col)->{
				Object label = record.get(labelColumn.getColumnIndex());
				Set<String> values = new HashSet<>();
				if(label instanceof String[]) {
					values.addAll(Arrays.asList((String[])label));
				} else {
					values.add((String)label);
				}
	
				for(String value : values) {
					if(value!=null) {
						col.next(MatchableTableRowToKBComparator.preprocessKBString(value));
					}
				}
			});

			// create the blocker
			System.out.println(String.format("\tBlocking via {%s}", StringUtils.join(Q.project(webToEntitySchemaMapping.keySet(), new MatchableTableColumn.ColumnHeaderProjection()), ",")));
			MatchableTableRowTokenBlockingKeyGenerator2 bkg  = new MatchableTableRowTokenBlockingKeyGenerator2(blockedLeft);
			MatchableTableRowTokenBlockingKeyGenerator2 bkg2  = new MatchableTableRowTokenBlockingKeyGenerator2(blockedRight);
			StandardRecordBlocker<MatchableTableRow, MatchableTableColumn> blocker = new StandardRecordBlocker<>(bkg,bkg2);
//			blocker.setMeasureBlockSizes(true);
			blocker.setBlockFilterRatio(blockFilterRatio);
			blocker.setMaxBlockPairSize(1000000);

			MatchingRule<MatchableTableRow, MatchableTableColumn> effectiveRule = matchingRule;
			if(useLearnedLinkageRule && t.getMapping()!=null && t.getMapping().getMappedClass()!=null) {
				File f = new File(entityDefinitionLocation, "matching_rules/" + t.getMapping().getMappedClass().getFirst());
				if(f.exists()) {
					String className = t.getMapping().getMappedClass().getFirst();
					System.out.println(String.format("\tLoading matching rule for class %s", className));
					int cls = kb.getClassIds().get(className);
					
					MatchingRuleGenerator ruleGenerator = new MatchingRuleGenerator(kb, 0.5);
					ruleGenerator.setWeights(weights);
					WekaMatchingRule<MatchableTableRow, MatchableTableColumn> wekaRule = ruleGenerator.createWekaMatchingRule(targetSchema, schemaCorrespondences, "SimpleLogistic", new String[] {});
					wekaRule.readModel(f);
					System.out.println(String.format("\tLoaded matching rule:\n%s", wekaRule.getModelDescription()));
					effectiveRule = wekaRule;
				}
			}
			System.out.println(String.format("\tUsing matching rule: %s", effectiveRule.toString()));
			
			// run the matching
			MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();
			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences = engine.runIdentityResolution(records, kbRecords, schemaCorrespondences, effectiveRule, blocker);

			if(createOneToOneMapping) {
				//run 1:1 matching, as no two records from the entity table should link to the same entity
				GreedyOneToOneMatchingAlgorithm<MatchableTableRow, MatchableTableColumn> oneToOne = new GreedyOneToOneMatchingAlgorithm<>(correspondences);
				oneToOne.run();
				correspondences = oneToOne.getResult();
				
				System.out.println(String.format("\t%d correspondences after greedy 1:1 mapping", correspondences.size()));
			}
			
			if(verbose) {
				for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : correspondences
	//					.where((c)->c.getSimilarityScore()<1.0)
						.sort((c)->c.getFirstRecord().getIdentifier(),false)
						.get()) {
					System.out.println(String.format("\tMatch %.6f:\n\t%s\n\t%s", cor.getSimilarityScore(), cor.getFirstRecord().format(50), cor.getSecondRecord().format(50)));
					
					Pair<String, Double> correct = t.getMapping().getMappedInstance(cor.getFirstRecord().getRowNumber());
					if(correct!=null) {
						if(correct.getFirst().startsWith("http")) {
							MatchableTableRow record = kb.getRecords().getRecord(String.format("%s.csv:%s", t.getMapping().getMappedClass().getFirst(), correct.getFirst()));
							if(record!=null) {
	//							System.out.println(String.format("Match %.6f:\n\t%s\n\t%s", cor.getSimilarityScore(), cor.getFirstRecord().format(50), cor.getSecondRecord().format(50)));
								System.out.println(String.format("\t  ->\t%s", record.format(50)));
							} else {
								System.out.println(String.format("\t ->\t%s (not in entity definition)", correct.getFirst()));
							}
						} else {
							System.out.println(String.format("\t ->\t%s", correct.getFirst()));
						}
					}
				}
			}
			
			System.out.println(String.format("\tFound %d existing entities, %d new entities.", correspondences.size(), t.getSize()-correspondences.size()));
			
			// add URI column
			TableColumn uriColumn = new TableColumn(t.getColumns().size(), t);
			uriColumn.setDataType(DataType.string);
			uriColumn.setHeader("URI");
			t.insertColumn(uriColumn.getColumnIndex(), uriColumn);
			
			// add values from KB to entity table for evaluation
			Map<MatchableTableColumn, TableColumn> kbColumnsToWebColumns = new HashMap<>();
			if(addValuesFromKB) {
				for(MatchableTableColumn kbC : kb.getSchema().where((c)->c.getTableId()==classId).get()) {
					if(!kbC.getHeader().equals("URI")) {
						TableColumn webC = new TableColumn(t.getColumns().size(), t);
						webC.setDataType(kbC.getType());
						webC.setHeader(kbC.getHeader());
						t.insertColumn(webC.getColumnIndex(), webC);
						kbColumnsToWebColumns.put(kbC, webC);
					}
				}
			}
			
			// fill added columns with values from correspondences
			for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : correspondences.get()) {
				TableRow row = t.get(cor.getFirstRecord().getRowNumber());
				String uri = cor.getSecondRecord().getIdentifier();
				row.set(uriColumn.getColumnIndex(), uri);
				t.getMapping().setMappedInstance(row.getRowNumber(), new Pair<>(uri,cor.getSimilarityScore()));
				
				pkToEntity.put(row.get(0).toString(), cor.getSecondRecord().getIdentifier());
				
				for(MatchableTableColumn kbC : kbColumnsToWebColumns.keySet()) {
					TableColumn webC = kbColumnsToWebColumns.get(kbC);
					row.set(webC.getColumnIndex(), cor.getSecondRecord().get(kbC.getColumnIndex()));
				}
			}
			
			result = result.append(correspondences);
		}
		
		return result;
	}

	public Performance learnMatchingRule(Collection<Table> tables, double blockFilterRatio, MatchingGoldStandard gs, boolean verbose, boolean createOneToOneMapping, boolean saveModel) throws Exception {
		
		WebTableDataSetLoader loader = new WebTableDataSetLoader();

		System.out.println(String.format("EntityTableMatcher.learnMatchingRule] loading data set from tables: %s", 
				StringUtils.join(Q.project(tables, (t)->t.getPath()), ", ")
				));
		// create a dataset of records
		DataSet<MatchableTableRow, MatchableTableColumn> records = loader.loadRowDataSet(tables);
		
		WekaMatchingRule<MatchableTableRow, MatchableTableColumn> wekaRule = null;
		Integer classId = null;
		String className = null;
		Map<MatchableTableColumn, MatchableTableColumn> webToEntitySchemaMapping = new HashMap<>();
		
		ProcessableCollection<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = new ProcessableCollection<>();
		
		for(Table t : tables) {
			System.out.println(String.format("EntityTableMatcher.learnMatchingRule] Generating schema mapping for table %s", t.getPath()));
			
			if(classId==null) {
				classId = kb.getClassIds().get(t.getMapping().getMappedClass().getFirst());
				className = t.getMapping().getMappedClass().getFirst();
			}
			
			// get the schema mapping
			for(TableColumn c : t.getColumns()) {
	
				MatchableTableColumn webColumn = records.getSchema().getRecord(c.getIdentifier());
				
				Pair<String, Double> mapping = t.getMapping().getMappedProperty(c.getColumnIndex());
				if(mapping!=null) {
					String property = mapping.getFirst();
					MatchableTableColumn kbColumn = kb.getSchema().getRecord(property);
					
					if(kbColumn!=null) {
						webToEntitySchemaMapping.put(webColumn, kbColumn);
						schemaCorrespondences.add(new Correspondence<MatchableTableColumn, Matchable>(webColumn, kbColumn, 1.0));
						System.out.println(String.format("Schema mapping: %s -> %s (%s)", webColumn, kbColumn, property));
					} else {
						System.out.println(String.format("Schema mapping: %s -> %s", webColumn, kbColumn));
					}
				}
			}
		}
		
		if(classId==null) {
			return new Performance(0, 0, 0);
		}
		
		final int cls = classId;
		System.out.println(String.format("[EntityTableMatcher.learnMatchingRule] Preparing data set for class %s", className));
		DataSet<MatchableTableRow, MatchableTableColumn> kbRecords = new ParallelHashedDataSet<>(kb.getRecords().where((r)->r.getTableId()==cls).get());
		Processable<MatchableTableColumn> targetSchema = kb.getSchema().where((c)->c.getTableId()==cls);
		
		MatchableTableColumn labelColumn = targetSchema.where((c)->KnowledgeBase.RDFS_LABEL.equals(c.getHeader())).firstOrNull();

		TFIDF<String> weights = new TFIDF<>(
			(s)->s.split(" "),
			VectorCreationMethod.TFIDF);
		
		weights.prepare(kbRecords, (record,col)->{
			Object label = record.get(labelColumn.getColumnIndex());
			Set<String> values = new HashSet<>();
			if(label instanceof String[]) {
				values.addAll(Arrays.asList((String[])label));
			} else {
				values.add((String)label);
			}

			for(String value : values) {
				if(value!=null) {
					col.next(MatchableTableRowToKBComparator.preprocessKBString(value));
				}
			}
		});

		System.out.println(String.format("[EntityTableMatcher.learnMatchingRule] Generating matching rule for target schema %s", 
				Q.project(targetSchema.sort((c)->c.getColumnIndex()).get(), (c)->c.getHeader())
				));
		
		MatchingRuleGenerator ruleGenerator = new MatchingRuleGenerator(kb, 0.5);
		ruleGenerator.setWeights(weights);
		wekaRule = ruleGenerator.createWekaMatchingRule(targetSchema, schemaCorrespondences, "SimpleLogistic", new String[] {});

		System.out.println(String.format("[EntityTableMatcher.learnMatchingRule] learning matching rule: %s", wekaRule.toString()));
		
		RuleLearner<MatchableTableRow, MatchableTableColumn> learner = new RuleLearner<>();
		FeatureVectorDataSet features = learner.generateTrainingDataForLearning(records, kbRecords, gs, wekaRule, schemaCorrespondences);
		// FeatureVectorDataSet deduplicatedFeatures = learner.deduplicateFeatureDataSet(features);
		// features = deduplicatedFeatures;
		// if(deduplicatedFeatures.size() > 100) {

		// 	features = deduplicatedFeatures;
		// } else {
		// 	System.out.println(String.format("[EntityTableMatcher.learnMatchingRule] less than 100 deduplicated examples (%d), using original data", deduplicatedFeatures.size()));
		// 	// System.out.println(String.format("EntityTableMatcher.learnMatchingRule] less than 100 deduplicated examples (%d), sampling random examples", deduplicatedFeatures.size()));
		// 	// List<Record> list = new ArrayList<>(features.get());
		// 	// Random r = new Random();
		// 	// for(int i = 0; i < Math.min(1000, features.size()); i++) {
		// 	// 	int index = r.nextInt(list.size());
		// 	// 	deduplicatedFeatures.add(list.get(index));
		// 	// }
		// 	// features = deduplicatedFeatures;
		// }
		new RecordCSVFormatter().writeCSV(new File(logDirectory, className + "matching_rule_features.csv"), features, null);
		
		if(features.size()==0) {
			return new Performance(0, 0, 0);
		}
		
//		Performance p = learner.learnMatchingRule(records, kbRecords, null, wekaRule, gs);
		// wekaRule.setBackwardSelection(true);
		// wekaRule.setForwardSelection(true);
		wekaRule.setBalanceTrainingData(true);
		Performance p = wekaRule.learnParameters(features);
		System.out.println(String.format("Performance after rule learning:\n\tPrecision:\t%f\n\tRecall:\t%f\n\tF1-measure:\t%f", p.getPrecision(), p.getRecall(), p.getF1()));
		System.out.println(wekaRule.getModelDescription());
		
		wekaRule.exportModel(new File(entityDefinitionLocation, "matching_rules/" + className));
		
		return p;
	}

	protected EntityTable getReferenceEntityTable(Table t) {
		if(referenceEntityTables!=null) {
			for(EntityTable et : referenceEntityTables) {
				if(!et.isNonEntity()) {
					Collection<String> referenceAttributes = Q.project(Q.union(et.getAttributes().values()), new MatchableTableColumn.ColumnIdProjection());
					Collection<String> tableAttributes = Q.project(t.getColumns(), new TableColumn.ColumnIdentifierProjection());
					
					if(Q.intersection(referenceAttributes, tableAttributes).size()>0) {
						return et;
					}
				}
			}
		}
			
		return null;
	}
}
