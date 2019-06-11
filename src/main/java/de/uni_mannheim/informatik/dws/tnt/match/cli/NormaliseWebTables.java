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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.snow.SchemaNormalisation;
import de.uni_mannheim.informatik.dws.snow.SchemaNormalisation.MatcherType;
import de.uni_mannheim.informatik.dws.snow.SchemaNormalisation.NormalisationHeuristic;
import de.uni_mannheim.informatik.dws.snow.SchemaNormalisation.StitchingMethod;
import de.uni_mannheim.informatik.dws.snow.SnowOutput;
import de.uni_mannheim.informatik.dws.snow.SnowPerformance;
import de.uni_mannheim.informatik.dws.snow.SnowRuntime;
import de.uni_mannheim.informatik.dws.snow.SnowStatisticsWriter;
import de.uni_mannheim.informatik.dws.snow.SnowTableToTableEvaluator;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.CorrespondenceFormatter;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.TableReconstructor;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.EntityTableDefinitionEvaluator;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.CandidateKeyBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.DeterminantBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.EntityLabelBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.ImprovedValueBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.LabelBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.TableToTableMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.ValueBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.preprocessing.CreateCorrespondencesForGeneratedColumns;
// import de.uni_mannheim.informatik.dws.tnt.match.preprocessing.TableDisambiguationExtractor;
//import de.uni_mannheim.informatik.dws.tnt.match.preprocessing.TableNumberingExtractor;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.ColumnTranslator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.GraphBasedRefinement;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.TransitivityMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.UnionTablesCorrespondenceGenerator;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.parallel.Parallel;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.FunctionalDependencyDiscovery;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.preprocessing.TableDisambiguationExtractor;
import de.uni_mannheim.informatik.dws.winter.webtables.preprocessing.TableNumberingExtractor;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;
import edu.stanford.nlp.util.StringUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class NormaliseWebTables extends Executable {

	
	
	@Parameter(names = "-normalisation")
	private NormalisationHeuristic normalisationHeuristic = NormalisationHeuristic.LargestDeterminant;
	
	@Parameter(names = "-stitching", required=true)
	private StitchingMethod stitchingMethod;
	
	// @Parameter(names = "-finalStitching", required=true)
	// private StitchingMethod finalStitchingMethod;

	@Parameter(names = "-matcher")
	private MatcherType matcher = MatcherType.NonTrivialFull;
	
	@Parameter(names = "-web", required=true)
	private String webLocation;
	
	@Parameter(names = "-results", required=true)
	private String resultLocation;
	
	@Parameter(names = "-csv", required=true)
	private String csvLocation;
	
	@Parameter(names = "-correspondences")
	private String correspondencesLocation;
	
	@Parameter(names = "-contextCorrespondences")
	private String contextCorrespondencesLocation;

	@Parameter(names = "-serialise")
	private boolean serialise;
	
	@Parameter(names = "-entityStructure")
	private String entityStructureLocation;	// a file that defines the keys for the union tables' schema
	
	@Parameter(names = "-entityDefinition")
	private String entityDefinitionLocation; // a folder that contains prototype tables for different types of entities
	
	@Parameter(names = "-candidateKeys")
	private String candidateKeysLocation;
	
	@Parameter(names = "-relations")
	private String relationsLocation;
	
	@Parameter(names = "-stitchedRelations")
	private String stitchedRelationsLocation;

	@Parameter(names = "-functionalDependencies")
	private String functionalDependencyLocation;

	@Parameter(names = "-extractPredefinedRelations")
	private boolean extractPredefinedRelations = false;

	@Parameter(names = "-normalise")
	private boolean normaliseRelations = false;
	
	@Parameter(names = "-binary")
	private boolean binaryRelationsOnly = false;
	
	@Parameter(names = "-logEntityDetection")
	private boolean logEntityDetection = false;
	
	@Parameter(names = "-logTableStatistics")
	private boolean logTableStatistics = false;

	@Parameter(names = "-logFDStitching")
	private boolean logFDStitching = false;
	
	@Parameter(names = "-minEntities")
	private int minNumberOfEntities = 0;
	
	@Parameter(names = "-minInstanceCorrespondences")
	private int minNumberOfInstanceCorrespondences = 25;

	@Parameter(names = "-minInstanceMatchRatio")
	private double minInstanceMatchRatio = 0.0;
	
	@Parameter(names = "-minDensity")
	private double minDensity = 0.0;
	
	@Parameter(names = "-matchTables", description = "If true, the table-to-table matching step is always executed (except if -skipT2T is true). Correspondences loaded via the -correspondences parameter are then used for evaluation.")
	private boolean matchTables = false;

	@Parameter(names = "-skipT2T", description = "If true, the table-to-table matching step is skipped")
	private boolean skipTableToTableMatching = false;

	@Parameter(names = "-useOriginalUnion", description = "Uses Union Tables instead of Clustered Union Tables")
	private boolean useOriginalUnion = false;
	
	@Parameter(names = "-minFDApproximation")
	private double minFDApproximationRate = 1.0;
	
	@Parameter(names = "-testFK")
	private boolean testForeignKeys = false;
	
	@Parameter(names = "-useEntityLabel")
	private boolean useEntityLabelDetection = false;
	
	@Parameter(names = "-logFiltering")
	private boolean logFiltering = false;

	@Parameter(names = "-heuristicDependencyPropagation")
	private boolean heuristicDependencyPropagation;

	@Parameter(names = "-assumeBinaryRelations", description = "Instead of FD discovery, a subject column detection is used and all non-subject columns are considered as binary relations.")
	private boolean assumeBinaryRelations = false;

	@Parameter(names = "-discoverDependencies")
	private boolean discoverDependencies = false;

	@Parameter(names = "-evaluateRefinement")
	private boolean evaluateRefinement = false;

	@Parameter(names = "-t2tRefinement")
	private double t2tRefinementThreshold = 0.0;

	@Parameter(names = "-skipStitching", description = "Stops the execution after the matching step, i.e., the stitching step is skipped")
	private boolean skipStitching = false;

	@Parameter(names = "-noFDStitching", description = "The FD stitching step is skipped and no universal schema is created.")
	private boolean noFDStitching = false;

	@Parameter(names = "-doNotDeduplicateInstanceCandidates")
	private boolean doNotDeduplicateInstanceCandidates = false;

	@Parameter(names = "-noContextColumns", description = "All context columns are ignored")
	private boolean noContextColumns = false;

	@Parameter(names = "-evaluateNormalisation")
	private boolean evaluateNormalisation = false;

	@Parameter(names = "-taneRoot", description = "root directory of the tane implementation")
	private String taneRoot;

	public static void main(String[] args) throws Exception {
		NormaliseWebTables app = new NormaliseWebTables();
		
		if(app.parseCommandLine(NormaliseWebTables.class, args)) {
			app.run();
		}
	}
	
	private String generateFDConfiguationIdentifier() {
		String fdConfig = "";
		if(heuristicDependencyPropagation) {
			fdConfig = "heuristic";
		} else if(assumeBinaryRelations) {
			fdConfig = "binary";
		} else if(discoverDependencies) {
			fdConfig = "discover";
		} else {
			fdConfig = "basic";
		}
		return fdConfig;
	}

	private void sayHello(String configuration) {

		
		System.out.println("\t\t   .      .                         .      .      ");
		System.out.println("\t\t   _\\/  \\/_                         _\\/  \\/_      ");
		System.out.println("\t\t    _\\/\\/_                           _\\/\\/_       ");
		System.out.println("\t\t_\\_\\_\\/\\/_/_/_    SNoW v1.0       _\\_\\_\\/\\/_/_/_  ");
		System.out.println("\t\t/ /_/\\/\\_\\ \\                      / /_/\\/\\_\\ \\    ");
		System.out.println("\t\t    _/\\/\\_                           _/\\/\\_       ");
		System.out.println("\t\t    /\\  /\\                           /\\  /\\       ");
		System.out.println("\t\t   '      '                         '      '      ");
		System.out.println(String.format("*** configuration: %s ***", configuration));

	}

	public void run() throws Exception {
		Parallel.setReportIfStuck(false);

		// create configuration identifier
		String configuration = String.format("%s-context*%s-union*%s-t2t*%s-t2k*fd-%s*%s*eval-%s",
			noContextColumns ? "no" : "extract",
			useOriginalUnion ? "original" : "clustered",
			skipTableToTableMatching ? "skip" : correspondencesLocation==null || matchTables ? "match" : "load",
			entityDefinitionLocation==null ? "load" : "match",
			generateFDConfiguationIdentifier(),
			noFDStitching ? "no-fd-stitching" : "",
			evaluateNormalisation ? "norm" : "fd"
		);
		sayHello(configuration);

		System.out.println();

		if(taneRoot!=null) {
			FunctionalDependencyDiscovery.taneRoot = new File(taneRoot);
		}

		int firstTableId = 0;
		if(entityDefinitionLocation!=null) {
			// make sure the web table ids and the entity definition ids do not overlap
			firstTableId = new File(entityDefinitionLocation, "tables/").list().length;
			System.out.println(String.format("%d tables in target knowledge base", firstTableId));
		}
		
		File webLocationFile = new File(webLocation);
		File logDirectory = webLocationFile.getParentFile();
		File csvLocationFile = new File(csvLocation);
		csvLocationFile.mkdirs();
		String taskName = webLocationFile.getParentFile().getName();
		
		SnowPerformance performanceLog = new SnowPerformance();
		SnowRuntime runtime = new SnowRuntime("Normalise web tables");
		runtime.startMeasurement();

		/*****************************************************************************************
		 * LOAD WEB TABLES
		 ****************************************************************************************/
		System.err.println("Loading Web Tables");
		SnowRuntime rt = runtime.startMeasurement("Load web tables");
		WebTables web = WebTables.loadWebTables(webLocationFile, true, false, false, serialise, firstTableId);
		web.removeHorizontallyStackedTables();
		// web.printSchemata();
		rt.endMeasurement();
		
		// remove provenance data for original web tables
		Map<String, Integer> originalWebTableColumnCount = new HashMap<>();
		Map<String, Integer> originalWebTableColumnCountByTable = new HashMap<>();
		Map<String, Integer> originalWebTablesCount = new HashMap<>();
		for(Table t : web.getTables().values()) {
			Set<String> tblProv = new HashSet<>();
			for(TableColumn c : t.getColumns()) {
				for(String prov : c.getProvenance()) {
					tblProv.add(prov.split("~")[0]);
				}

				originalWebTableColumnCount.put(c.getIdentifier(), c.getProvenance().size());
				MapUtils.add(originalWebTableColumnCountByTable, t.getPath(), c.getProvenance().size());
				c.getProvenance().clear();
			}
			originalWebTablesCount.put(t.getPath(), tblProv.size());
			for(TableRow r : t.getRows()) {
				r.getProvenance().clear();
			}
		}

		/*****************************************************************************************
		 * RECONSTRUCT ORIGINAL UNION TABLES (optional)
		 ****************************************************************************************/
		Map<String, String> columnIdTranslation = null;
		if(useOriginalUnion) {
			columnIdTranslation = new HashMap<>();
			System.out.println("Generating correspondences for original union tables");
			UnionTablesCorrespondenceGenerator gen = new UnionTablesCorrespondenceGenerator();
			Processable<Correspondence<MatchableTableColumn, Matchable>> unionCorrespondences = gen.GenerateCorrespondencesForOriginalUnionTables(web);
			CorrespondenceFormatter.printAttributeClusters(unionCorrespondences);
			System.out.println("Reconstructing original union tables");
			TableReconstructor tr = new TableReconstructor(web.getTables());
			tr.setKeepUnchangedTables(true);
			tr.setOverlappingAttributesOnly(false);
			tr.setClusterByOverlappingAttributes(false);
			tr.setMergeToSet(false);
			tr.setCreateNonOverlappingClusters(true);
			tr.setVerifyDependencies(false);	

			Collection<Table> result = tr.reconstruct(Q.<Integer>max(web.getTables().keySet())+1, web.getRecords(), web.getSchema(), web.getCandidateKeys(), new ParallelProcessableCollection<>(), unionCorrespondences);
			web.getTables().clear();

			Map<String, Integer> originalWebTableColumnCountOU = new HashMap<>();
			Map<String, Integer> originalWebTableColumnCountByTableOU = new HashMap<>();
			Map<String, Integer> originalWebTablesCountOU = new HashMap<>();
			for(Table t : result) {
				web.getTables().put(t.getTableId(), t);
				int tblCnt = 0, colCntByTbl = 0;
				System.out.println(String.format("Original Union Table %s stitched from Clustered Union Tables: {%s}",
					t.getPath(),
					StringUtils.join(t.getProvenance(), ",")
				));
				
				
				for(TableColumn c : t.getColumns()) {
					if(c.getProvenance().size()==0) {
						c.addProvenanceForColumn(c);
					}

					MapUtils.add(originalWebTableColumnCountOU, c.getIdentifier(), 0); // make sure every column appears in the map
					for(String prov : c.getProvenance()) {
						MapUtils.add(originalWebTableColumnCountOU, c.getIdentifier(), originalWebTableColumnCount.get(prov));
						columnIdTranslation.put(prov, c.getIdentifier());
						System.out.println(String.format("Column-ID translation: %s -> %s", prov, c.getIdentifier()));
					}
				}
				for(String prov : t.getProvenance()) {
					tblCnt += originalWebTablesCount.get(prov);
					colCntByTbl += originalWebTableColumnCountByTable.get(prov);
				}
				for(TableColumn c : t.getColumns()) {
					c.getProvenance().clear();
				}
				for(TableRow r : t.getRows()) {
					r.getProvenance().clear();
				}
				originalWebTablesCountOU.put(t.getPath(), tblCnt);
				
				originalWebTableColumnCountByTableOU.put(t.getPath(), colCntByTbl);
			}
			originalWebTableColumnCount = originalWebTableColumnCountOU;
			originalWebTableColumnCountByTable = originalWebTableColumnCountByTableOU;
			originalWebTablesCount = originalWebTablesCountOU;
			web.reloadSchema();
			web.reloadRecords();

			web.printSchemata(true);
		}
		
		/*****************************************************************************************
		 * TABLE PRE-PROCESSING
		 ****************************************************************************************/
		rt = runtime.startMeasurement("Web table preprocessing");
		Map<Table, Integer> tableColumnsBeforePreprocessing = new HashMap<>();
		for(Table t : web.getTables().values()) {
			tableColumnsBeforePreprocessing.put(t, t.getColumns().size());
		}
		TableDisambiguationExtractor dis = new TableDisambiguationExtractor();
		Map<Integer, Map<Integer, TableColumn>> tableToColumnToDisambiguation = null;
		TableNumberingExtractor num = new TableNumberingExtractor();
		Map<Integer, Map<Integer, TableColumn>> tableToColumnToNumbering = null;

		if(!noContextColumns) {
			tableToColumnToDisambiguation = dis.extractDisambiguations(web.getTables().values());
			tableToColumnToNumbering = num.extractNumbering(web.getTables().values());
		} else {
			// remove values which would be extracted into context columns, so the existing columns have the same values
			dis.removeDisambiguations(web.getTables().values());
			num.removeNumbering(web.getTables().values());
			
			// remove all existing context columns
			for(Table t : web.getTables().values()) {
				for(TableColumn c : Q.toArrayFromCollection(t.getColumns(), TableColumn.class)) {
					if(ContextColumns.isContextColumn(c)) {
						// removing the column changes the column index of the remaining columns
						// but the indices are used to define the gold standard, so we cannot change them before the gs is loaded
						//t.removeColumn(c);
						// hence just set all values to null
						for(TableRow r : t.getRows()) {
							r.set(c.getColumnIndex(), null);
						}
					}
				}
			}
		}

		// convert values into their data types after extracting the additional columns
		// also infer the data type for the new columns
		for(Table t : web.getTables().values()) {
			t.inferSchemaAndConvertValues();
		}

		web.reloadSchema();
		web.reloadRecords();
		rt.endMeasurement();
		// web.printSchemata(true);

		/*****************************************************************************************
		 * SCHEMA MATCHING (Table-to-Table)
		 ****************************************************************************************/
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = new ParallelProcessableCollection<>();
		Processable<Correspondence<MatchableTableColumn, Matchable>> reference = null;
		boolean updateReference = false;

		if(correspondencesLocation!=null) {
			// don't rename the columns if we apply the table-to-table matcher, which uses column headers as features!
			reference = loadCorrespondences(correspondencesLocation, contextCorrespondencesLocation, web.getSchema(), false, web.getTables(), columnIdTranslation);
		}

		if(!skipTableToTableMatching) {
			if(correspondencesLocation==null || matchTables) {
				rt = runtime.startMeasurement("Table-to-table matching");
				schemaCorrespondences = runTableMatching(web, logDirectory);
				System.out.println(String.format("Table-to-table matching resulted in %d correspondences", schemaCorrespondences.size()));
				rt.endMeasurement();

				// remember to apply the post-processing of schemaCorrespondences to reference, too. Otherwise the evaluation will be incorrect
				updateReference = reference!=null;			
			} else {
				schemaCorrespondences = reference;
				updateReference = false;
			}
			CorrespondenceFormatter.printAttributeClusters(schemaCorrespondences);

			if(!noContextColumns) {
				/*****************************************************************************************
				 * Load correspondences created during clustered union stitching
				 ****************************************************************************************/
				if(contextCorrespondencesLocation!=null && new File(contextCorrespondencesLocation).exists()) {
					// load the correspondences for context columns which were created during clustered union stitching
					Processable<Correspondence<MatchableTableColumn, Matchable>> contextCorrespondences = loadContextCorrespondences(contextCorrespondencesLocation, web.getSchema());

					schemaCorrespondences = addContextCorrespondences(schemaCorrespondences, contextCorrespondences, web);

					if(updateReference) {
						reference = addContextCorrespondences(reference, contextCorrespondences, web);
					}

					CorrespondenceFormatter.printAttributeClusters(schemaCorrespondences);
				}

				/*****************************************************************************************
				 * Create correspondences for generated context columns (numbering, disambiguation)
				 ****************************************************************************************/
				// why do we calculate transitivity here and not after adding all generated correspondences?
				// - it is needed when generating correspondences for generated columns
				rt = runtime.startMeasurement("Correspondence closure calculation");
				TransitivityMatcher<MatchableTableColumn> transitivity = new TransitivityMatcher<>();
				schemaCorrespondences = transitivity.run(schemaCorrespondences);
				if(updateReference) {
					reference = transitivity.run(reference);
				}
				System.out.println(String.format("Transitivity resulted in %d correspondences", schemaCorrespondences.size()));
				CorrespondenceFormatter.printAttributeClusters(schemaCorrespondences);
				rt.endMeasurement();

				// create correspondences for all created context columns: disambiguations, numberings
				schemaCorrespondences = createCorrespondencesForGeneratedColumns(schemaCorrespondences, tableToColumnToDisambiguation, dis, tableToColumnToNumbering, web);
				if(updateReference) {
					reference = createCorrespondencesForGeneratedColumnsInReference(reference, tableToColumnToDisambiguation, tableToColumnToNumbering, web);
				}
			}

			if(matchTables && reference!=null) {
				/*****************************************************************************************
				 * Evaluate Table-to-Table Matching
				 ****************************************************************************************/
				SnowTableToTableEvaluator t2tEval = new SnowTableToTableEvaluator();
				t2tEval.evaluateTableToTableMatching(schemaCorrespondences, reference, originalWebTablesCount, logDirectory, taskName, performanceLog, "initial", web.getSchema());
			}
		}
		
		web.printSchemata(true);

		SnowStatisticsWriter.writeInputStatistics(logDirectory, taskName, web.getTables().values(), originalWebTablesCount, originalWebTableColumnCountByTable, tableColumnsBeforePreprocessing);
		
		if(logTableStatistics) {
			web.printDensityReport();
		}
		
		/*****************************************************************************************
		 * NORMALISATION
		 ****************************************************************************************/
		SchemaNormalisation snow = new SchemaNormalisation(web, taskName, csvLocationFile, logDirectory, normalisationHeuristic, stitchingMethod, entityDefinitionLocation, minNumberOfEntities, minDensity, binaryRelationsOnly, minFDApproximationRate, originalWebTableColumnCount, originalWebTablesCount, useEntityLabelDetection, minNumberOfInstanceCorrespondences, configuration, reference);
		snow.setLogEntityDetection(logEntityDetection);
		snow.setLogFiltering(logFiltering);
		snow.setLogFDStitching(logFDStitching);
		snow.setHeuristicDependencyPropagation(heuristicDependencyPropagation);
		snow.setAssumeBinary(assumeBinaryRelations);
		snow.setDiscoverFDs(discoverDependencies);
		snow.setEvaluateRefinementThresholds(evaluateRefinement);
		snow.setStopAfterMatching(skipStitching);
		snow.setMinInstanceMatchRatio(minInstanceMatchRatio);
		snow.setDoNotDeduplicateInstanceCandidates(doNotDeduplicateInstanceCandidates);
		snow.setSkipTableToTableMatching(skipTableToTableMatching | noFDStitching);
		snow.setSkipT2T(skipTableToTableMatching);
		snow.setEvaluateNormalisation(evaluateNormalisation);

		if(entityStructureLocation!=null) {
			snow.loadEntityGroups(entityStructureLocation, candidateKeysLocation, relationsLocation, stitchedRelationsLocation, functionalDependencyLocation, columnIdTranslation);
			snow.setHasPredefinedRelations(functionalDependencyLocation!=null && extractPredefinedRelations);
		}
		
		System.out.println(String.format("Schema correspondences are a %s",schemaCorrespondences.getClass().getName()));

		snow.setPerformanceLog(performanceLog);
		snow.setRuntime(runtime.startMeasurement("SNoW"));
		Collection<Table> normalised = snow.run(schemaCorrespondences);

		// write class mapping
		Collection<Table> entityTables = Q.where(normalised, (t)->!t.getPath().contains("_rel_"));
		SnowOutput.writeClassMapping(new File(logDirectory, configuration + "_class_mapping.tsv"), taskName, entityTables, originalWebTablesCount);
		SnowStatisticsWriter.writeStatisticsForRelations(normalised, taskName, configuration, new File(logDirectory, configuration + "_relation_statistics.tsv"));

		SnowRuntime writeTime = runtime.startMeasurement("Write results");

		File outFile = new File(resultLocation);
		File fdOutFile = new File(resultLocation + "_fd_relations");
		outFile.mkdirs();
		fdOutFile.mkdirs();
		System.err.println(String.format("Writing Stitched Union Tables to %s", outFile.getAbsolutePath()));
		JsonTableWriter w = new JsonTableWriter();
		w.setWriteMapping(true);
		CSVTableWriter csvW = new CSVTableWriter();
		for(Table t : normalised) {
			t.addDataTypesToMapping();
			System.out.println(String.format("\twriting %s: {%s}", t.getPath(), StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
			w.write(t, new File(outFile, t.getPath()));
			csvW.write(t, new File(csvLocationFile, t.getPath()));

			// if we extracted FD relations, copy the entity tables to the fd-relations path
			if(snow.getExtractedRelationsForSchemaExtension().size()>0 && !t.getPath().contains("_rel_")) {
				w.write(t, new File(fdOutFile, t.getPath()));
			}
		}

		if(snow.getExtractedRelationsForSchemaExtension().size()>0) {
			System.err.println(String.format("Writing relations for selected FDs to %s", fdOutFile.getAbsolutePath()));
			for(Table t : snow.getExtractedRelationsForSchemaExtension()) {
				t.addDataTypesToMapping();
				w.write(t, new File(fdOutFile, t.getPath()));
				SnowStatisticsWriter.printStatisticsForFDRelation(t);
			}

			SnowStatisticsWriter.writeStatisticsForFDRelations(snow.getExtractedRelationsForSchemaExtension(), taskName, configuration, new File(logDirectory, "extracted_fd_relatins_statistics.tsv"));
		}

		writeTime.endMeasurement();
		runtime.endMeasurement();
		
		performanceLog.print();
		runtime.print();

		System.err.println("Done.");

		// if we're using PYRO for FD discovery the application does not terminate anymore ...
		System.exit(0);
	}


	private Processable<Correspondence<MatchableTableColumn, Matchable>> createCorrespondencesForGeneratedColumnsInReference(
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences, 
		Map<Integer, Map<Integer, TableColumn>> tableToColumnToDisambiguation,
		Map<Integer, Map<Integer, TableColumn>> tableToColumnToNumbering,
		WebTables web
	) {
		CreateCorrespondencesForGeneratedColumns corGenerator = new CreateCorrespondencesForGeneratedColumns();
		Processable<Correspondence<MatchableTableColumn, Matchable>> generatedCorrespondences = new ParallelProcessableCollection<>();
		Processable<Correspondence<MatchableTableColumn, Matchable>> disambiguationCorrespondences = corGenerator.run(schemaCorrespondences, web.getSchema(), tableToColumnToDisambiguation, web.getTables());
		Processable<Correspondence<MatchableTableColumn, Matchable>> numberingCorrespondences = corGenerator.run(schemaCorrespondences, web.getSchema(), tableToColumnToNumbering, web.getTables());
		
		generatedCorrespondences = disambiguationCorrespondences.append(numberingCorrespondences);
		return schemaCorrespondences.append(generatedCorrespondences).distinct();
	}

	private Processable<Correspondence<MatchableTableColumn, Matchable>> createCorrespondencesForGeneratedColumns(
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences, 
		Map<Integer, Map<Integer, TableColumn>> tableToColumnToDisambiguation,
		TableDisambiguationExtractor dis,
		Map<Integer, Map<Integer, TableColumn>> tableToColumnToNumbering,
		WebTables web
	) throws IOException {
		ColumnTranslator ct = new ColumnTranslator();
		ct.prepare(schemaCorrespondences);

		// go through all tables with disambiguation columns
		for(Integer tableWithDisambiguationId : new HashSet<>(tableToColumnToDisambiguation.keySet())) {
			Table tableWithDisambiguation = web.getTables().get(tableWithDisambiguationId);
			Map<Integer, TableColumn> columnToDisambiguation = tableToColumnToDisambiguation.get(tableWithDisambiguationId);
			// for every disambiguated column
			for(Integer disambiguationColumnIndex : new HashSet<>(columnToDisambiguation.keySet())) {
				TableColumn disambiguatedColumn = tableWithDisambiguation.getSchema().get(disambiguationColumnIndex);
				TableColumn disambiguationColumn = columnToDisambiguation.get(disambiguationColumnIndex);
				// get all columns that are mapped to the disambiguated column
				Collection<TableColumn> mappedColumns = ct.getAllMappedColumns(disambiguatedColumn, web.getTables());
				Collection<TableColumn> mappedToDisambiguation = ct.getAllMappedColumns(disambiguationColumn, web.getTables());

				// only create a new column if the existing disambiguation column is not mapped to any other column
				// or it is only mapped to disambiguations of the same attribute
				if(mappedToDisambiguation.size()==0 || Q.all(mappedToDisambiguation, (c)->disambiguationColumn.getHeader().equals(c.getHeader()))) {
					for(TableColumn mappedColumn : mappedColumns) {
						Table mappedTable = mappedColumn.getTable();
						if(
							// check if the mapped column also has a disambiguation
							!(
								tableToColumnToDisambiguation.containsKey(mappedTable.getTableId()) 
								&& 
								tableToColumnToDisambiguation.get(mappedTable.getTableId()).containsKey(mappedColumn.getColumnIndex())
							)
							&&
							// and that the other column is not a disambiguation column already!
							!ContextColumns.isDisambiguationColumn(mappedColumn.getHeader())
						) {
							// there is no disambiguation column
							TableColumn newDisambiguation = dis.createDisambiguationColumn(mappedColumn);
							mappedTable.insertColumn(newDisambiguation.getColumnIndex(), newDisambiguation);
							// add the new disambiguation column to the map
							Map<Integer, TableColumn> newColumnToDisambiguation = MapUtils.get(tableToColumnToDisambiguation, mappedTable.getTableId(), new HashMap<>());
							newColumnToDisambiguation.put(mappedColumn.getColumnIndex(), newDisambiguation);
							// also add the new column to the model (the matchable table columns)
							web.getSchema().add(new MatchableTableColumn(mappedTable.getTableId(), newDisambiguation));
						}
					}
				}
			}
		}
		// reload the records to add the new columns
		web.reloadRecords();
		
		CreateCorrespondencesForGeneratedColumns corGenerator = new CreateCorrespondencesForGeneratedColumns();
		Processable<Correspondence<MatchableTableColumn, Matchable>> generatedCorrespondences = new ParallelProcessableCollection<>();
		System.out.println(String.format("Created %d disambiguation columns", tableToColumnToDisambiguation.size()));

		Processable<Correspondence<MatchableTableColumn, Matchable>> disambiguationCorrespondences = corGenerator.run(schemaCorrespondences, web.getSchema(), tableToColumnToDisambiguation, web.getTables());

		System.out.println(String.format("Generated %d correspondences for created disambiguation columns.", disambiguationCorrespondences.size()));
		System.out.println(String.format("Created %d numbering columns", tableToColumnToNumbering.size()));
		Processable<Correspondence<MatchableTableColumn, Matchable>> numberingCorrespondences = corGenerator.run(schemaCorrespondences, web.getSchema(), tableToColumnToNumbering, web.getTables());
		System.out.println(String.format("Generated %d correspondences for created numbering columns.", numberingCorrespondences.size()));
		
		generatedCorrespondences = disambiguationCorrespondences.append(numberingCorrespondences);
		System.out.println(String.format("Generated %d correspondences for created context columns.", generatedCorrespondences.size()));
		CorrespondenceFormatter.printAttributeClusters(generatedCorrespondences);
		
		if(correspondencesLocation!=null && !matchTables) {
			// if correspondences were loaded from file, write the correspondences that were generated
			File f = new File(correspondencesLocation + "_generated_context.tsv");
			System.out.println(String.format("Writing generated correspondences for context columns to %s", f.getAbsolutePath()));
			corGenerator.writeN2NGoldstandard(generatedCorrespondences, f);
			Set<TableColumn> generatedColumns = new HashSet<>();
			for(Map<Integer, TableColumn> m : tableToColumnToDisambiguation.values()) {
				generatedColumns.addAll(m.values());
			}
			for(Map<Integer, TableColumn> m : tableToColumnToNumbering.values()) {
				generatedColumns.addAll(m.values());
			}
			if(entityStructureLocation!=null) {
				f = new File(entityStructureLocation + "_generated_context.tsv");
				System.out.println(String.format("Writing generated context columns to %s", f.getAbsolutePath()));
				corGenerator.writeEntityDefinition(generatedColumns, f);
			}
		} 

		System.out.println("Adding generated correspondences for context columns to table-to-table correspondences.");
		return schemaCorrespondences.append(generatedCorrespondences).distinct();
	}

	private Processable<Correspondence<MatchableTableColumn, Matchable>> addContextCorrespondences(
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
		Processable<Correspondence<MatchableTableColumn, Matchable>> contextCorrespondences,
		WebTables web
	) throws Exception {

		System.out.println("[addContextCorrespondences] indexing correspondence clusters");
		Set<Collection<MatchableTableColumn>> components = Correspondence.getConnectedComponents(schemaCorrespondences.get());
		Map<MatchableTableColumn, Collection<MatchableTableColumn>> colToComponent = new HashMap<>();
		for(Collection<MatchableTableColumn> comp : components) {
			for(MatchableTableColumn col : comp) {
				colToComponent.put(col,comp);
			}
		}

		System.out.println("[addContextCorrespondences] filtering correspondences");
		Processable<Correspondence<MatchableTableColumn, Matchable>> filteredContextCorrespondences = contextCorrespondences.map(
			(c)-> {
				// only add a context correspondence if the involved columns have no other correspondences
				// - or if they are always mapped ot the same column
				MatchableTableColumn mc = c.getFirstRecord();
				TableColumn col = web.getTables().get(mc.getTableId()).getSchema().get(mc.getColumnIndex());
				Collection<MatchableTableColumn> mapped = colToComponent.get(mc);
				boolean firstOk = true;
				if(mapped!=null) {
					Set<String> headers = new HashSet<>(Q.project(mapped, (m)->m.getHeader()));
					firstOk = mapped.size()==0 || Q.all(headers, (h)->col.getHeader().equals(h));
				}

				mc = c.getSecondRecord();
				TableColumn col2 = web.getTables().get(mc.getTableId()).getSchema().get(mc.getColumnIndex());
				mapped = colToComponent.get(mc);
				boolean secondOk = true;
				if(mapped!=null) {
					Set<String> headers = new HashSet<>(Q.project(mapped, (m)->m.getHeader()));
					secondOk = mapped.size()==0 || Q.all(headers, (h)->col2.getHeader().equals(h));
				}

				if(firstOk && secondOk) {
					return c;
				} else {
					return null;
				}
			});

		System.out.println(String.format("Adding %d filtered context correspondences", filteredContextCorrespondences.size()));

		return schemaCorrespondences.append(filteredContextCorrespondences).distinct();
	}

	private Processable<Correspondence<MatchableTableColumn, Matchable>> loadCorrespondences(
		String correspondencesLocation, 
		String contextCorrespondencesLocation, 
		DataSet<MatchableTableColumn,MatchableTableColumn> attributes, 
		boolean renameColumns, 
		Map<Integer, Table> tables,
		Map<String, String> columnIdTranslation
	) throws Exception {
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = null;

		System.err.println("Loading Union Table Correspondences");
		N2NGoldStandard corGs = new N2NGoldStandard();
		corGs.loadFromTSV(new File(correspondencesLocation));
		if(columnIdTranslation!=null) {
			corGs.translateIds(columnIdTranslation);
		}
		schemaCorrespondences = corGs.toCorrespondences(attributes);

		if(renameColumns) {
			// rename the columns according to the gs (for debugging)
			for(Set<String> cluster : corGs.getCorrespondenceClusters().keySet()) {
				String name = corGs.getCorrespondenceClusters().get(cluster);
				for(String id : cluster) {
					MatchableTableColumn mc = attributes.getRecord(id);
					if(mc!=null) {
						mc.setHeader(name);
						Table t = tables.get(mc.getTableId());
						if(t!=null) {
							TableColumn c = t.getSchema().get(mc.getColumnIndex());
							if(c!=null) {
								c.setSynonyms(Q.toList(c.getHeader()));
								c.setHeader(name);
							}
						}
					}
				}
			}
		}
		
		if(!noContextColumns) {
			boolean onlyStaticContextColumns = contextCorrespondencesLocation!=null;

			// if no context correspondences are provided, create them by linking all context columns (with the same header)

			// get table clusters
			Set<Collection<Integer>> clustering = Correspondence.getDataSourceClusters(schemaCorrespondences);
			
			// group by table cluster & context column header
			Map<Integer, Integer> tableToClusterId = new HashMap<>();
			for(Collection<Integer> cluster : clustering) {
				Integer clusterId = Q.firstOrDefault(cluster);
				
				for(Integer tableId : cluster) {
					tableToClusterId.put(tableId, clusterId);
				}
			}
			
			// add correspondences between context columns
			Processable<Group<String, MatchableTableColumn>> contextGroups = attributes.group(new RecordKeyValueMapper<String, MatchableTableColumn, MatchableTableColumn>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void mapRecordToKey(MatchableTableColumn record,
						DataIterator<Pair<String, MatchableTableColumn>> resultCollector) {
					if(
						onlyStaticContextColumns && ContextColumns.isStaticContextColumn(record.getHeader())
						|| !onlyStaticContextColumns && ContextColumns.isContextColumn(record)
					) {
						resultCollector.next(new Pair<String, MatchableTableColumn>(tableToClusterId.get(record.getTableId()) + "||" + record.getHeader(), record));
					}
				}
			});
			
			Processable<Correspondence<MatchableTableColumn, Matchable>> contextCorrespondences = contextGroups.map(new RecordMapper<Group<String,MatchableTableColumn>, Correspondence<MatchableTableColumn, Matchable>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void mapRecord(Group<String, MatchableTableColumn> record,
						DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) {
					
					for(MatchableTableColumn c1 : record.getRecords().get()) {
						for(MatchableTableColumn c2 : record.getRecords().get()) {
							if(c1!=c2) {
								Correspondence<MatchableTableColumn, Matchable> cor = new Correspondence<MatchableTableColumn, Matchable>(c1, c2, 1.0);
								resultCollector.next(cor);
							}
						}
					}
					
				}
			});

			Set<String> contextColumnsWithCors = new HashSet<String>(
				contextCorrespondences.<String>map((cor,col)->{col.next(cor.getFirstRecord().getHeader()); col.next(cor.getSecondRecord().getHeader());})
				.distinct()
				.get());
			System.out.println(String.format("Generated %d context correspondences for context columns: %s", contextCorrespondences.size(), StringUtils.join(contextColumnsWithCors, ",")));

			schemaCorrespondences = schemaCorrespondences.append(contextCorrespondences).distinct();
		}
		
		System.out.println(String.format("Loaded %d union table correspondences", schemaCorrespondences.size()));
		
		return schemaCorrespondences;
	}

	private Processable<Correspondence<MatchableTableColumn, Matchable>> loadContextCorrespondences(String contextCorrespondencesLocation, DataSet<MatchableTableColumn,MatchableTableColumn> attributes) throws Exception {
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = null;

		System.err.println("Loading Union Table Context Correspondences");
		N2NGoldStandard context = new N2NGoldStandard();
		context.loadFromTSV(new File(contextCorrespondencesLocation));
		schemaCorrespondences = context.toCorrespondences(attributes);

		System.out.println(String.format("Loaded %d union table context correspondences", schemaCorrespondences.size()));

		return schemaCorrespondences;
	}
		
	private void calculateFunctionalDependencies(WebTables web, File csvLocation) throws Exception {
		for(Table t : web.getTables().values()) {
			if(t.getSchema().getFunctionalDependencies().size()==0) {
				File csv = new File(csvLocation, t.getPath());
				CSVTableWriter w = new CSVTableWriter();
				csv = w.write(t, csv);
				System.out.println(String.format("Calculating functional dependencies for table %s", t.getPath()));
				Map<Set<TableColumn>, Set<TableColumn>> fds = FunctionalDependencyDiscovery.calculateFunctionalDependencies(t, csv);
				t.getSchema().setFunctionalDependencies(fds);
			}
		}
	}

	private Processable<Correspondence<MatchableTableColumn, Matchable>> runTableMatching(WebTables web, File logDirectory) throws Exception {
		TableToTableMatcher matcher = null;
		
    	switch(this.matcher) {
		case CandidateKey:
			calculateFunctionalDependencies(web, new File(csvLocation));
			matcher = new CandidateKeyBasedMatcher();
			break;
		case Label:
			matcher = new LabelBasedMatcher();
			break;
		case NonTrivialFull:
			calculateFunctionalDependencies(web, new File(csvLocation));
			matcher = new DeterminantBasedMatcher();
			break;
		case Trivial:
			matcher = new ImprovedValueBasedMatcher(0.2);
			break;
		case Entity:
			matcher = new EntityLabelBasedMatcher();
			break;
		default:
			break;
    		
    	}
		
		// Scalability: if we do not prevent correspondences between context columns in this step,
		// the correspondence graph can be huge, which means that the graph-based refinement cannot handle it in a reasonable time
		// (if we add context columns to the set of disjoint column headers by setting the second parameter to false, 
		// the pair-wise refinement will take care of such conflicts and reduce the size of the graph before the graph-based refinement)
    	DisjointHeaders dh = DisjointHeaders.fromTables(web.getTables().values(), false);
    	Map<String, Set<String>> disjointHeaders = dh.getAllDisjointHeaders();
    	
    	matcher.setWebTables(web);
    	matcher.setMatchingEngine(new MatchingEngine<>());
    	matcher.setDisjointHeaders(disjointHeaders);
		matcher.setVerbose(true);
		matcher.setLogDirectory(logDirectory);
		
    	matcher.initialise();
    	matcher.match();
    	
    	Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = matcher.getSchemaCorrespondences();

    	return schemaCorrespondences;
	}

	private Processable<Correspondence<MatchableTableColumn, Matchable>> filterContextCorrespondences(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		schemaCorrespondences = schemaCorrespondences.map((c)->
		{
			// filter out correspondences between two union context columns (they were created when creating the clustered union columns)
			String h1 = c.getFirstRecord().getHeader();
			String h2 = c.getSecondRecord().getHeader();
			if(!(
				(ContextColumns.isRenamedUnionContextColumn(h1) || ContextColumns.isDisambiguationColumn(h1) || ContextColumns.isNumberingColumn(h1))
				&&
				(ContextColumns.isRenamedUnionContextColumn(h2) || ContextColumns.isDisambiguationColumn(h2) || ContextColumns.isNumberingColumn(h2))
				&&
				h1.equals(h2)	// we only want to filter out correspondences created based on the column header, which is not correct for context columns
				)
			) {
				return c;
			} else {
				return null;
			}
		});

		

		return schemaCorrespondences;
	}
}
