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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

// import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.DeterminantSelector;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.TableReconstructor;
import de.uni_mannheim.informatik.dws.tnt.match.data.EntityTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableLodColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.StitchedModel;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableDataSetLoader;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.ApproximateFunctionalDependency;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.ApproximateFunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.ForeignKeyFDFilter;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyPropagation;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.StitchedFunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.CorrespondenceProjector;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.EntityTableDiscovery;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.EntityTableExtractor;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.ForeignKeyInduction;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers.EntityTableMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers.TableToTableRefinementMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.EntityTableDefinitionEvaluator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.ColumnTranslator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.BothKeysFullyMappedFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.BothTablesFullyMappedFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.MaxColumnsMappedFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.NonContextColumnMappedFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.OneKeyFullyMappedFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.OneTableFullyMappedFilter;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaNormalisation {

	public static enum MatcherType {
		Trivial,
		NonTrivialPartial,
		NonTrivialFull,
		CandidateKey,
		Label,
		Entity
	}
	
	public static enum NormalisationHeuristic {
		None,
		LargestDeterminant,
		LeftMostDeterminant,
		MostUniqueDeterminant,
		LargestColumnCluster,
		MostNonContextColumnsInDependant,
		DeterminantClusters,
		BCNF,
		_3NF,
		_2NF
	}
	
	public static enum StitchingMethod {
		None,
		All,
		OneTableFullyMapped,
		BothTablesFullyMapped,
		OneKeyFullyMapped,
		BothKeysFullyMapped,
		DeterminantClusters,
		NonContextColumnMapped,
		MaxColumnsMapped
	}
	

	private NormalisationHeuristic normalisationHeuristic;
	private StitchingMethod stitchingMethod;
	private double minDensity;
	private int minEntityTableSize = 0;
	private double minFDApproximationRate = 1.0;
	private String entityDefinitionLocation; // a folder that contains prototype tables for different types of entities
	private boolean binaryRelationsOnly = false;
	private boolean useEntityLabelDetection = false;
	private int minNumberOfInstanceCorrespondences = 0;
	private double minInstanceMatchRatio = 0.0;
	
	private boolean heuristicDependencyPropagation = true;
	private boolean assumeBinary = false;
	private boolean discoverFDs = false;
	private boolean skipT2T = false;

	/**
	 * @param minInstanceMatchRatio the minInstanceMatchRatio to set
	 */
	public void setMinInstanceMatchRatio(double minInstanceMatchRatio) {
		this.minInstanceMatchRatio = minInstanceMatchRatio;
	}

	/**
	 * @param heuristicDependencyPropagation the heuristicDependencyPropagation to set
	 */
	public void setHeuristicDependencyPropagation(boolean heuristicDependencyPropagation) {
		this.heuristicDependencyPropagation = heuristicDependencyPropagation;
	}

	/**
	 * @param assumeBinary the assumeBinary to set
	 */
	public void setAssumeBinary(boolean assumeBinary) {
		this.assumeBinary = assumeBinary;
	}

	/**
	 * @param discoverFDs the discoverFDs to set
	 */
	public void setDiscoverFDs(boolean discoverFDs) {
		this.discoverFDs = discoverFDs;
	}

	/**
	 * @param skipT2T the skipT2T to set
	 */
	public void setSkipT2T(boolean skipT2T) {
		this.skipT2T = skipT2T;
	}

	private StitchedModel model;
	Map<Integer, Table> universalSchema;	// universal schema for each entity type
	DataSet<MatchableTableColumn, MatchableTableColumn> universalSchemaAttributes;
	Processable<Correspondence<MatchableTableColumn, Matchable>> tableToUniversalSchemaCorrespondences;

	String taskName;
	File csvLocation; 
	File logDirectory;
	Map<String, Integer> originalWebTableColumnCount;	// by union column id
	Map<String, Integer> originalWebTableCount;
	Map<String, String> unionTableColumnHeaders;
	Map<String, Integer> unionColumnRowCount;
	
	private boolean hasPredefinedRelations = false;

	/**
	 * @param hasPredefinedRelations the hasPredefinedRelations to set
	 */
	public void setHasPredefinedRelations(boolean hasPredefinedRelations) {
		this.hasPredefinedRelations = hasPredefinedRelations;
	}
	
	private Map<TableColumn, Pair<Double, Double>> columnStats = new HashMap<>(); // column -> (density, FD approximation rate)

	private SnowRuntime runtime = null;
	/**
	 * @return the runtime
	 */
	public SnowRuntime getRuntime() {
		return runtime;
	}

	/**
	 * @param runtime the runtime to set
	 */
	public void setRuntime(SnowRuntime runtime) {
		this.runtime = runtime;
	}

	private SnowPerformance performanceLog;
	
	/**
	 * @return the performanceLog
	 */
	public SnowPerformance getPerformanceLog() {
		return performanceLog;
	}

	/**
	 * @param performanceLog the performanceLog to set
	 */
	public void setPerformanceLog(SnowPerformance performanceLog) {
		this.performanceLog = performanceLog;
	}
	
	private boolean logEntityDetection = false;
	public void setLogEntityDetection(boolean logEntityDetection) {
		this.logEntityDetection = logEntityDetection;
	}

	private boolean logFiltering = false;
	public void setLogFiltering(boolean logFiltering) {
		this.logFiltering = logFiltering;
	}
	
	private boolean logFDStitching = false;
	/**
	 * @param logFDStitching the logFDStitching to set
	 */
	public void setLogFDStitching(boolean logFDStitching) {
		this.logFDStitching = logFDStitching;
	}

	private Collection<Table> extractedRelationsForSchemaExtension = new LinkedList<>();

	/**
	 * @return the extractedRelationsForSchemaExtension
	 */
	public Collection<Table> getExtractedRelationsForSchemaExtension() {
		return extractedRelationsForSchemaExtension;
	}

	String configurationIdentifier;

	Processable<Correspondence<MatchableTableColumn, Matchable>> tableToTableReferenceCorrespondences;

	private boolean evaluateRefinementThresholds = false;
	/**
	 * @param evaluateRefinementThresholds the evaluateRefinementThresholds to set
	 */
	public void setEvaluateRefinementThresholds(boolean evaluateRefinementThresholds) {
		this.evaluateRefinementThresholds = evaluateRefinementThresholds;
	}

	double t2tRefinementThreshold = 0.0;
	public void setT2tRefinementThreshold(double t2tRefinementThreshold) {
		this.t2tRefinementThreshold = t2tRefinementThreshold;
	}

	boolean stopAfterMatching = false;
	/**
	 * @param stopAfterMatching the stopAfterMatching to set
	 */
	public void setStopAfterMatching(boolean stopAfterMatching) {
		this.stopAfterMatching = stopAfterMatching;
	}

	boolean doNotDeduplicateInstanceCandidates = false;
	/**
	 * @param doNotDeduplicateInstanceCandidates the doNotDeduplicateInstanceCandidates to set
	 */
	public void setDoNotDeduplicateInstanceCandidates(boolean doNotDeduplicateInstanceCandidates) {
		this.doNotDeduplicateInstanceCandidates = doNotDeduplicateInstanceCandidates;
	}

	boolean skipTableToTableMatching = false;
	/**
	 * @param skipTableToTableMatching the skipTableToTableMatching to set
	 */
	public void setSkipTableToTableMatching(boolean skipTableToTableMatching) {
		this.skipTableToTableMatching = skipTableToTableMatching;
	}

	boolean evaluateNormalisation = false;
	public void setEvaluateNormalisation(boolean evaluateNormalisation) {
		this.evaluateNormalisation = evaluateNormalisation;
	}

	public SchemaNormalisation(
			WebTables web, 
			String taskName, File 
			csvLocation, 
			File logDirectory,
			NormalisationHeuristic normalisationHeuristic,
			StitchingMethod stitchingMethod,
			String entityDefinitionLocation,
			int minEntities,
			double minDensity,
			boolean binaryRelationsOnly,
			double minFDApproximationRate,
			Map<String, Integer> originalWebTableColumnCount,
			Map<String, Integer> originalWebTableCount,
			boolean useEntityLabelDetection,
			int minNumberOfInstanceCorrespondences,
			String configurationIdentifier,
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToTableReferenceCorrespondences) {
		model = new StitchedModel();
		model.setRecords(web.getRecords());
		model.setAttributes(web.getSchema());
		model.setCandidateKeys(web.getCandidateKeys());
		model.setTables(web.getTables());
		this.taskName = taskName;
		this.csvLocation = csvLocation;
		this.logDirectory = logDirectory;
		this.normalisationHeuristic = normalisationHeuristic;
		this.stitchingMethod = stitchingMethod;
		this.entityDefinitionLocation = entityDefinitionLocation;
		this.minEntityTableSize = minEntities;
		this.minDensity = minDensity;
		this.binaryRelationsOnly = binaryRelationsOnly;
		this.minFDApproximationRate = minFDApproximationRate;
		this.originalWebTableColumnCount = originalWebTableColumnCount;
		this.originalWebTableCount = originalWebTableCount;
		this.useEntityLabelDetection = useEntityLabelDetection;
		this.minNumberOfInstanceCorrespondences = minNumberOfInstanceCorrespondences;
		this.configurationIdentifier = configurationIdentifier;
		this.tableToTableReferenceCorrespondences = tableToTableReferenceCorrespondences;
		
		unionTableColumnHeaders = Pair.toMap(Q.project(model.getAttributes().get(), (c)->new Pair<>(c.getIdentifier(),c.getHeader())));
		unionColumnRowCount = Pair.toMap(Q.project(model.getAttributes().get(), (c)->new Pair<>(c.getIdentifier(),web.getTables().get(c.getTableId()).getRows().size())));
	}
	
	private void printStep(String message) {
		System.out.println("*********************************************************************");
		System.out.println("*********************************************************************");
		System.out.println(String.format("***************** %s",message));
		System.out.println("*********************************************************************");
		System.out.println("*********************************************************************");
	}

	public Collection<Table> run(
			Processable<Correspondence<MatchableTableColumn, Matchable>> initialSchemaCorrespondences
			) throws Exception {
	
		if(runtime == null) {
			runtime = new SnowRuntime("SNoW");
			runtime.startMeasurement();
		}
		if(performanceLog==null) {
			performanceLog = new SnowPerformance();
		}

		model.setSchemaCorrespondences(initialSchemaCorrespondences);
		printSchemaClusters();
		
		Collection<EntityTable> referenceEntityTables = model.getEntityGroups();

		/*************************************************************************************************************
		 *******************************  Match to KB 
		 ************************************************************************************************************/
		// (optional step) match tables to entity definitions
		if(entityDefinitionLocation!=null) {
			
			SnowRuntime r = runtime.startMeasurement("Match to KB");
			printStep("Matching tables to entity defintions");
			matchToEntityDefinition(model.getSchemaCorrespondences(), referenceEntityTables);
			r.endMeasurement();
			
			
		}
		if(useEntityLabelDetection) {
			// create entity groups using the entity labels of the union tables
			createEntityGroupsFromEntityLabelColumns();
		}
		
		/*************************************************************************************************************
		 *******************************  Extract entities and create FKs
		 ************************************************************************************************************/
		ForeignKeyInduction fki = null;
		Map<Table, EntityTable> entityTables = null;
		if(model.getEntityGroups()!=null) {
			SnowRuntime r = runtime.startMeasurement("Extract entities & create FKs");

			// remove correspondences between different entity types
			model.filterSchemaCorrespondencesByEntityGroups();
			printSchemaClusters();
			System.out.println(String.format("%d attributes / %d records in total before entity extraction", model.getAttributes().size(), model.getRecords().size()));
			
			// extract entities into entity tables
			printStep("Extracting entity tables");
			EntityTableExtractor entityExtractor = new EntityTableExtractor();
			entityTables = entityExtractor.extractEntityTables(taskName, model);
			
			// filter out small entity tables
			entityExtractor.filterEntityTables(entityTables, minEntityTableSize);
			
			// replace entity columns with foreign keys
			printStep("Creating foreign keys");
			fki = new ForeignKeyInduction(model.getSchemaCorrespondences(), entityTables, model.getTables(), model.getAttributes(), csvLocation, logDirectory);
			fki.replaceEntityColumnsWithForeignKey();
			
			// print entity tables
			for(EntityTable et : model.getEntityGroups()) {
				System.out.println(String.format("Entity Table '%s'", et.getEntityName()));
				for(String att : et.getAttributes().keySet()) {
					System.out.println(String.format("\t%s: %s",
						att,
						StringUtils.join(et.getAttributeProvenance().get(att), ",")
					));
				}
			}
			
			// if the entity structure was predefined, we can use it to evaluate the matching results
			// important: evaluate after creating the FKs s.t. the Entity Context columns have been generated!
			if(entityDefinitionLocation!=null && referenceEntityTables!=null) {
				printStep("Evaluating Schema Mapping");
				EntityTableDefinitionEvaluator eval = new EntityTableDefinitionEvaluator();
				eval.evaluateEntityTableDefinition(model.getEntityGroups(), referenceEntityTables, logDirectory, taskName, performanceLog, originalWebTableCount);
			}

			// update tables, datasets & correspondences
			model.setTables(Q.map(fki.getTablesWithFK(), (t)->t.getTableId()));
			model.setRecords(fki.getRecordsWithFK());
			model.setAttributes(fki.getAttributes());
			model.setSchemaCorrespondences(fki.getSchemaCorrespondences());			
			model.loadCandidateKeys();
			System.out.println(String.format("%d attributes / %d records in total after entity extraction", model.getAttributes().size(), model.getRecords().size()));
			r.endMeasurement();

			// Table-to-Table Matching Refinement (not used)
			// - run value-based matching with FK & data type as token prefix = duplicate-based matching based on FKs
			// - constraint: no correspondences between different attributes in the discovered entity table = we only remove incorrect correspondences
			// --- implementation: join existing correspondences with created ones, only keep if in join result (INNER JOIN)
			if(evaluateRefinementThresholds) {
				evaluateTableToTableCorrespondenceRefinementThresholds(model, referenceEntityTables);
			}
			if(t2tRefinementThreshold>0.0) {
				runTableToTableCorrespondenceRefinement(model, referenceEntityTables, t2tRefinementThreshold);
			}
		}
		EntityTableDefinitionEvaluator.logEntityDefinitions(model.getEntityGroups(), taskName, logDirectory);
		printSchemaClusters();
		
		if(stopAfterMatching) {
			return Q.union(entityTables.keySet(), model.getTables().values());
		}

		/*************************************************************************************************************
		 *******************************  Correspondence-based Stitching: Create Stitched Union Tables
		 ************************************************************************************************************/
		// (optional step) stitch tables based on available correspondences to merge identical schemas with alternative headers
		printStep("Initial stitching");
		Collection<Table> reconstructed = stitchTables(model.getTables(), false, stitchingMethod, true, false, false, false, false);
		printSchemaClusters();
		// important: after stitching, we must re-calculate the functional dependencies (the stitched dependencies may be invalid)
		if(!hasPredefinedRelations) {
			if(!assumeBinary && !discoverFDs) {
				SnowRuntime r = runtime.startMeasurement("Discover functional dependencies");
				updateDependencies(reconstructed, false);
				r.endMeasurement();
			} else {
				setBinaryFunctionalDependencies(reconstructed);
			}
		}

		Collection<Table> normalised = null;
		if(reconstructed.size()>0) {
			

			boolean useNFDs = false;

			/*************************************************************************************************************
			 *******************************  Stitch into universal relation
			************************************************************************************************************/
			printStep("Creating universal schema");
			model.setTables(Q.map(reconstructed, (t)->t.getTableId()));
			model.printTablesAndFDs();

			// remove correspondences if table-to-table matching is deactivated
			if(skipTableToTableMatching) {
				// remove all correspondences
				model.setSchemaCorrespondences(model.getSchemaCorrespondences().where((c)->false));

				// Alternative:
				// keep correspondences among FK columns so we can still create the universal schema
				// Problematic, because this results in a large number of FDs which makes key detection for the universal schema extremely slow
				// model.setSchemaCorrespondences(model.getSchemaCorrespondences().where((c)->"FK".equals(c.getFirstRecord().getHeader())));
			}

			// create the universal schema
			StitchedModel universal = model.createUniversalSchema(true);
			// universal.printTablesAndFDs();

			for(Table t : universal.getTables().values()) {
				System.out.println(String.format("Universal relation #%d {%s}",
					t.getTableId(),
					StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
				));
				// printFunctionalDependencies(t);	
			}

		/*************************************************************************************************************
		 *******************************  Stitch functional dependencies
		 ************************************************************************************************************/
			if(hasPredefinedRelations) {
				// set the FDs based on the entity definitions
				for(Table t : universal.getTables().values()) {
					t.getSchema().getFunctionalDependencies().clear();
				}
				EntityTableDefinitionEvaluator.setPredefinedFunctionalDependencies(universal, entityTables, fki.getEntityIdToEntityTableMap(), model);

				// extract FDs
				EntityTableDefinitionEvaluator eval = new EntityTableDefinitionEvaluator();
				Map<EntityTable, Collection<Table>> relationsForFDs = eval.extractRelationsForPredefinedFDs(universal, entityTables, fki.getEntityIdToEntityTableMap(), model, taskName, originalWebTableCount);
				for(Collection<Table> relations : relationsForFDs.values()) {
					extractedRelationsForSchemaExtension.addAll(relations);
				}
				
			} else {
				printStep("Stitching Functional Dependencies");
				// clear the FDs (but only if tables were actually stitched)
				for(Table t : universal.getTables().values()) {
					if(universal.getStitchedProvenance().get(t).size()>1) {
						t.getSchema().getFunctionalDependencies().clear();
					}
				}

				if(!discoverFDs) {
					SnowRuntime r = runtime.startMeasurement("Stitch FDs");
					universal.stitchDependencies(model.getTables().values(), logFDStitching);
					universal.printTablesAndFDs();
					r.endMeasurement();

					if(heuristicDependencyPropagation) { // not used
						r = runtime.startMeasurement("Propagate FDs");
						FunctionalDependencyPropagation propagation = new FunctionalDependencyPropagation();
						propagation.propagateDependencies(universal, logFDStitching);
						r.endMeasurement();
					}
				} else {
					updateDependencies(model.getTables().values(), false);
				}
				
				for(Table t : universal.getTables().values()) {
					t.getSchema().setCandidateKeys(StitchedFunctionalDependencyUtils.listCandidateKeysBasedOnMinimalFDs(t.getSchema().getFunctionalDependencies(), new HashSet<>(t.getColumns()), universal, false));
				}
				universal.printTablesAndFDs();

				// Remove all FDs which do not contain the FK in the determinant
				printStep("Filtering functional dependencies by FK");
				ForeignKeyFDFilter filter = new ForeignKeyFDFilter();
				filter.filter(universal);
				for(Table t : universal.getTables().values()) {
					t.getSchema().setCandidateKeys(StitchedFunctionalDependencyUtils.listCandidateKeysBasedOnMinimalFDs(t.getSchema().getFunctionalDependencies(), new HashSet<>(t.getColumns()), universal, false));
				}
				universal.printTablesAndFDs();

				if(referenceEntityTables!=null) {
					// evaluate FDs
					EntityTableDefinitionEvaluator eval = new EntityTableDefinitionEvaluator();
					Map<EntityTable, Collection<Table>> relationsForFDs = eval.evaluateFunctionalDependencies(universal, referenceEntityTables, fki.getEntityIdToEntityTableMap(), taskName, logDirectory, heuristicDependencyPropagation, false, assumeBinary, discoverFDs, performanceLog, originalWebTableCount, configurationIdentifier, "");
					for(Collection<Table> relations : relationsForFDs.values()) {
						extractedRelationsForSchemaExtension.addAll(relations);
					}
				}
			}

		/*************************************************************************************************************
		 *******************************  Create normalised relations
		 ************************************************************************************************************/
			printStep("Synthesizing relational schema");
			SnowRuntime normaliseTime = runtime.startMeasurement("Normalise");
			StitchedModel normalisedUniversal = null;
			normalisedUniversal = universal.normalise(NormalisationHeuristic.BCNF, true, false, true, useNFDs);
			normalisedUniversal.printTablesAndFDs();
			normalised = normalisedUniversal.getTables().values();
			normaliseTime.endMeasurement();

			for(Table t : normalised) {
				for(TableColumn placeholder : Q.where(t.getColumns(), (c)->c.getHeader().startsWith("placeholder_"))) {
					System.out.println(String.format("Removing placeholder '%s'", placeholder.getHeader()));
					t.getSchema().removeColumn(placeholder);
				}
			}

			printStep("Filtering relational schema");
			SnowRuntime filterTime = runtime.startMeasurement("Filter relational schema");
			normalised = filterDecompositions(normalised, universal, normalisedUniversal);
			filterTime.endMeasurement();

			// this step can remove most or even all rows, if the relation has a large key and only sparse attributes are non-key
			// printStep("Remove empty rows");
			// SnowRuntime removeRowsTime = runtime.startMeasurement("Remove empty rows");
			// removeEmptyRows(normalised);
			// removeRowsTime.endMeasurement();

			if(evaluateNormalisation) {
				if(referenceEntityTables!=null) {
					// evaluate FDs
					extractedRelationsForSchemaExtension.clear();
					EntityTableDefinitionEvaluator eval = new EntityTableDefinitionEvaluator();
					Map<EntityTable, Collection<Table>> relationsForFDs = eval.evaluateFunctionalDependencies(normalisedUniversal, referenceEntityTables, fki.getEntityIdToEntityTableMap(), taskName, logDirectory, heuristicDependencyPropagation, false, assumeBinary, discoverFDs, performanceLog, originalWebTableCount, configurationIdentifier, "");
					for(Collection<Table> relations : relationsForFDs.values()) {
						extractedRelationsForSchemaExtension.addAll(relations);
					}
				}
			}
		}
		
		if(fki!=null) {
			// if foreign keys have been created, filter the relation tables by density
			printStep("Filtering normalised tables");
			SnowRuntime r = runtime.startMeasurement("Filter normalise tables");
			normalised = filterNormalisedTables(normalised, fki.getEntityIdToEntityTableMap());
			r.endMeasurement();
		}

		if(entityTables!=null) {
			
			System.out.println("Extracted entity tables");
			for(Table t : entityTables.keySet()) {
				System.out.println(String.format("\t#%d %s {%s} / %d rows", 
						t.getTableId(),
						t.getPath(),
						StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
						t.getRows().size()
						));
			}
			
			System.out.println("Extracted relation tables");
			for(Table t : normalised) {
				System.out.println(String.format("\t#%d %s {%s} / %d rows", 
						t.getTableId(),
						t.getPath(),
						StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
						t.getRows().size()
						));
			}
			
			SnowOutput.writeColumnClassificationResults(entityTables.keySet(), normalised, binaryRelationsOnly, logDirectory, originalWebTableColumnCount, unionTableColumnHeaders, unionColumnRowCount);
			SnowOutput.writeRelationExtractionResults(entityTables.keySet(), normalised, binaryRelationsOnly, taskName, logDirectory, columnStats);
			
			normalised = Q.union(normalised, entityTables.keySet());
		}
		
		runtime.endMeasurement();

		return normalised;
	}
	
	Function<Boolean, Correspondence<MatchableTableColumn, Matchable>> skipRefinementCondition = (c) -> 
		//both columns are context columns
		ContextColumns.isContextColumn(c.getFirstRecord()) && ContextColumns.isContextColumn(c.getSecondRecord())
		||
		// both columns have the same header
		c.getFirstRecord().getHeader().equals(c.getSecondRecord().getHeader()) && !"null".equals(c.getFirstRecord().getHeader())
		||
		// columns are not of type numeric
		c.getFirstRecord().getType()!=DataType.numeric
		;

	protected void runTableToTableCorrespondenceRefinement(StitchedModel model, Collection<EntityTable> referenceEntityTables, double threshold) throws IOException {
		TableToTableRefinementMatcher tableToTableRefinement = new TableToTableRefinementMatcher(threshold);

		// split the correspondences: we do not refine correspondences between context attributes
		Processable<Correspondence<MatchableTableColumn, Matchable>> correspondencesToCheck = model.getSchemaCorrespondences().map((c)->{
			if(!skipRefinementCondition.execute(c)) {
				return c;
			} else {
				return null;
			}
		});
		Processable<Correspondence<MatchableTableColumn, Matchable>> skippedCorrespondences = model.getSchemaCorrespondences().map((c)->{
			if(skipRefinementCondition.execute(c)) {
				return c;
			} else {
				return null;
			}
		});
		Processable<Correspondence<MatchableTableColumn, Matchable>> filteredCorrespondences = tableToTableRefinement.run(correspondencesToCheck, model.getRecords());
		filteredCorrespondences = filteredCorrespondences.append(skippedCorrespondences);
		System.out.println(String.format("[TableToTableRefinement] %d/%d table-to-table correspondences after FK-based refinement", filteredCorrespondences.size(), model.getSchemaCorrespondences().size()));
		model.setSchemaCorrespondences(filteredCorrespondences);

		if(tableToTableReferenceCorrespondences!=null && referenceEntityTables!=null) {
			// re-evaluate the table-to-table matching
			SnowTableToTableEvaluator t2tEval = new SnowTableToTableEvaluator();
			t2tEval.evaluateProjectedTableToTableMatching(model.getSchemaCorrespondences(), tableToTableReferenceCorrespondences, originalWebTableCount, logDirectory, taskName, performanceLog, "refined", model.getTables(), model.getEntityGroups(), referenceEntityTables, false);
		}
	}

	protected void evaluateTableToTableCorrespondenceRefinementThresholds(StitchedModel model, Collection<EntityTable> referenceEntityTables) throws IOException {
		for(int i = 0; i<10; i++) {
			double threshold = i / 10.0;
			TableToTableRefinementMatcher tableToTableRefinement = new TableToTableRefinementMatcher(threshold);
			// split the correspondences: we do not refine correspondences between context attributes
			Processable<Correspondence<MatchableTableColumn, Matchable>> correspondencesToCheck = model.getSchemaCorrespondences().map((c)->{
				if(!skipRefinementCondition.execute(c)) {
					return c;
				} else {
					return null;
				}
			});
			Processable<Correspondence<MatchableTableColumn, Matchable>> skippedCorrespondences = model.getSchemaCorrespondences().map((c)->{
				if(skipRefinementCondition.execute(c)) {
					return c;
				} else {
					return null;
				}
			});
			Processable<Correspondence<MatchableTableColumn, Matchable>> filteredCorrespondences = tableToTableRefinement.run(correspondencesToCheck, model.getRecords());
			filteredCorrespondences = filteredCorrespondences.append(skippedCorrespondences);
			System.out.println(String.format("[TableToTableRefinement] %d/%d table-to-table correspondences after FK-based refinement", filteredCorrespondences.size(), model.getSchemaCorrespondences().size()));

			if(tableToTableReferenceCorrespondences!=null && referenceEntityTables!=null) {
				//re-evaluate the table-to-table matching
				SnowTableToTableEvaluator t2tEval = new SnowTableToTableEvaluator();
				t2tEval.evaluateProjectedTableToTableMatching(filteredCorrespondences, tableToTableReferenceCorrespondences, originalWebTableCount, logDirectory, taskName, performanceLog, String.format("refined %.1f", threshold), model.getTables(), model.getEntityGroups(), referenceEntityTables, true);
			}
		}
	}

	protected void printSchemaClusters() {
		Set<Collection<MatchableTableColumn>> components = Correspondence.getConnectedComponents(model.getSchemaCorrespondences().get());
		
		System.out.println(String.format("+++ %d schema clusters", components.size()));
		for(Collection<MatchableTableColumn> comp : components) {
			Collection<Integer> allTableIds = Q.project(comp, (c)->c.getTableId());
			Set<Integer> uniqueTableIds = new HashSet<>(allTableIds);
			System.out.println(String.format("\t%s", StringUtils.join(Q.project(comp, (c)->c.toString()), ",")));
			if(allTableIds.size()>uniqueTableIds.size()) {
				Collection<Integer> duplicates = new LinkedList<>(allTableIds);
				for(Integer id : uniqueTableIds) {
					duplicates.remove(id);
				}
				System.out.println(String.format("\tInconsistent attribute cluster! Duplicate tables: %s", StringUtils.join(duplicates, ",")));
			}
		}
	}
	
	protected void updateDependencies(Collection<Table> tables, boolean verbose) {
		try {
			FunctionalDependencyUtils.calculateApproximateFunctionalDependencies(tables, csvLocation, 1.0 - minFDApproximationRate);
			
			for(Table t : tables) {
			// 	System.out.println(String.format("#%d %s: {%s}", t.getTableId(), t.getPath(), StringUtils.join(Q.project(t.getColumns(),(c)->c.getHeader()), ",")));
				t.getSchema().setCandidateKeys(FunctionalDependencyUtils.listCandidateKeys(t));
			// 	printFunctionalDependencies(t);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	private void setBinaryFunctionalDependencies(Collection<Table> tables) {
		for(Table t : tables) {
			TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
			if(fk!=null) {
				Set<TableColumn> determinant = Q.toSet(fk);
				Set<TableColumn> dependant = new HashSet<>(Q.without(t.getColumns(), determinant));
				t.getSchema().getFunctionalDependencies().clear();
				t.getSchema().getFunctionalDependencies().put(determinant, dependant);
			}
		}
	}
	
	public void loadEntityGroups(String entityStructureLocation, String candidateKeysLocation, String relationsLocation, String stitchedRelationsLocation, String functionalDependencyLocation, Map<String, String> columnIdTranslation) throws IOException {
		File entityFile = entityStructureLocation==null ? null : new File(entityStructureLocation);
		File candidateKeyFile = candidateKeysLocation==null ? null : new File(candidateKeysLocation);
		File relationFile = relationsLocation==null ? null : new File(relationsLocation);
		File stitchedRelationFile = stitchedRelationsLocation==null ? null : new File(stitchedRelationsLocation);
		File functionalDependencyFile = functionalDependencyLocation==null ? null : new File(functionalDependencyLocation);
		Collection<EntityTable> entityGroups = EntityTable.loadFromDefinition(model.getAttributes(), entityFile, candidateKeyFile, relationFile, stitchedRelationFile, functionalDependencyFile, false, columnIdTranslation);
		model.setEntityGroups(entityGroups);
	}
	
	private void matchToEntityDefinition(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences, Collection<EntityTable> predefined) throws IOException {
		EntityTableDiscovery entityDiscovery = new EntityTableDiscovery();

		EntityTableMatcher entityMatcher = new EntityTableMatcher(minNumberOfInstanceCorrespondences);
		entityMatcher.setMinMatchRatio(minInstanceMatchRatio);
		entityMatcher.setTaskName(taskName);
		entityMatcher.setLogCorrespondences(logEntityDetection);
		entityMatcher.setLogDirectory(logDirectory, taskName);
		entityMatcher.loadEntityDefinitions(new File(entityDefinitionLocation), false);
		entityMatcher.setReferenceEntityTables(predefined);
		entityMatcher.setDoNotDeduplicateInstanceCandidates(doNotDeduplicateInstanceCandidates);
		entityMatcher.setConfiguration(configurationIdentifier);
		entityMatcher.setUseGraphBasedRefinement(!skipT2T); // graph-based refinement is based on table-to-table correspondences, skip it if no such correspondences were created
		
		// the call to detect finds entity columns w.r.t. to the reference KB
		Processable<Correspondence<MatchableTableColumn, Matchable>> refinedCorrespondences = entityMatcher.detectEntityColumns(
			model.getRecords(), 
			model.getAttributes(), 
			model.getCandidateKeys(), 
			schemaCorrespondences, 
			model.getTables(), 
			DisjointHeaders.fromTables(model.getTables().values()));
		
		Collection<EntityTable> entityGroups = entityDiscovery.createEntityColumnsFromCorrespondences(
			refinedCorrespondences, 
			entityMatcher.getKnowledgeBase().getClassIndices(), 
			model.getAttributes(),
			entityMatcher.getKnowledgeBase().getCandidateKeyDefinitions());
		model.setEntityGroups(entityGroups);

		// filter out all correspondences to the KB and replace the table-to-table correspondences (schemaCorrespondences)
		// - we only need clusters of web table columns, and don't merge the KB data into our extracted relations
		schemaCorrespondences = refinedCorrespondences.where((c)->!(c.getSecondRecord() instanceof MatchableLodColumn));
		
		model.setSchemaCorrespondences(schemaCorrespondences);
		System.out.println(String.format("Entity detection resulted in %d correspondences", schemaCorrespondences.size()));
	}
	
	private void createEntityGroupsFromEntityLabelColumns() {
		Collection<EntityTable> entityGroups = new LinkedList<>();
		
		for(Table t : model.getTables().values()) {
			TableColumn entityLabelColumn = t.getSubjectColumn();
			
			if(entityLabelColumn!=null) {
				EntityTable ent = new EntityTable();
				ent.setEntityName(t.getPath());
				ent.setCanddiateKeys(Q.toList(Q.toList(Q.toList(model.getAttributes().getRecord(entityLabelColumn.getIdentifier())))));
				Map<String, Collection<MatchableTableColumn>> att = new HashMap<>();
				att.put(entityLabelColumn.getHeader(), Q.toList(model.getAttributes().getRecord(entityLabelColumn.getIdentifier())));
				ent.setAttributes(att);
				entityGroups.add(ent);
			}
		}

		model.setEntityGroups(entityGroups);
	}
	
	private Collection<Table> stitchTables(
		Map<Integer, Table> tablesToStitch, 
		boolean log, 
		StitchingMethod stitchingMethod, 
		boolean keepUnchangedTables, 
		boolean overlappingAttributesOnly, 
		boolean clusterByAttributes, 
		boolean mergeToSet,
		boolean propagateDependencies) {
		int nextTableId = Q.<Integer>max(Q.project(tablesToStitch.values(), (t)->t.getTableId()))+1;
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> filteredCorrespondences = model.getSchemaCorrespondences();

		// filter correspondences to control which tables are stitched
		switch (stitchingMethod) {
		case OneTableFullyMapped:
			OneTableFullyMappedFilter filter = new OneTableFullyMappedFilter();
			// filter.setLog(log);
			filter.setLogDirectory(logDirectory);
			System.out.println(String.format("[OneTableFullyMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = filter.run(filteredCorrespondences, tablesToStitch.values());	
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			break;
		case BothTablesFullyMapped:
			BothTablesFullyMappedFilter filterBoth = new BothTablesFullyMappedFilter();
			filterBoth.setEnforceDistinctProvenance(false);
			// filterBoth.setLog(log);
			filterBoth.setLogDirectory(logDirectory);
			System.out.println(String.format("[BothTablesFullyMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = filterBoth.run(filteredCorrespondences, tablesToStitch.values());	
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			break;
		case OneKeyFullyMapped:
			OneKeyFullyMappedFilter filterOneKey = new OneKeyFullyMappedFilter();
			filterOneKey.setEnforceDistinctProvenance(false);
			filterOneKey.setPropagateKeys(true);
			// filterOneKey.setPropagateDependencies(propagateDependencies);
			filterOneKey.setLogDirectory(logDirectory);
			System.out.println(String.format("[OneKeyFullyMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = filterOneKey.run(filteredCorrespondences, model.getAttributes(), tablesToStitch.values());	
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			// candidateKeys = filterOneKey.getClusteredCandidateKeys();
			model.setCandidateKeys(filterOneKey.getClusteredCandidateKeys());
			System.out.println(String.format("Created %d clustered candidate keys", model.getCandidateKeys().size()));
			for(MatchableTableDeterminant key : model.getCandidateKeys().get()) {
				System.out.println(String.format("[OneKeyFyllyMapped] clustered candidate key: #%d: {%s}",
					key.getTableId(),
					MatchableTableColumn.formatCollection(key.getColumns())
				));
			}
			break;
		case MaxColumnsMapped:
			MaxColumnsMappedFilter maxColumnsMapped = new MaxColumnsMappedFilter();
			maxColumnsMapped.setEnforceDistinctProvenance(false);
			maxColumnsMapped.setLogDirectory(logDirectory);
			System.out.println(String.format("[MaxColumnsMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = maxColumnsMapped.run(filteredCorrespondences, tablesToStitch.values());	
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			break;
		case NonContextColumnMapped:
			NonContextColumnMappedFilter filterNonContextMapped = new NonContextColumnMappedFilter();
			filterNonContextMapped.setEnforceDistinctProvenance(false);
			filterNonContextMapped.setLogDirectory(logDirectory);
			System.out.println(String.format("[NonContextColumnMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = filterNonContextMapped.run(filteredCorrespondences, tablesToStitch.values());	
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			break;
		case BothKeysFullyMapped:
			// cluster all matching candidate keys
			// then stitch all tables which have candidate keys in the same clusters
			BothKeysFullyMappedFilter filterKeys = new BothKeysFullyMappedFilter();
			// filterKeys.setEnforceDistinctProvenance(false);
			filterKeys.setEnforceDistinctProvenance(true);
			System.out.println(String.format("[BothKeysFullyMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = filterKeys.run(filteredCorrespondences, model.getAttributes(), tablesToStitch.values());	
			// candidateKeys = filterKeys.getClusteredCandidateKeys();
			model.setCandidateKeys(filterKeys.getClusteredCandidateKeys());
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			System.out.println(String.format("Created %d clustered candidate keys", model.getCandidateKeys().size()));
			break;
		case DeterminantClusters:
			DeterminantSelector ds = new DeterminantSelector(true, logDirectory);
			System.out.println(String.format("[DeterminantClusters] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = ds.selectDeterminant(model.getRecords(), model.getAttributes(), model.getCandidateKeys(), model.getSchemaCorrespondences(), tablesToStitch);
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			System.out.println(String.format("Created %d propagated candidate keys", model.getCandidateKeys().size()));
			break;
		default:
			break;
		}
		
		if(stitchingMethod==StitchingMethod.None) {
			return tablesToStitch.values();
		} else {
			if(log) {
				for(Correspondence<MatchableTableColumn, Matchable> cor : filteredCorrespondences.get()) {
					System.out.println(String.format("\t{%s}->{%s}", cor.getFirstRecord(), cor.getSecondRecord()));
				}
			}

			TableReconstructor tr = new TableReconstructor(tablesToStitch);
			tr.setKeepUnchangedTables(keepUnchangedTables);
			tr.setOverlappingAttributesOnly(overlappingAttributesOnly);
			tr.setClusterByOverlappingAttributes(clusterByAttributes);
			tr.setMergeToSet(mergeToSet);
			tr.setCreateNonOverlappingClusters(true);
			tr.setVerifyDependencies(propagateDependencies);	
//			tr.setLog(log);
//			tr.setLogDirectory(logDirectory);
			WebTableDataSetLoader loader = new WebTableDataSetLoader();
			// load all FDs with their closure, so we don't loose any dependency information if certain attributes are not projected into the reconstructed table
			// DataSet<MatchableTableDeterminant, MatchableTableColumn> functionalDependencies = loader.loadFunctionalClosureDataSet(tablesToStitch.values(), records);
			DataSet<MatchableTableDeterminant, MatchableTableColumn> functionalDependencies = loader.loadFunctionalDependencyDataSet(tablesToStitch.values(), model.getRecords());
			Collection<Table> result = tr.reconstruct(nextTableId, model.getRecords(), model.getAttributes(), model.getCandidateKeys(), functionalDependencies, filteredCorrespondences);
			
			if(result.size()>0) {
				// update schema correspondences, record, and attribute datasets
				// important: update the original schema correspondences, not the filtered ones!
				// the filtered correspondences were used for stitching, so they only connect one column to itself after stitching
				System.out.println(String.format("[stitchTables] Projecting %d schema correspondences", model.getSchemaCorrespondences().size()));
				
				model.setRecords(loader.loadRowDataSet(result));
				Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = CorrespondenceProjector.projectCorrespondences(model.getSchemaCorrespondences(), model.getRecords().getSchema(), result, model.getAttributes(), false).distinct();
				// remove correspondences between a single column (two corresponding columns were stitched together)
				schemaCorrespondences = schemaCorrespondences.where((c)->c.getFirstRecord().getIdentifier()!=c.getSecondRecord().getIdentifier());
				model.setSchemaCorrespondences(schemaCorrespondences);
				model.setAttributes(model.getRecords().getSchema());
				System.out.println(String.format("[stitchTables] %d schema correspondences after projection", schemaCorrespondences.size()));
				
				try {
					Correspondence.toGraph(schemaCorrespondences.get()).writePajekFormat(new File(logDirectory, "stitched_correspondences.net"));
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				System.out.println(String.format("%d attributes / %d records in total after table stitching", model.getAttributes().size(), model.getRecords().size()));
			} else {
				System.out.println("Stitching did not result in any new tables!");
			}

			return result;
		}
	}

	private Collection<Table> filterNormalisedTables(Collection<Table> normalised, Map<String, Table> entityIdToEntityTable) throws Exception {

		BufferedWriter relationLog = new BufferedWriter(new FileWriter(new File(logDirectory, "extracted_relations.log")));
		
		/*********************************************************************************
		 * post-process tables: check that FK exists and filter by density
		 *********************************************************************************/
		
		Collection<Table> result = new LinkedList<>();
		Distribution<String> fkDistribution = new Distribution<>();
		
		// check that an FK column exists and filter by density
		for(Table t : normalised) {			
			Set<Table> relatedTables = new HashSet<>();

			TableColumn fkColumn = null;
			Object firstFKValue = null;
			
			for(TableColumn c : t.getColumns()) {
				if(c.getHeader().equals("FK")) {
					fkColumn = c;
					break;
				}
			}
			
			if(fkColumn!=null) {
				List<TableRow> toRemove = new LinkedList<>();
				
				// add the FK values and measure value density per FK value
				// use column index as key because we change the table name, which affects the column ids (so we cannot use the ids as key)
				Map<Integer, Collection<String>> fkDensity = new HashMap<>();
				Map<Integer, Integer> valueCount = new HashMap<>();
				
				for(TableRow r : t.getRows()) {
					
					// check if the row has any values
					boolean hasValues = false;
					for(TableColumn c : t.getColumns()) {
						if(!ContextColumns.isContextColumn(c) && c!=fkColumn) {
							if(r.get(c.getColumnIndex())!=null) {
								hasValues=true;
								break;
							}
						}
					}
					
					if(hasValues) {
						Object fkValue = r.get(fkColumn.getColumnIndex());

						if(fkValue!=null) {
							String fk = fkValue.toString();

							fkDistribution.add(fk);
							
							Table entityTable = entityIdToEntityTable.get(fk);
							if(entityTable!=null) {
								relatedTables.add(entityTable);

								if(firstFKValue==null) {
									firstFKValue = fkValue;
								}
							} else {
								System.out.println(String.format("[filterNormalisedTables] Table #%d %s: No entity table for FK value %s!", t.getTableId(), t.getPath(), fk));
							}
							
							// increment density
							// boolean hasOnlyKeyValues = true;
							Set<TableColumn> keyColumns = new HashSet<>();
							Set<TableColumn> columnsWithValue = new HashSet<>();
							for(Set<TableColumn> key : t.getSchema().getCandidateKeys()) {
								keyColumns.addAll(key);
							}
							for(TableColumn c : t.getColumns()) {
								if(c!=fkColumn && r.get(c.getColumnIndex())!=null) {
									Collection<String> fkValues = fkDensity.get(c.getColumnIndex());
									if(fkValues==null) {
										fkValues = new HashSet<>();
										fkDensity.put(c.getColumnIndex(), fkValues);
									}
									fkValues.add(fk.toString());
									MapUtils.increment(valueCount, c.getColumnIndex());
								}

								if(!keyColumns.contains(c) && r.get(c.getColumnIndex())!=null) {
									columnsWithValue.add(c);
								}
							}

							if(keyColumns.size()<t.getColumns().size() && Q.any(t.getSchema().getCandidateKeys(), (key)->key.equals(columnsWithValue))) {
								// if the record has only values for a single candidate key and no other values and the table is not all-key, then remove the record
								toRemove.add(r);
							}
						} else {
//							System.err.println(String.format("[RelationReconstructur] Missing FK value for row: %s", r.format(20)));
							toRemove.add(r);
						}
					} else {
						toRemove.add(r); // remove rows that do not contain any values
					}
				}
				
				// remove rows that did not get an FK value assigned
				// toRemove = Q.sort(toRemove, new Comparator<TableRow>() {

				// 	@Override
				// 	public int compare(TableRow o1, TableRow o2) {
				// 		return -Integer.compare(o1.getRowNumber(), o2.getRowNumber());
				// 	}
					
				// });
				
				// for(TableRow r : toRemove) {
				// 	t.getRows().remove(r.getRowNumber());
				// }
				
				if(relatedTables.size()>1) {
					System.err.println(String.format("[filterNormalisedTables] Multiple entity tables for relation table %d: %s", t.getTableId(),
							StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
					
					for(Table rel : relatedTables) {
						System.err.println(String.format("\trelated table #%d: %s", rel.getTableId(), StringUtils.join(Q.project(rel.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
					}
					
					// for debugging purposes, add it to the results
//					result.add(t);
				} else if(relatedTables.size()==1) {
					Table entityTable = Q.firstOrDefault(relatedTables);
					
					t.setPath(String.format("%s_rel_%d", entityTable.getPath(), t.getTableId()));
					t.getMapping().setMappedClass(entityTable.getMapping().getMappedClass());
					
					result.add(t);
					

					Collection<TableColumn> columnsToRemove = new LinkedList<>();
					
					if(firstFKValue!=null) {
						Table et = entityIdToEntityTable.get(firstFKValue);
					
						// remove columns below density threshold
						int ttlFKs = et.getRows().size();
						
						System.out.println(String.format("[filterNormalisedTables] Table '%s':'%s' cardinality 1:%f (max 1:%d)", et.getPath(), t.getPath(), t.getRows().size()/(double)ttlFKs, fkDistribution.getMaxFrequency()));
						
						for(Integer idx : fkDensity.keySet()) {
							TableColumn c = t.getSchema().get(idx);
							Collection<String> fksWithValue = fkDensity.get(idx);
							
							if(fksWithValue!=null) {
								int ttlFKsWithValue = fksWithValue.size();
								
								double density = ttlFKsWithValue / (double)ttlFKs;
								int values = valueCount.get(idx);
								
								System.out.println(String.format("[filterNormalisedTables] Table '%s', Column '%s' density %f (%d/%d), %d values", t.getPath(), c, density, ttlFKsWithValue, ttlFKs, values));
								
								columnStats.put(c, new Pair<>(density,-1.0));
								
								if(density<minDensity && !hasPredefinedRelations) {
									
									System.out.println(String.format("[filterNormalisedTables] Table '%s': removing column '%s' (below density threshold: %f < %f)", t.getPath(), c, density, minDensity));
									
									columnsToRemove.add(c);
								}
								
								relationLog.write(String.format("%s\n", 
									StringUtils.join(new String[] {
										taskName,
										t.getPath(),
										c.getHeader(),
										Double.toString(density),
										Integer.toString(ttlFKsWithValue),
										Integer.toString(ttlFKs),
										Integer.toString(values)
									}, "\t")));
							}
						}
					}
					
					if(!hasPredefinedRelations) {
						// remove binary relations that do not satisfy a functional dependency - not needed anymore, but keep the code to calculate the approximation rate for statistics
						// if(t.getColumns().size()==2) {
							// TableColumn fk=null, otherColumn=null;
							// for(TableColumn c : t.getColumns()) {
							// 	if("FK".equals(c.getHeader())) {
							// 		fk = c;
							// 	} else {
							// 		otherColumn = c;
							// 	}
							// }
							TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
							
							Set<TableColumn> binaryAttributes = t.getSchema().getFunctionalDependencies().get(Q.toSet(fk));

							if(binaryAttributes!=null) {
								for(TableColumn otherColumn : binaryAttributes) {
									ApproximateFunctionalDependency approximateFD = ApproximateFunctionalDependencyUtils.determineApproximateFunctionalDependency(t, Q.toSet(fk), otherColumn);
									
									Pair<Double,Double> stats = columnStats.get(otherColumn);
									double density = -1.0;
									
									if(stats==null) {
										System.out.println(String.format("\tNo density information for column %s!", otherColumn));
									} else {
										density = stats.getFirst();
									}
									
									columnStats.put(otherColumn, new Pair<>(density, approximateFD.getApproximationRate()));
									
									if(approximateFD.getApproximationRate()<minFDApproximationRate) {
										System.out.println(String.format("\tRemoving %s (approximation rate %.4f below threshold %.4f)", 
												otherColumn,
												approximateFD.getApproximationRate(), 
												minFDApproximationRate));
										// columnsToRemove.add(otherColumn);
									} else {
										System.out.println(String.format("\t%s is determined with with approximation rate %.4f", 
												otherColumn,
												approximateFD.getApproximationRate()
												));
									}
								}
							}
						// }
					}
					
					for(TableColumn c : columnsToRemove) {
						t.removeColumn(c);
					}
					
					// deduplicate the table
					// System.out.println(String.format("[filterNormalisedTables] Deduplicating Table '%s': {%s}", t.getPath(), StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
					// int before = t.getRows().size();
					// t.deduplicate(t.getColumns(), ConflictHandling.KeepFirst);
					// System.out.println(String.format("\t\tremoved %d duplicates", before-t.getRows().size()));	
				} else {
					System.err.println(String.format("[filterNormalisedTables] Could not find a related entity table for relation table #%d: %s (%d rows with FK value)", t.getTableId(),
							StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
							t.getRows().size()
							));
				}

			} else {
				System.err.println(String.format("[filterNormalisedTables] Table #%d has no FK column: %s", t.getTableId(),
						StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
				continue;
			}
			

		}

		Iterator<Table> tIt = result.iterator();
		while(tIt.hasNext()) {
			Table t = tIt.next();
		
			// remove tables that only contain the FK and context attributes
			boolean hasNonContextColumn = false;
			for(TableColumn c : t.getColumns()) {
				if(!ContextColumns.isContextColumn(c) && !"FK".equals(c.getHeader())) {
					hasNonContextColumn=true;
					break;
				}
			}
			if(!hasNonContextColumn) {
				System.out.println(String.format("Removing table '%s' (no non-empty content columns): %s", 
					t.getPath(),
					StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
					));
				tIt.remove();
				// continue;
			}
			
			
		}
		
		relationLog.close();
		
		return result;
	}

	public Collection<Table> filterDecompositions(Collection<Table> normalised, StitchedModel universal, StitchedModel normalisedUniversal) {
		ColumnTranslator ct = new ColumnTranslator();
		ct.prepare(CorrespondenceProjector.createProjectionCorrespondences(universal.getTables().values(), normalised, universal.getAttributes(), normalisedUniversal.getAttributes()));
		// check if we want to keep the table:
		// - we are only interested in tables with FK in a candidate key 
		// - we are only interested in tables where the dependent attributes are not artifically created (context attributes)
		Set<TableColumn> coveredOriginalAttributes = new HashSet<>();
		Collection<Table> filtered = new LinkedList<>();
		Map<Table, Set<TableColumn>> excluded = new HashMap<>();
		for(Table t : normalised) {
			Table universalRelation = null;
			for(Table u : universal.getTables().values()) {
				if(t.getProvenance().contains(u.getPath())) {
					universalRelation = u;
					break;
				}
			}

			TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
			Collection<TableColumn> originalAttributes = Q.without(
				Q.where(t.getColumns(), new ContextColumns.IsNoContextColumnPredicate()),
				Q.toSet(fk)
				);

			if(
				// Q.any(t.getSchema().getCandidateKeys(), (k)->k.contains(fk) && Q.without(originalAttributes, k).size()>0)
				t.getColumns().contains(fk)
			) {
				System.out.println(String.format("[DecompositionFilter] keeping table #%d: {%s}",
					t.getTableId(),
					StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
				));
				filtered.add(t);
				coveredOriginalAttributes.addAll(ct.translateColumns(originalAttributes, universalRelation));
			} else {
				System.out.println(String.format("[DecompositionFilter] excluding table #%d: {%s}",
					t.getTableId(),
					StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
				));
				Collection<TableColumn> originalAttributesInUniversal = ct.translateColumns(originalAttributes, universalRelation);
				excluded.put(t, new HashSet<>(originalAttributesInUniversal));
			}
		}
		// - BUT: if MVDs exist in the data, we might not find a candidate key with FK and a non-context attribute as dependant
		// - so, we first apply both rules, then we add tables for all missing, original attributes which violate the rules
		for(Table universalRelation : universal.getTables().values()) {
			Set<TableColumn> missingOriginalAttributes = new HashSet<>(
				Q.without(
					Q.where(universalRelation.getColumns(), (c)->!"FK".equals(c.getHeader()) && !ContextColumns.isContextColumn(c)),
					coveredOriginalAttributes
				)
			);

			if(missingOriginalAttributes.size()>0) {
				System.out.println(String.format("[DecompositionFilter] identified %d missing attributes {%s} for universal relation {%s}",
					missingOriginalAttributes.size(),
					StringUtils.join(Q.project(missingOriginalAttributes, (c)->c.getHeader()), ","),
					StringUtils.join(Q.project(universalRelation.getColumns(), (c)->c.getHeader()), ",")
					));

				// choose excluded tables with the largest number of excluded attributes
				Set<TableColumn> missingOriginalAttributesFinal = missingOriginalAttributes;
				Collection<Table> sorted = Q.sort(excluded.keySet(), (t1,t2)->-Integer.compare(
					Q.intersection(missingOriginalAttributesFinal, ct.translateColumns(t1.getColumns(), t1, universalRelation)).size(),
					Q.intersection(missingOriginalAttributesFinal, ct.translateColumns(t2.getColumns(), t2, universalRelation)).size()

				));

				while(missingOriginalAttributes.size()>0 && sorted.size()>0) {
					Table t = Q.firstOrDefault(sorted);
					sorted.remove(t);

					Set<TableColumn> addedAttributes = Q.intersection(excluded.get(t), missingOriginalAttributes);

					if(addedAttributes.size()>0) {
						System.out.println(String.format("[DecompositionFilter]\tadding missing attributes {%s} via excluded table #%d: {%s}",
							StringUtils.join(Q.project(addedAttributes, (c)->c.getHeader()), ","),
							t.getTableId(),
							StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
							));

						filtered.add(t);

						missingOriginalAttributes = new HashSet<>(
							Q.without(missingOriginalAttributes,
							excluded.get(t)
							));

					}
				}
			}
		}
		return filtered;
	}
	public void removeEmptyRows(Collection<Table> tables) {
		for(Table t : tables) {
			// remove empty rows = rows which have no value for any non-key attribute
			if(t.getSchema().getCandidateKeys().size()>0) {
				Set<TableColumn> primeAttributes = new HashSet<>();
				for(Set<TableColumn> candidateKey : t.getSchema().getCandidateKeys()) {
					primeAttributes.addAll(candidateKey);
				}
				ArrayList<TableRow> nonEmpty = new ArrayList<>(t.getSize());
				for(TableRow r : t.getRows()) {
					for(TableColumn nonPrime : Q.without(t.getColumns(), primeAttributes)) {
						if(r.get(nonPrime.getColumnIndex())!=null) {
							nonEmpty.add(r);
							break;
						}
					}
				}
				nonEmpty.trimToSize();
				System.out.println(String.format("Table #%d: Removal of empty rows resulted in %d/%d rows / prime attributes: {%s}", 
					t.getTableId(),
					nonEmpty.size(), 
					t.getSize(),
					StringUtils.join(Q.project(primeAttributes, (c)->c.getHeader()), ",")
				));
				t.setRows(nonEmpty);
				t.reorganiseRowNumbers();
			}
		}
	}



}
