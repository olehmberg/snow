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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableDataSetLoader;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers.EntityTableMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers.RelationTableMatcher;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.io.CSVCorrespondenceFormatter;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchEntityStitchedTables extends Executable {

	@Parameter(names = "-web", required=true)
	private String webLocation;
	
	@Parameter(names = "-entityDefinition", required=true)
	private String entityDefinitionLocation; // a folder that contains prototype tables for different types of entities
	
	@Parameter(names = "-entityGS")
	private String entityGSLocation;
	
	@Parameter(names = "-schemaGS")
	private String schemaGSLocation;
	
	@Parameter(names = "-learnLinkageRule")
	private boolean learnLinkageRule;
	
	@Parameter(names = "-applyModel")
	private boolean useLearnedLinkageRule = false;
	
	@Parameter(names = "-testDuplicateBased")
	private boolean testDuplicateBased;
	
	@Parameter(names = "-additionalClasses")
	private String additionalClasses;
	
	@Parameter(names = "-testBlocking")
	private boolean testBlocking;
	
	@Parameter(names = "-results")
	private String resultsLocation; 
	
	@Parameter(names = "-blockFilter")
	private double blockFilter = 0.9;
	
	@Parameter(names = "-verbose")
	private boolean verbose;
	
	@Parameter(names = "-oneToOne")
	private boolean createOneToOneMapping;
	
	@Parameter(names = "-createCandidates")
	private boolean createCandidates = false;
	
	@Parameter(names = "-join")
	private String joinLocation = null;
	
	@Parameter(names = "-log")
	private String logLocation;

	@Parameter(names = "-addKbAttributes")
	private boolean addValuesFromKb = false;
	
	@Parameter(names = "-surfaceForms")
	private boolean useSurfaceForms = false;

	public static void main(String[] args) throws Exception {
		MatchEntityStitchedTables app = new MatchEntityStitchedTables();
		
		if(app.parseCommandLine(MatchEntityStitchedTables.class, args)) {
			app.run();
		}
	}
	
	public void run() throws Exception {
		
		WebTables web = WebTables.loadWebTables(new File(webLocation), true, true, true, false);
		
		File logDirectory = null;
		if(logLocation!=null) {
			logDirectory = new File(logLocation);
		}
		
		EntityTableMatcher entityMatcher = new EntityTableMatcher(0);
//    	if(entityGSLocation!=null) {
//    		entityMatcher.loadEntityGoldStandard(new File(entityGSLocation));
//    	}
		

		// entityMatcher.loadEntityDefinitions(new File(entityDefinitionLocation), true);
		entityMatcher.loadEntityDefinitions(new File(entityDefinitionLocation), useSurfaceForms);
    	File logDir = new File(webLocation);
    	if(logDir.isFile()) {
    		logDir = logDir.getParentFile().getParentFile();
    	} else {
    		logDir = logDir.getParentFile();
    	}
    	
    	entityMatcher.setLogDirectory(logDir, "");
		
		Collection<Table> entityTables = Q.where(web.getTables().values(), (t)->!t.getPath().contains("_rel_"));
		Collection<Table> relationTables = Q.where(web.getTables().values(), (t)->t.getPath().contains("_rel_"));
		
		// System.out.println("Entity Tables:");
		// for(Table t : entityTables) {
			
		// }
		
    	/*****************************************************************************************
		 * LINK ENTITY TABLES TO ENTITY DEFINITION
		 ****************************************************************************************/
//		for(Table t : web.getTables().values()) {
//			System.out.println(t.getPath());
//			for(TableRow r : t.getRows()) {
//				System.out.println(r.format(30));
//			}
//		}
				
		Map<String, String> pkToEntity = new HashMap<>();
		
		if(createCandidates) {
			// create instance correspondence candidate for annotation
			
			for(Table t : entityTables) {
				System.out.println(String.format("[MatchEntityStitchedTables] Creating candidate correspondences for annotation: %s", t.getPath()));
				Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = entityMatcher.linkEntitiesForAnnotation(t, true, blockFilter, pkToEntity);
				new CSVCorrespondenceFormatter().writeCSV(new File(new File(resultsLocation),  t.getPath() + ".csv"), instanceCorrespondences);
			}
			return;
		} else {
			// run identity resolution & evaluation
			
			MatchingGoldStandard gs =null;
			Distribution<String> classDistribution = new Distribution<>();
			Distribution<String> negativeclassDistribution = new Distribution<>();
			if(entityGSLocation!=null) {
				gs =  new MatchingGoldStandard();
				gs.loadFromTSVFile(new File(entityGSLocation));
				gs.setComplete(false);
				int corsInKB = 0;

				// remove all examples from the GS which are not in the loaded KB
				Set<Pair<String,String>> toRemove = new HashSet<>();
				for(Pair<String, String> p : gs.getPositiveExamples()) {
					MatchableTableRow record = entityMatcher.getKnowledgeBase().getRecords().getRecord(p.getSecond());
					if(record!=null) {
						corsInKB++;
						classDistribution.add(entityMatcher.getKnowledgeBase().getClassIndices().get(record.getTableId()));
						if(verbose) {
							MatchableTableRow webRecord = web.getRecords().getRecord(p.getFirst());
							System.out.println(String.format("[GoldStandard] resource in kb: \n\t%s\n\t%s", webRecord.format(20), record.format(30)));
						}
					} else {
						toRemove.add(p);
					}
				}
				for(Pair<String, String> p : gs.getNegativeExamples()) {
					MatchableTableRow record = entityMatcher.getKnowledgeBase().getRecords().getRecord(p.getSecond());
					if(record!=null) {
						negativeclassDistribution.add(entityMatcher.getKnowledgeBase().getClassIndices().get(record.getTableId()));
					}
				}
				System.out.println(String.format("[GoldStandard] %d/%d referenced entities in KB: %s", corsInKB, gs.getPositiveExamples().size(), classDistribution.formatCompact()));
				System.out.println("[GoldsStandard] removing correspondences which are not in KB.");
				gs.getPositiveExamples().removeAll(toRemove);
			}
			
			if(learnLinkageRule && gs!=null) {
				// learn matching rules
				
				Map<String, Collection<Table>> tablesByClass = Q.group(entityTables, (t)->t.getMapping().getMappedClass().getFirst());
				
				for(String className : tablesByClass.keySet()) {
					System.out.println(String.format("[MatchEntityStitchedTables] Learning matching rule: %s", className));
					
					Collection<Table> tables = tablesByClass.get(className);
//					Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences 
					Performance entityPerf = entityMatcher.learnMatchingRule(tables, blockFilter, gs, verbose, createOneToOneMapping, true);
//					MatchingEvaluator<MatchableTableRow, MatchableTableColumn> evaluator = new MatchingEvaluator<>(verbose);
//					Performance entityPerf = evaluator.evaluateMatching(instanceCorrespondences.get(), gs);
					System.out.println(String.format("Entity matching performance (%s):\n\tPrecision:\t%f\n\tRecall:\t%f\n\tF1-measure:\t%f", className, entityPerf.getPrecision(), entityPerf.getRecall(), entityPerf.getF1()));
					
					if(logDirectory!=null) {
						File f = new File(logDirectory, "entity_linking_learning_evaluation.tsv");
						BufferedWriter w = new BufferedWriter(new FileWriter(f, true));
						w.write(String.format("%s\n", 
								StringUtils.join(new String[] {
										className,
										Integer.toString(classDistribution.getFrequency(className)),
										Integer.toString(negativeclassDistribution.getFrequency(className)),
										Double.toString(entityPerf.getPrecision()),
										Double.toString(entityPerf.getRecall()),
										Double.toString(entityPerf.getF1())
								}, "\t")));
						w.close();
					}
				}				
				return;
			} else if(testBlocking && gs!=null) {
				// debug the blocker
				
				System.out.println("[MatchEntityStitchedTables] debugging blocker");
				Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = entityMatcher.testBlocking(entityTables, false, blockFilter, pkToEntity, verbose);
				
				MatchingEvaluator<MatchableTableRow, MatchableTableColumn> evaluator = new MatchingEvaluator<>();
				Performance entityPerf = evaluator.evaluateMatching(instanceCorrespondences.get(), gs);
				System.out.println(String.format("Entity matching performance:\n\tPrecision:\t%f\n\tRecall:\t%f\n\tF1-measure:\t%f", entityPerf.getPrecision(), entityPerf.getRecall(), entityPerf.getF1()));
			} else if(testDuplicateBased) {
				// debug the class matching
				
				System.out.println("[MatchEntityStitchedTables] debugging duplicate-based schema matching");
				entityMatcher.setLogCorrespondences(true);
				for(Table t : entityTables) {
					WebTableDataSetLoader loader = new WebTableDataSetLoader();
					DataSet<MatchableTableRow, MatchableTableColumn> ds = loader.loadDataSet(t);
					
					Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBCorrespondences = new ProcessableCollection<>();
					
					// get the schema mapping
					for(TableColumn c : t.getColumns()) {

						MatchableTableColumn webColumn = ds.getSchema().getRecord(c.getIdentifier());
						
						Pair<String, Double> mapping = t.getMapping().getMappedProperty(c.getColumnIndex());
						if(mapping!=null) {
							String property = mapping.getFirst();
							MatchableTableColumn kbColumn = entityMatcher.getKnowledgeBase().getSchema().getRecord(property);
							
							if(kbColumn!=null) {
								tableToKBCorrespondences.add(new Correspondence<MatchableTableColumn, Matchable>(webColumn, kbColumn, 1.0));
								
								if("rdf-schema#label".equals(kbColumn.getHeader())) {
									//add a second class for testing
									if(additionalClasses!=null) {
										for(String cls : additionalClasses.split(",")) {
											tableToKBCorrespondences.add(new Correspondence<MatchableTableColumn, Matchable>(webColumn, entityMatcher.getKnowledgeBase().getSchema().getRecord(String.format("%s.csv::http://www.w3.org/2000/01/rdf-schema#label", cls)), 1.0));
										}
									}
									
//									tableToKBCorrespondences.add(new Correspondence<MatchableTableColumn, Matchable>(webColumn, entityMatcher.getKnowledgeBase().getSchema().getRecord("Artist.csv::http://www.w3.org/2000/01/rdf-schema#label"), 1.0));
//									tableToKBCorrespondences.add(new Correspondence<MatchableTableColumn, Matchable>(webColumn, entityMatcher.getKnowledgeBase().getSchema().getRecord("OfficeHolder.csv::http://www.w3.org/2000/01/rdf-schema#label"), 1.0));
								}
							}
						}
					}
					
					entityMatcher.runDuplicatBasedSchemaMatching(tableToKBCorrespondences, ds, ds.getSchema(), createOneToOneMapping, web.getTables());
				}
				return;
			}
			else {
				System.out.println(String.format("[MatchEntityStitchedTables] Evaluating identity resolution: %s", 
						StringUtils.join(Q.project(entityTables, (t)->t.getPath()), ", ")
						));
				
				// run identity resolution & evaluation
				Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = entityMatcher.linkEntities(entityTables, addValuesFromKb, blockFilter, pkToEntity, verbose, createOneToOneMapping, useLearnedLinkageRule);

				if(logDirectory!=null) {
					
					// log evaluation
					if(gs!=null) {
						MatchingEvaluator<MatchableTableRow, MatchableTableColumn> evaluator = new MatchingEvaluator<>();
						Performance entityPerf = evaluator.evaluateMatching(instanceCorrespondences.get(), gs);
						System.out.println(String.format("Entity matching performance:\n\tPrecision:\t%f\n\tRecall:\t%f\n\tF1-measure:\t%f", entityPerf.getPrecision(), entityPerf.getRecall(), entityPerf.getF1()));
						
						File f = null;
						if(useLearnedLinkageRule) {
							f = new File(logDirectory, "entity_linking_learned_evaluation.tsv");
						} else {
							f = new File(logDirectory, "entity_linking_evaluation.tsv");
						}
						BufferedWriter w = new BufferedWriter(new FileWriter(f, true));
						w.write(String.format("%s\n", 
								StringUtils.join(new String[] {
										entityGSLocation,
										Double.toString(entityPerf.getPrecision()),
										Double.toString(entityPerf.getRecall()),
										Double.toString(entityPerf.getF1())
								}, "\t")));
						w.close();
					}
					
					// log entity set completion
					for(Table t : entityTables) {
						
						Map<TableColumn, Set<Object>> domain = t.getColumnDomains();
						TableColumn uriColumn = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"URI".equals(c.getHeader())));
						
						System.out.println(String.format("Table '%s': {%s} with URI column '%s'",
							t.getPath(),
							StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ","),
							uriColumn
						));

						File f = null;
						f = new File(logDirectory, "entity_set_completion.tsv");
						BufferedWriter w = new BufferedWriter(new FileWriter(f, true));
						int linkedEntities = domain.containsKey(uriColumn) ? domain.get(uriColumn).size() : 0;
						w.write(String.format("%s\n", 
								StringUtils.join(new String[] {
										t.getPath(),
										useLearnedLinkageRule ? "supervised" : "unsupervised",
										t.getMapping().getMappedClass().getFirst(),
										Integer.toString(linkedEntities),								// linked entities
										Integer.toString(t.getRows().size() - linkedEntities)			// un-linked entities
								}, "\t")));
						w.close();
						
					}
				}
			}
	
		}

    	/*****************************************************************************************
		 * LINK RELATION TABLES TO KB PROPERTIES
		 ****************************************************************************************/
		if(relationTables.size()>0) {
			
			// join all relation tables with their respective entity tables
			joinEntityTablesToRelationTables(web);
			relationTables = Q.project(relationTables, (t)->web.getTables().get(t.getTableId()));

			RelationTableMatcher relMatcher = new RelationTableMatcher();
			// relMatcher.loadKnowledgeBase(new File(entityDefinitionLocation, "tables/"));
			relMatcher.loadKnowledgeBase(new File(entityDefinitionLocation), false);

			for(Table t : relationTables) {

				System.out.println(String.format("Matching schema of table %s", t.getPath()));

				MatchingGoldStandard gs = new MatchingGoldStandard();
				gs.setComplete(true);
					
				// create gold standard from table mapping
				for(TableColumn c : t.getColumns()) {
					Pair<String, Double> columnMapping = t.getMapping().getMappedProperty(c.getColumnIndex());
					if(columnMapping!=null) {
						gs.addPositiveExample(new Pair<>(c.getIdentifier(), columnMapping.getFirst()));
						System.out.println(String.format("Gold Standard: %s -> %s", c, columnMapping.getFirst()));
					}
				}
				
				Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = relMatcher.matchRelationTablesToKnowledgeBase(relationTables);
				
				if(gs.getPositiveExamples().size()>0) {
					
					System.out.println("Gold Standard:");
					for(Pair<String,String> p : gs.getPositiveExamples()) {
						System.out.println(String.format("\t%s -> %s", p.getFirst(), p.getSecond()));
					}
					
					MatchingEvaluator<MatchableTableColumn, Matchable> evaluator = new MatchingEvaluator<>();
					Performance perf = evaluator.evaluateMatching(schemaCorrespondences.get(), gs);
					
					System.out.println(String.format("Schema matching performance:\n\tPrecision:\t%f\n\tRecall:\t%f\n\tF1-measure:\t%f", perf.getPrecision(), perf.getRecall(), perf.getF1()));
					
					Pair<String, Double> clsMapping = t.getMapping().getMappedClass();

					File f = new File(logDirectory, "schema_matching_evaluation.tsv");
					BufferedWriter w = new BufferedWriter(new FileWriter(f, true));
					w.write(String.format("%s\n", 
							StringUtils.join(new String[] {
									t.getPath(),
									clsMapping==null ? "" : clsMapping.getFirst(),
									Integer.toString(gs.getPositiveExamples().size()),
									Integer.toString(t.getColumns().size()),
									Double.toString(perf.getPrecision()),
									Double.toString(perf.getRecall()),
									Double.toString(perf.getF1())
							}, "\t")));
					w.close();
				}
			}
		}
    	
    	/*****************************************************************************************
		 * WRITE RESULTS
		 ****************************************************************************************/
		
		if(resultsLocation!=null) {
			System.out.println("Writing results");
			JsonTableWriter jsonWriter = new JsonTableWriter();
			jsonWriter.setWriteMapping(true);
			CSVTableWriter csvWriter = new CSVTableWriter();
			File results = new File(resultsLocation);
			
			for(Table t : web.getTables().values()) {
				jsonWriter.write(t, new File(results, t.getPath()));
				csvWriter.write(t, new File(results, t.getPath()));
			}
		}
	}
	
	protected void joinEntityTablesToRelationTables(WebTables web) throws Exception {
		for(Table t : web.getTables().values()) {
			if(t.getPath().contains("_rel_")) {
				String entityTableName = t.getPath().replaceAll("_rel_\\d+", "");
				Integer entityTableId = web.getTableIndices().get(entityTableName);
				if(entityTableId==null) {
					System.out.println(String.format("Could not find entity table for '%s'", t.getPath()));
				} else {
					Table entityTable = web.getTables().get(entityTableId);
					
					TableColumn pk = Q.firstOrDefault(Q.where(entityTable.getColumns(), (c)->"PK".equalsIgnoreCase(c.getHeader())));
					TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equalsIgnoreCase(c.getHeader())));
					
					Table joined = t.join(entityTable, Q.toList(new Pair<>(fk, pk)), Q.union(Q.without(entityTable.getColumns(), Q.toList(pk)), Q.without(t.getColumns(), Q.toList(fk))));
					joined.setTableId(t.getTableId());
					
					System.out.println(String.format("joining relation table #%d %s {%s} with entity table {%s} to {%s} / %d rows", 
							t.getTableId(),
							t.getPath(),
							StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
							StringUtils.join(Q.project(entityTable.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
							StringUtils.join(Q.project(joined.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
							t.getRows().size()));
					
					System.out.println("*** Schema mapping of entity table ***");
					for(TableColumn c : entityTable.getColumns()) {
						Pair<String, Double> columnMapping = entityTable.getMapping().getMappedProperty(c.getColumnIndex());
						if(columnMapping!=null) {
							System.out.println(String.format("Schema mapping (gs): %s -> %s", c, columnMapping.getFirst()));
						}
					}
					System.out.println("*** Schema mapping of relation table ***");
					for(TableColumn c : t.getColumns()) {
						Pair<String, Double> columnMapping = t.getMapping().getMappedProperty(c.getColumnIndex());
						if(columnMapping!=null) {
							System.out.println(String.format("Schema mapping (gs): %s -> %s", c, columnMapping.getFirst()));
						}
					}
					System.out.println("*** Schema mapping of joined table ***");
					for(TableColumn c : joined.getColumns()) {
						Pair<String, Double> columnMapping = joined.getMapping().getMappedProperty(c.getColumnIndex());
						if(columnMapping!=null) {
							System.out.println(String.format("Schema mapping (gs): %s -> %s", c, columnMapping.getFirst()));
						}
					}
					
					web.getTables().put(t.getTableId(), joined);
					
					String path = String.format("%s_%s", entityTable.getPath(), t.getPath());
					
					if(joinLocation!=null) {
						JsonTableWriter w = new JsonTableWriter();
						w.setWriteMapping(true);
						w.write(joined, new File(new File(joinLocation), path));
					}
					
					joined.setPath(path);
				}
			}
		}
	}
}
