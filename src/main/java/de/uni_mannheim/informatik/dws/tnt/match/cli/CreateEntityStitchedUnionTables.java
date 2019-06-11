///** 
// *
// * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
// * 
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * 
// * 		http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// */
//package de.uni_mannheim.informatik.dws.tnt.match.cli;
//
//import java.io.File;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Set;
//
//import com.beust.jcommander.Parameter;
//
//import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
//import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
//import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
//import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
//import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
//import de.uni_mannheim.informatik.dws.tnt.match.matchers.CandidateKeyBasedMatcher;
//import de.uni_mannheim.informatik.dws.tnt.match.matchers.DeterminantBasedMatcher;
//import de.uni_mannheim.informatik.dws.tnt.match.matchers.EntityLabelBasedMatcher;
//import de.uni_mannheim.informatik.dws.tnt.match.matchers.LabelBasedMatcher;
//import de.uni_mannheim.informatik.dws.tnt.match.matchers.TableToTableMatcher;
//import de.uni_mannheim.informatik.dws.tnt.match.matchers.ValueBasedMatcher;
//import de.uni_mannheim.informatik.dws.tnt.match.preprocessing.TableDisambiguationExtractor;
//import de.uni_mannheim.informatik.dws.tnt.match.preprocessing.TableNumberingExtractor;
//import de.uni_mannheim.informatik.dws.tnt.match.stitching.EntityStitchedUnionTables;
//import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
//import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
//import de.uni_mannheim.informatik.dws.winter.model.Matchable;
//import de.uni_mannheim.informatik.dws.winter.model.Pair;
//import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
//import de.uni_mannheim.informatik.dws.winter.processing.Group;
//import de.uni_mannheim.informatik.dws.winter.processing.Processable;
//import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
//import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
//import de.uni_mannheim.informatik.dws.winter.utils.Executable;
//import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
//import de.uni_mannheim.informatik.dws.winter.webtables.Table;
//import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
//import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
//import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;
//import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;
//
///**
// * @author Oliver Lehmberg (oli@dwslab.de)
// *
// */
//public class CreateEntityStitchedUnionTables extends Executable {
//
//	public static enum MatcherType {
//		Trivial,
//		NonTrivialPartial,
//		NonTrivialFull,
//		CandidateKey,
//		Label,
//		Entity
//	}
//	
//	@Parameter(names = "-matcher")
//	private MatcherType matcher = MatcherType.NonTrivialFull;
//	
//	@Parameter(names = "-web", required=true)
//	private String webLocation;
//	
//	@Parameter(names = "-results", required=true)
//	private String resultLocation;
//	
//	@Parameter(names = "-csv", required=true)
//	private String csvLocation;
//	
//	@Parameter(names = "-correspondences")
//	private String correspondencesLocation;
//	
//	@Parameter(names = "-serialise")
//	private boolean serialise;
//	
//	@Parameter(names = "-entityStructure")
//	private String entityStructureLocation;	// a file that defines the keys for the union tables' schema
//	
//	@Parameter(names = "-entityDefinition")
//	private String entityDefinitionLocation; // a folder that contains prototype tables for different types of entities
//	
//	@Parameter(names = "-candidateKeys")
//	private String candidateKeysLocation;
//	
//	@Parameter(names = "-relations")
//	private String relationsLocation;
//	
//	@Parameter(names = "-normalise")
//	private boolean normaliseRelations = false;
//	
//	@Parameter(names = "-logEntityDetection")
//	private boolean logEntityDetection = false;
//	
//	@Parameter(names = "-logTableStatistics")
//	private boolean logTableStatistics = false;
//	
//	public static void main(String[] args) throws Exception {
//		CreateEntityStitchedUnionTables app = new CreateEntityStitchedUnionTables();
//		
//		if(app.parseCommandLine(CreateEntityStitchedUnionTables.class, args)) {
//			app.run();
//		}
//	}
//	
//	public void run() throws Exception {
//		int firstTableId = 0;
//		if(entityDefinitionLocation!=null) {
////			EntityTableMatcher em = new EntityTableMatcher();
////			em.loadEntityDefinitions(new File(entityDefinitionLocation));
////			firstTableId = em.getKnowledgeBase().getClassIds().size();
//			firstTableId = new File(entityDefinitionLocation, "tables/").list().length;
//		}
//		
//		System.err.println("Loading Web Tables");
//		File webLocationFile = new File(webLocation);
//		String taskName = webLocationFile.getParentFile().getName();
////		WebTables web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise, firstTableId);
//		WebTables web = WebTables.loadWebTables(webLocationFile, true, true, true, serialise, firstTableId);
//		web.removeHorizontallyStackedTables();
//		
//		// remove provenance data for original web tables
//		for(Table t : web.getTables().values()) {
//			for(TableColumn c : t.getColumns()) {
//				c.getProvenance().clear();
//			}
//			for(TableRow r : t.getRows()) {
//				r.getProvenance().clear();
//			}
//		}
//
////		if(logTableStatistics) {
////			web.printDensityReport();
////			for(Table t : web.getTables().values()) {
////				TableColumnDomainStatistics.printColumnDomainStatistics(t);
////			}
////		}
//		
//		//TODO improve table pre-processing
//		// - determine table clusters based on schema & context patterns (alex)
//		//TODO split values into lists using ',' and '&' ?
//		
//		/*****************************************************************************************
//		 * SCHEMA MATCHING (Table-to-Table)
//		 ****************************************************************************************/
//		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = null;
//		if(correspondencesLocation==null) {
//			System.err.println("Matching Union Tables");
//			schemaCorrespondences = runTableMatching(web);
//		} else {
//			System.err.println("Loading Union Table Correspondences");
//			N2NGoldStandard corGs = new N2NGoldStandard();
//			corGs.loadFromTSV(new File(correspondencesLocation));
//			schemaCorrespondences = corGs.toCorrespondences(web.getSchema());
//			
//			// get table clusters
//			Set<Collection<Integer>> clustering = Correspondence.getDataSourceClusters(schemaCorrespondences);
//			
//			// group by table cluster & context column header
//			Map<Integer, Integer> tableToClusterId = new HashMap<>();
//			for(Collection<Integer> cluster : clustering) {
//				Integer clusterId = Q.firstOrDefault(cluster);
//				
//				for(Integer tableId : cluster) {
//					tableToClusterId.put(tableId, clusterId);
//				}
//			}
//			
//			// add correspondences between context columns
//			Processable<Group<String, MatchableTableColumn>> contextGroups = web.getSchema().group(new RecordKeyValueMapper<String, MatchableTableColumn, MatchableTableColumn>() {
//
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public void mapRecordToKey(MatchableTableColumn record,
//						DataIterator<Pair<String, MatchableTableColumn>> resultCollector) {
//					if(ContextColumns.isContextColumn(record)) {
//						resultCollector.next(new Pair<String, MatchableTableColumn>(tableToClusterId.get(record.getTableId()) + "||" + record.getHeader(), record));
//					}
//				}
//			});
//			
//			Processable<Correspondence<MatchableTableColumn, Matchable>> contextCorrespondences = contextGroups.map(new RecordMapper<Group<String,MatchableTableColumn>, Correspondence<MatchableTableColumn, Matchable>>() {
//
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public void mapRecord(Group<String, MatchableTableColumn> record,
//						DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) {
//					
//					for(MatchableTableColumn c1 : record.getRecords().get()) {
//						for(MatchableTableColumn c2 : record.getRecords().get()) {
//							if(c1!=c2) {
//								Correspondence<MatchableTableColumn, Matchable> cor = new Correspondence<MatchableTableColumn, Matchable>(c1, c2, 1.0);
//								resultCollector.next(cor);
//							}
//						}
//					}
//					
//				}
//			});
//			
//			schemaCorrespondences = schemaCorrespondences.append(contextCorrespondences).distinct();
//			
//			System.out.println(String.format("Loaded %d union table correspondences", schemaCorrespondences.size()));
//		}
//		
//		/*****************************************************************************************
//		 * PRE-PROCESSING: Extract disambiguations into individual columns
//		 ****************************************************************************************/
//		TableDisambiguationExtractor dis = new TableDisambiguationExtractor();
//		//TODO add square brackets to disambiguation extraction
//		dis.extractDisambiguations(web, schemaCorrespondences);
//		TableNumberingExtractor num = new TableNumberingExtractor();
//		num.extractNumbering(web, schemaCorrespondences);
//		System.out.println(String.format("%d schema correspondences after table pre-processing", schemaCorrespondences.size()));
//		web.reloadRecords();
//		
//		if(logTableStatistics) {
//			web.printDensityReport();
//		}
//		
//		/*****************************************************************************************
//		 * ENTITY STITCHING
//		 ****************************************************************************************/
//		System.err.println("Creating Stitched Union Tables");
//		EntityStitchedUnionTables entityStitchedUnion = null;
//		if(entityStructureLocation==null) {
//			entityStitchedUnion = new EntityStitchedUnionTables(normaliseRelations, taskName, webLocationFile.getParentFile());
//		} else {
//			File entityFile = new File(entityStructureLocation);
//			File keyFile = new File(candidateKeysLocation);
//			File relationFile = relationsLocation==null ? null : new File(relationsLocation);
//			entityStitchedUnion = new EntityStitchedUnionTables(entityFile, keyFile, relationFile, normaliseRelations, taskName, webLocationFile.getParentFile());
//		}
//		entityStitchedUnion.setLogEntityDetection(logEntityDetection);
//		File csvLocationFile = new File(csvLocation);
//		csvLocationFile.mkdirs();
//		
//		if(entityDefinitionLocation!=null) {
//			entityStitchedUnion.setEntityDefinitionLocation(new File(entityDefinitionLocation));
//			entityStitchedUnion.setDisjointHeaders(DisjointHeaders.fromTables(web.getTables().values()));
//		}
//		
//		Collection<Table> reconstructed = entityStitchedUnion.create(web.getTables(), web.getRecords(), web.getSchema(), web.getCandidateKeys(), schemaCorrespondences, csvLocationFile);
//		
//		File outFile = new File(resultLocation);
//		outFile.mkdirs();
//		System.err.println("Writing Stitched Union Tables");
//		JsonTableWriter w = new JsonTableWriter();
//		w.setWriteMapping(true);
//		CSVTableWriter csvW = new CSVTableWriter();
//		for(Table t : reconstructed) {
//			w.write(t, new File(outFile, t.getPath()));
//			csvW.write(t, new File(csvLocationFile, t.getPath()));
//		}
//		
//		System.err.println("Done.");
//	}
//	
//	private Processable<Correspondence<MatchableTableColumn, Matchable>> runTableMatching(WebTables web) throws Exception {
//		TableToTableMatcher matcher = null;
//		
//    	switch(this.matcher) {
//		case CandidateKey:
//			matcher = new CandidateKeyBasedMatcher();
//			break;
//		case Label:
//			matcher = new LabelBasedMatcher();
//			break;
//		case NonTrivialFull:
//			matcher = new DeterminantBasedMatcher();
//			break;
//		case Trivial:
//			matcher = new ValueBasedMatcher();
//			break;
//		case Entity:
//			matcher = new EntityLabelBasedMatcher();
//			break;
//		default:
//			break;
//    		
//    	}
//    	
//    	DisjointHeaders dh = DisjointHeaders.fromTables(web.getTables().values());
//    	Map<String, Set<String>> disjointHeaders = dh.getAllDisjointHeaders();
//    	
//    	matcher.setWebTables(web);
//    	matcher.setMatchingEngine(new MatchingEngine<>());
//    	matcher.setDisjointHeaders(disjointHeaders);
//    	matcher.setVerbose(true);
//		
//    	matcher.initialise();
//    	matcher.match();
//    	
//    	Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = matcher.getSchemaCorrespondences();
//    	
//    	return schemaCorrespondences;
//	}
//
//}
