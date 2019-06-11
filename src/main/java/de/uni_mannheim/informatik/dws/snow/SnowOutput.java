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

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.*;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FDScorer;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.StitchedFunctionalDependencyUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import org.apache.commons.lang.StringUtils;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.*;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SnowOutput {

	public static void logFDScoring(
        StitchedModel universal, 
        StitchedModel model, 
        Map<Table, EntityTable> entityTables, 
        Map<String, Table> entityIdToEntityTable, 
        String taskName,
        File logDirectory
    ) throws IOException {
		FDScorer scorer = new FDScorer();
		File logFile = new File(logDirectory.getParentFile(), "fd_scores.tsv");
		System.out.println(String.format("[FD Scoring] writing all scores to %s", logFile.getAbsolutePath()));
		scorer.addScoringFunction(FDScorer.dataTypeScore, "data type score");
		scorer.addScoringFunction(FDScorer.lengthScore, "length score");
		scorer.addScoringFunction(FDScorer.extensionalDeterminantProvenanceScore, "extensional determinant provenance score");
		scorer.addScoringFunction(FDScorer.disambiguationOfForeignKey, "disambiguation of FK score");
		scorer.addScoringFunction(FDScorer.filenameScore, "filename score");
		scorer.addScoringFunction(FDScorer.disambiguationSplitScore, "disambiguation split score");
		// scorer.addScoringFunction(FDScorer.dataTypeScore, "data type score");
		// scorer.addScoringFunction(FDScorer.lengthScore, "length score");
		// scorer.addScoringFunction(FDScorer.positionScore, "position score");
		// scorer.addScoringFunction(FDScorer.determinantProvenanceScore, "determinant provenance score");
		// scorer.addScoringFunction(FDScorer.dependantProvenanceScore, "determinant provenance score");
		// scorer.addScoringFunction(FDScorer.extensionalDeterminantProvenanceScore, "extensional determinant provenance score");
		// scorer.addScoringFunction(FDScorer.extensionalDependantProvenanceScore, "extensional dependant provenance score");
		// scorer.addScoringFunction(FDScorer.duplicationScore, "duplication score");
		// scorer.addScoringFunction(FDScorer.attributeDeterminantFrequency, "attribute determinant frequency score");
		// scorer.addScoringFunction(FDScorer.attributeCombinationDeterminantFrequency, "attribute combination determinant frequency score");
		// scorer.addScoringFunction(FDScorer.attributeDependantFrequency, "attribute dependant frequency score");
		// scorer.addScoringFunction(FDScorer.disambiguation, "disambiguation score");
		// scorer.addScoringFunction(FDScorer.disambiguationOfForeignKey, "disambiguation of FK score");
		// scorer.addScoringFunction(FDScorer.weakFDScore, "weak FD score");
		// scorer.addScoringFunction(FDScorer.strongFDScore, "strong FD score");
		// scorer.addScoringFunction(FDScorer.conflictRateScore, "conflict rate score");
		// scorer.addScoringFunction(FDScorer.filenameScore, "filename score");
		// scorer.addScoringFunction(FDScorer.disambiguationSplitScore, "disambiguation split score");
		
		RecordCSVFormatter formatter = new RecordCSVFormatter();
		DataSet<Record,Attribute> combinedFeatures = new HashedDataSet<>();
		Attribute tableName = new Attribute("table name");
		Attribute entityName = new Attribute("entity name");
		Attribute isAnnotated = new Attribute("annotated");
		combinedFeatures.addAttribute(tableName);
		combinedFeatures.addAttribute(isAnnotated);
		combinedFeatures.addAttribute(entityName);
		for(Attribute a : scorer.getFeatures().getSchema().get()) {
			combinedFeatures.addAttribute(a);
		}
		for(Table t : universal.getTables().values()) {
			System.out.println(String.format("[FD Scoring] scores for table #%d", t.getTableId()));
			// scorer.scoreFunctionalDependenciesForDecomposition(Pair.fromMap(t.getSchema().getFunctionalDependencies()), t, universal);
			scorer.scoreFunctionalDependenciesForDecomposition(Pair.fromMap(StitchedFunctionalDependencyUtils.allClosures(t.getSchema().getFunctionalDependencies(), universal)), t, universal);
			DataSet<Record,Attribute> features = scorer.getFeatures();
			features.addAttribute(isAnnotated);
			List<Attribute> headers = new ArrayList<>(features.getSchema().get());

			System.out.println(StringUtils.join(formatter.getHeader(headers), "\t"));

			EntityTable nonEntityTable = null;
			for(EntityTable et : model.getEntityGroups()) {
            // for(EntityTable et : universal.getEntityGroups()) {
				if(et.isNonEntity()) {
					nonEntityTable = et;
					break;
				}
			}
			// get the corresponding entity table
			TableColumn foreignKey = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
			Object firstFKValue = null;
			for(TableRow r : t.getRows()) {
				firstFKValue = r.get(foreignKey.getColumnIndex());
				if(firstFKValue!=null) {
					break;
				}
			}
			EntityTable ent = null;
			if(firstFKValue!=null) {
				Table et = entityIdToEntityTable.get(firstFKValue);
				ent = entityTables.get(et);
			}
			for(Pair<Set<String>, Set<String>> fdToCheck : Pair.fromMap(ent.getFunctionalDependencies())) {
				Set<String> determinantToCheck = new HashSet<>(fdToCheck.getFirst());

				Set<TableColumn> extraColumns = new HashSet<>();
				if(determinantToCheck.contains("FK")) {
					determinantToCheck.remove("FK");
					extraColumns.add(foreignKey);
				}
				Set<TableColumn> determinant = new HashSet<>(ent.translateAttributes(t, determinantToCheck, nonEntityTable));
				determinant.addAll(extraColumns);
				String fdId = String.format("%d:%s",t.getTableId(),StringUtils.join(Q.project(determinant, (c)->c.getHeader()), ","));
				Record r = features.getRecord(fdId);
				if(r!=null) {
					r.setValue(isAnnotated, "true");
				}
			}

			for(Record record : features.get()) {
				System.out.println(StringUtils.join(formatter.format(record, scorer.getFeatures(), headers), "\t"));
				record.setValue(tableName, t.getPath());
				if(ent!=null) {
					record.setValue(entityName, ent.getEntityName());
				}
				combinedFeatures.add(record);
			}
		}
		List<Attribute> combinedHeaders = new ArrayList<>(combinedFeatures.getSchema().get());
		formatter.writeCSV(new File(logDirectory, "fd_scores.csv"), combinedFeatures, combinedHeaders);

		BufferedWriter globalLog = new BufferedWriter(new FileWriter(logFile, true));
		for(Record record : combinedFeatures.get()) {
			globalLog.write(String.format("%s\t%s\n",
				taskName,
				StringUtils.join(formatter.format(record, combinedFeatures, combinedHeaders), "\t")
			));
		}
		globalLog.close();
	}

	public static void writeColumnClassificationResults(
        Collection<Table> entityTables, 
        Collection<Table> relationTables,
        boolean binaryRelationsOnly,
        File logDirectory,
        Map<String, Integer> originalWebTableColumnCount,
	    Map<String, String> unionTableColumnHeaders,
	    Map<String, Integer> unionColumnRowCount
    ) throws IOException {
		String fileName = binaryRelationsOnly ? "column_classification_binary.tsv" : "column_classification.tsv";
		BufferedWriter w = new BufferedWriter(new FileWriter(new File(logDirectory, fileName)));
		
		Set<String> unionTableColumnIdentifiers = new HashSet<>(originalWebTableColumnCount.keySet());
		
		int numEntities = 0;
		int numBinaryAttributes = 0;
		int numBinaryValues = 0;
		int numNAryTuples = 0;

		// all columns in entity tables are classified as 'entity'
		for(Table t : entityTables) {
			for(TableColumn c : t.getColumns()) {
				System.out.println(String.format("[Classification] %s == entity", c));
				for(String prov : c.getProvenance()) {
					if(unionTableColumnIdentifiers.contains(prov)) {
						int numColumns = originalWebTableColumnCount.get(prov);
						int numRows = unionColumnRowCount.get(prov);
						w.write(StringUtils.join(
							new String[] {
									prov,
									"entity",
									c.getHeader(),
									Integer.toString(numColumns),
									Integer.toString(numRows)
							}
							, "\t"));
						w.write("\n");
						unionTableColumnIdentifiers.remove(prov);
					}
				}
			}
			numEntities+=t.getSize();
		}

		// columns in relation tables are classified as 'binary' if there exists an FD with only the FK in the determinant, and as 'n-ary' otherwise
		for(Table t : relationTables) {
			TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
			// Set<TableColumn> binaryAttributes = t.getSchema().getFunctionalDependencies().get(Q.toSet(fk));
			Collection<TableColumn> independentAttributes = t.getSchema().getFunctionalDependencies().get(new HashSet<>());
			if(independentAttributes==null) {
				independentAttributes = new HashSet<>();
			}
			Collection<TableColumn> binaryAttributes = FunctionalDependencyUtils.closure(Q.toSet(fk), t.getSchema().getFunctionalDependencies(), false);
			// System.out.println(String.format("[ColumnClassification] table %s closure of {FK}: {%s}", 
			// 	t.getPath(),
			// 	StringUtils.join(
			// 		Q.project(binaryAttributes, (c)->c.getHeader()),
			// 		 ",")));
			binaryAttributes = Q.without(binaryAttributes, independentAttributes);
			if(binaryAttributes==null) {
				binaryAttributes = new HashSet<>();
			}

			// special rule:
			// - if a context attribute is dependent on the FK, don't classify it as binary
			// - classify as independent if only in FDs with other context columns
			// - classify as n-ary if in the determinant of any FD that determines a non-context attribute
			// however: exclude disambiguation columns, which are no actual context columns
			// --> consider removing disambiguations from the ContextColumns class

			Collection<TableColumn> contextAttributes = Q.where(t.getColumns(), 
				(c)->ContextColumns.isContextColumn(c) && !c.getHeader().startsWith("Disambiguation of ")
				//new ContextColumns.IsContextColumnPredicate()
				);
			for(TableColumn ctx : contextAttributes) {
				if(!independentAttributes.contains(ctx)) {

					binaryAttributes.remove(ctx);

					// if any FD with the context attribute in the determinant has a non-context attribute in its closure, classify as n-ary
					if(Q.any(
							Pair.fromMap(t.getSchema().getFunctionalDependencies()), 
							(fd)->
								fd.getFirst().contains(ctx) 
								&& Q.any(
									FunctionalDependencyUtils.closure(fd.getFirst(), t.getSchema().getFunctionalDependencies()), 
									(c)->!ContextColumns.isContextColumn(c) || c.getHeader().startsWith("Disambiguation of ")
									//new ContextColumns.IsNoContextColumnPredicate()
									)
							)
						) {

					} else {
						// otherwise classify as independent
						independentAttributes.add(ctx);
					}

				}
			}

			for(TableColumn c : Q.without(t.getColumns(), Q.toSet(fk))) {
				// System.out.println(String.format("[Classification] %s == %s", c, t.getColumns().size()==2 ? "binary" : "n-ary"));
				for(String prov : c.getProvenance()) {
					if(unionTableColumnIdentifiers.contains(prov)) {
						int numColumns = originalWebTableColumnCount.get(prov);
						int numRows = unionColumnRowCount.get(prov);
						w.write(StringUtils.join(
							new String[] {
									prov,
									// t.getColumns().size()==2 ? "binary" : "n-ary",
									independentAttributes.contains(c) ? "independant" :
										binaryAttributes.contains(c) ? "binary" : "n-ary",
									c.getHeader(),
									Integer.toString(numColumns),
									Integer.toString(numRows)
							}
							, "\t"));
						w.write("\n");
						unionTableColumnIdentifiers.remove(prov);
					}
				}
				if(binaryAttributes.contains(c)) {
					numBinaryValues+=t.getSize();
					numBinaryAttributes++;
				}
			}
			if(Q.without(t.getColumns(), Q.union(Q.toSet(fk), binaryAttributes, independentAttributes)).size()>0) {
				numNAryTuples+=t.getSize();
			}
			Collection<TableColumn> independent = independentAttributes;
			Collection<TableColumn> binary = binaryAttributes;
			TableColumn fkFinal = fk;
			System.out.println(String.format("Classification results for '%s': {%s}",
				t.getPath(),
				StringUtils.join(Q.project(Q.without(t.getColumns(), Q.toSet(fkFinal)), (c)->String.format("%s:%s", c.getHeader(), 
					independent.contains(c) ? "independent" : binary.contains(c)?"binary":"n-ary")
					), ",")
			));
			// printFunctionalDependencies(t);
			// printCandidateKeys(t, true);
		}
		
		System.out.println(String.format("Extracted %d entity tables (%d entities) and %d relation tables (%d binary attributes with %d values total, %d n-ary attribute tuples)",
			entityTables.size(),
			numEntities,
			relationTables.size(),
			numBinaryAttributes,
			numBinaryValues,
			numNAryTuples
			));

		// System.out.println(String.format("[Classification] %d remaining columns classified as independent", unionTableColumnIdentifiers.size()));
		for(String s : unionTableColumnIdentifiers) {
			int numColumns = originalWebTableColumnCount.get(s);
			if(unionColumnRowCount.containsKey(s)) {
				int numRows = unionColumnRowCount.get(s);
				// System.out.println(String.format("[Classification] %s == independent", s));
				
				w.write(StringUtils.join(
					new String[] {
							s,
							"independent",
							unionTableColumnHeaders.get(s),
							Integer.toString(numColumns),
							Integer.toString(numRows)
					}
					, "\t"));
				w.write("\n");
			}
		}
		
		w.close();
    }
    
    public static void writeRelationExtractionResults(
        Collection<Table> entityTables, 
        Collection<Table> relationTables,
        boolean binaryRelationsOnly,
        String taskName,
        File logDirectory,
        Map<TableColumn, Pair<Double, Double>> columnStats
    ) throws IOException {
		String fileNameE = "extracted_entity_tables"+ (binaryRelationsOnly ? "_binaryOnly" : "") + ".tsv";
		String fileNameB = "extracted_binary_relations" + (binaryRelationsOnly ? "_binaryOnly" : "") + ".tsv";
		String fileNameN = "extracted_nary_relations" + (binaryRelationsOnly ? "_binaryOnly" : "") + ".tsv";
		
		BufferedWriter w = new BufferedWriter(new FileWriter(new File(logDirectory, fileNameE)));
		for(Table t : entityTables) {
			w.write(String.format("%s\n", StringUtils.join(new String[] {
					taskName,
					t.getMapping().getMappedClass().getFirst(),
					t.getPath(),
					Integer.toString(t.getRows().size())
			}, "\t")));
		}
		w.close();
		
		BufferedWriter wB = new BufferedWriter(new FileWriter(new File(logDirectory, fileNameB)));
		BufferedWriter wN = new BufferedWriter(new FileWriter(new File(logDirectory, fileNameN)));
		// for(Table t : Q.where(relationTables, (t)->t.getColumns().size()==2)) {
		for(Table t : relationTables) {
			TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
			Set<TableColumn> binaryAttributes = t.getSchema().getFunctionalDependencies().get(Q.toSet(fk));
			if(binaryAttributes==null) {
				binaryAttributes = new HashSet<>();
			}

			for(TableColumn rel : binaryAttributes) {
				// TableColumn rel = Q.firstOrDefault(Q.where(t.getColumns(), (c)->!"FK".equals(c.getHeader())));
				Pair<Double,Double> stats = columnStats.get(rel);
				wB.write(String.format("%s\n", StringUtils.join(new String[] {
						taskName,
						t.getMapping().getMappedClass().getFirst(),
						t.getPath(),
						rel.getDataType().toString(),
						rel.getHeader(),
						Integer.toString(t.getRows().size()),
						stats==null ? "-1.0" : Double.toString(stats.getFirst()),	// density
						stats==null ? "-1.0" : Double.toString(stats.getSecond())	// fd-approx
				}, "\t")));
			}

			if(t.getColumns().size()>Q.union(Q.toSet(fk), binaryAttributes).size()) {
				wN.write(String.format("%s\n", StringUtils.join(new String[] {
					taskName,
					t.getMapping().getMappedClass().getFirst(),
					t.getPath(),
					Integer.toString(t.getColumns().size()),
					Integer.toString(t.getRows().size())
					}, "\t")));
			}
			
		}
		wB.close();
		wN.close();
	}

	public static void writeClassMapping(File f, String taskName, Collection<Table> entityTables, Map<String, Integer> originalWebTablesCount) throws IOException {
		BufferedWriter w = new BufferedWriter(new FileWriter(f));
		for(Table t : entityTables) {
			if(t.getMapping().getMappedClass()!=null) {

				int originalTables = 0;
				for(String prov : t.getProvenance()) {
					if(originalWebTablesCount.containsKey(prov)) {
						originalTables += originalWebTablesCount.get(prov);
					}
				}

				w.write(String.format("%s\n", StringUtils.join(new String[] {
					taskName,
					t.getPath(),
					t.getMapping().getMappedClass().getFirst(),
					Integer.toString(t.getSize()),
					Integer.toString(originalTables)
				}, "\t")));
			}
		}
		w.close();
	}
}