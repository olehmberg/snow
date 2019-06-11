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
package de.uni_mannheim.informatik.dws.tnt.match.preprocessing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.ColumnTranslator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CreateCorrespondencesForGeneratedColumns {

	public Processable<Correspondence<MatchableTableColumn, Matchable>> run(
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			DataSet<MatchableTableColumn, MatchableTableColumn> attributes,
			Map<Integer, Map<Integer, TableColumn>> tableToColumnToGenerated,
			Map<Integer, Table> tables) {
		Processable<Correspondence<MatchableTableColumn, Matchable>> newCors = new ProcessableCollection<>();

		if(schemaCorrespondences!=null) {
			ColumnTranslator ct = new ColumnTranslator();
			ct.prepare(schemaCorrespondences);
			
			DisjointHeaders dh = DisjointHeaders.fromTables(tables.values());

			// create correspondences for new columns
			// if there is a correspondence between 'a' and 'b', then there should also be one between 'a 2' and 'b 2' 
			for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
				
				// check if the correspondence is between two columns that are disambiguated
				Map<Integer, TableColumn> table1Generated = tableToColumnToGenerated.get(cor.getFirstRecord().getTableId());
				Map<Integer, TableColumn> table2Generated = tableToColumnToGenerated.get(cor.getSecondRecord().getTableId());
				
				if(table1Generated!=null && table2Generated!=null) {
					TableColumn generated1 = table1Generated.get(cor.getFirstRecord().getColumnIndex());
					TableColumn generated2 = table2Generated.get(cor.getSecondRecord().getColumnIndex());
					
					if(generated1!=null && generated2!=null) {
						
						// both columns in the current correspondence have a disambiguation column

						// don't create a correspondence to another disambiguation column if it is already mapped to another column in the other table
						Collection<TableColumn> generated1MappedColumns = ct.getAllMappedColumns(generated1, tables);
						Collection<TableColumn> generated2MappedColumns = ct.getAllMappedColumns(generated2, tables);

						// make sure that linking the two generated columns does not cause a conflict
						// only generate a correspondence if no other ones exist (this we we cannot create any inconsistencies)
						// - or if it is only mapped to disambiguations of the same column
						if(generated1MappedColumns.size()==0 && generated2MappedColumns.size()==0
							|| 
							Q.all(generated1MappedColumns, (c)->generated2.getHeader().equals(c.getHeader())) && Q.all(generated2MappedColumns, (c)->generated1.getHeader().equals(c.getHeader()))
						) {

							Correspondence<MatchableTableColumn, Matchable> newCor = new Correspondence<MatchableTableColumn, Matchable>(attributes.getRecord(generated1.getIdentifier()), attributes.getRecord(generated2.getIdentifier()), cor.getSimilarityScore());
							
							newCors.add(newCor);

						}
					}
				
				}
			}
		}
		
		return newCors;
	}
	
	public void writeN2NGoldstandard(Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences, File f) throws IOException {
		N2NGoldStandard generated = new N2NGoldStandard();
		
		Set<Collection<MatchableTableColumn>> clustering = Correspondence.getConnectedComponents(correspondences.get());
		
		for(Collection<MatchableTableColumn> cluster : clustering) {
			Distribution<String> headerDistribution = Distribution.fromCollection(cluster, (c)->c.getHeader());
			
			generated.getCorrespondenceClusters().put(
					new HashSet<>(Q.project(cluster, (c)->c.getIdentifier()))
					, headerDistribution.getMode()
					);
		}
		
		generated.writeToTSV(f);
	}
	
	public void writeEntityDefinition(Collection<TableColumn> generatedColumns, File f) throws IOException {
		BufferedWriter w = new BufferedWriter(new FileWriter(f));
		
		Map<String, Collection<TableColumn>> grouped = Q.group(generatedColumns, (c)->c.getHeader());
		
		for(String s : grouped.keySet()) {
			w.write(String.format("%s\t%s\n", 
					s
					, StringUtils.join(Q.project(grouped.get(s), (c)->c.getIdentifier()), ",")
					));
		}
		
		w.close();
	}
	
}
