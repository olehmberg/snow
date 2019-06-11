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
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SnowStatisticsWriter {

    public static void writeInputStatistics(
        File logDirectory, 
        String taskName, 
        Collection<Table> tables,
        Map<String, Integer> originalWebTablesCount,
        Map<String, Integer> originalWebTableColumnCountByTable,
        Map<Table, Integer> tableColumnsBeforePreprocessing
    ) throws IOException {
		BufferedWriter statWriter = new BufferedWriter(new FileWriter(new File(logDirectory, "input_statistics.tsv")));
		for(Table t : tables) {
			// System.out.println(String.format("Table #%d %s: {%s} / %d rows", 
			// 		t.getTableId(),
			// 		t.getPath(),
			// 		StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
			// 		t.getRows().size()));
			

			statWriter.write(String.format("%s\n", StringUtils.join(
					new String[] {
							taskName,
							t.getPath(),
							Integer.toString(originalWebTablesCount.get(t.getPath())),                                          // original web tables
							Integer.toString(Q.where(t.getColumns(), new ContextColumns.IsNoContextColumnPredicate()).size()),  // non-context attributes in clusterd union tables
							Integer.toString(tableColumnsBeforePreprocessing.get(t)),                                           // attributes in clustered union tables before preprocessing
							Integer.toString(t.getColumns().size()),                                                            // attributes in clustered union tables
							Integer.toString(t.getRows().size()),                                                               // rows in clustered union tables
							Integer.toString(originalWebTableColumnCountByTable.get(t.getPath()))                               // original web table columns
					}
					, "\t")));
			
			
		}
		statWriter.close();
    }

    public static void printStatisticsForFDRelation(Table t) {

		Set<Object> fkValues = new HashSet<>();
		TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));

		Set<TableColumn> determinant = Q.firstOrDefault(t.getSchema().getFunctionalDependencies().keySet());
		Set<TableColumn> dependant = t.getSchema().getFunctionalDependencies().get(determinant);
		TableColumn depCol = Q.firstOrDefault(dependant);

		if(fk!=null) {
			// count fk values
			for(TableRow r : t.getRows()) {
				fkValues.add(r.get(fk.getColumnIndex()));
			}
		}

		System.out.println(String.format("extracted %s: {%s}->{%s} %d tuples, %d entities, average values per entity: %f",
			t.getPath(),
			StringUtils.join(Q.project(determinant, (c)->c.getHeader()), ","),
			depCol.getHeader(),
			t.getSize(),
			fkValues.size(),
			t.getSize() / (double)fkValues.size()
		));
    }

    public static void writeStatisticsForRelations(Collection<Table> tables, String taskName, String configuration, File f) throws IOException {

        BufferedWriter w = new BufferedWriter(new FileWriter(f));

        for(Table t : tables) {
            Set<Object> fkValues = new HashSet<>();
            TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));

            if(fk!=null) {
                // count fk values
                for(TableRow r : t.getRows()) {
                    fkValues.add(r.get(fk.getColumnIndex()));
                }
            }

            String className = "";
            if(t.getMapping().getMappedClass()!=null) {
                className = t.getMapping().getMappedClass().getFirst();
            }

            w.write(String.format("%s\n", StringUtils.join(new String[] {
                taskName,
                configuration,
                t.getPath(),
                className,
                Integer.toString(t.getColumns().size()),                                // attributes
                Integer.toString(t.getSize()),                                          // tuples
                Integer.toString(fkValues.size()),                                      // entities
                Double.toString(t.getSize() / (double)fkValues.size())                  // avg. values per entity
            }, "\t")));
        }

        w.close();
    }

    public static void writeStatisticsForFDRelations(Collection<Table> tables, String taskName, String configuration, File f) throws IOException {

        BufferedWriter w = new BufferedWriter(new FileWriter(f));

        for(Table t : tables) {
            Set<Object> fkValues = new HashSet<>();
            TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));

            Set<TableColumn> determinant = Q.firstOrDefault(t.getSchema().getFunctionalDependencies().keySet());
            Set<TableColumn> dependant = t.getSchema().getFunctionalDependencies().get(determinant);
            TableColumn depCol = Q.firstOrDefault(dependant);

            if(fk!=null) {
                // count fk values
                for(TableRow r : t.getRows()) {
                    fkValues.add(r.get(fk.getColumnIndex()));
                }
            }

            w.write(String.format("%s\n", StringUtils.join(new String[] {
                taskName,
                configuration,
                t.getPath(),
                StringUtils.join(Q.project(determinant, (c)->c.getHeader()), ","),
                depCol.getHeader(),
                Integer.toString(t.getSize()),                                          // tuples
                Integer.toString(fkValues.size()),                                      // entities
                Double.toString(t.getSize() / (double)fkValues.size())                  // avg. values per entity
            }, "\t")));
        }

        w.close();
    }
    
    public static void writeTableToTableGoldStandardStatistics(File f, String taskName, DataSet<MatchableTableColumn, MatchableTableColumn> attributes, N2NGoldStandard goldstandard) throws IOException {
        BufferedWriter w = new BufferedWriter(new FileWriter(f));

        Set<MatchableTableColumn> unmapped = new HashSet<>(attributes.get());

        for(Set<String> cluster : goldstandard.getCorrespondenceClusters().keySet()) {
            String name = goldstandard.getCorrespondenceClusters().get(cluster);

            for(String c : cluster) {
                MatchableTableColumn col = attributes.getRecord(c);
                unmapped.remove(col);
            }

            w.write(String.format("%s\n", StringUtils.join(new String[] {
                taskName,
                name,
                Integer.toString(cluster.size()),								// attributes in cluster
                Integer.toString((cluster.size() * (cluster.size()-1)) / 2)		// correspondences in cluster
            }, "\t")));
        }

        for(MatchableTableColumn c : unmapped) {
            w.write(String.format("%s\n", StringUtils.join(new String[] {
                taskName,
                c.getHeader(),
                "1",							                                // attributes in cluster
                "0"	                                                        	// correspondences in cluster
            }, "\t")));
        }

        w.close();
    }
}