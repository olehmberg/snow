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

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.MatchableTableRowToKBComparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.UnadjustedDeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;
import java.io.File;
import org.apache.commons.lang.StringUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EvaluateSlotFilling extends Executable {

    @Parameter(names = "-web")
    private String webLocation;

    @Parameter(names = "-entityDefinition")
    private String kbLocation;

    @Parameter(names = "-join")
    private String joinLocation;

    public static void main(String[] args) throws Exception {
        EvaluateSlotFilling app = new EvaluateSlotFilling();

        if(app.parseCommandLine(EvaluateSlotFilling.class, args)) {
            app.run();
        }
    }

    public void run() throws Exception {
        WebTables web = WebTables.loadWebTables(new File(webLocation), true, true, true, false);
        KnowledgeBase kb = KnowledgeBase.loadKnowledgeBase(new File(new File(kbLocation), "tables/"), null, null, true, true);

        // alternative implementation:
        // create instance correspondences from mapping, run duplicate-based schema matchign with strict comparators, 
        // then join schema correspondences with mapping -> the resulting similarity value is the precision per column

        int correct = 0;
        int correctStrict = 0;
        int total = 0;

        int mappedRows = 0;

        joinEntityTablesToRelationTables(web);

        for(Table t : web.getTables().values()) {
            if(t.getPath().contains("_rel_")) {

                // iterate over all rows
                for(TableRow r : t.getRows()) {

                    Pair<String, Double> rowMapping = t.getMapping().getMappedInstance(r.getRowNumber());

                    // if an instance mapping exists
                    if(rowMapping!=null) {
                        mappedRows++;

                        // iterate over all columns
                        for(TableColumn c : t.getColumns()) {

                            // if a property mapping exists
                            Pair<String, Double> columnMapping = t.getMapping().getMappedProperty(c.getColumnIndex());

                            if(columnMapping!=null) {

                                MatchableTableRow webRow = web.getRecords().getRecord(r.getIdentifier());
                                MatchableTableColumn webCol = web.getSchema().getRecord(c.getIdentifier());
                                MatchableTableRow kbRow = kb.getRecords().getRecord(rowMapping.getFirst());
                                // String propId = String.format("%s::%s", t.getMapping().getMappedClass().getFirst(), columnMapping.getFirst());
                                String propId = columnMapping.getFirst();
                                MatchableTableColumn kbCol = kb.getSchema().getRecord(propId);

                                SimilarityMeasure<?> measure = null;
                                switch (kbCol.getType()) {
                                    case numeric:
                                        measure = new UnadjustedDeviationSimilarity();
                                        break;
                                    case date:
                                        WeightedDateSimilarity dateSim = new WeightedDateSimilarity(1, 3, 5);
                                        dateSim.setYearRange(10);
                                        measure = dateSim;
                                        break;
                                    default:
                                        measure = new TokenizingJaccardSimilarity();
                                        break;
                                    }

                                // create a comparator
                                MatchableTableRowToKBComparator<?> comp = new MatchableTableRowToKBComparator<>(kbCol, measure, 0.0);

                                // compare the value to the kb 
                                // Object webValue = r.get(c.getColumnIndex());
                                // Object kbValue = kbRow.get(kbCol.getColumnIndex());

                                double similarity = comp.compare(webRow, kbRow, new Correspondence<MatchableTableColumn,Matchable>(webCol,kbCol,1.0));

                                if(similarity>0.9) {
                                    correct++;
                                }

                                comp.setStrict(true);
                                similarity = comp.compare(webRow, kbRow, new Correspondence<MatchableTableColumn,Matchable>(webCol,kbCol,1.0));
                                if(similarity>0.9) {
                                    correctStrict++;
                                }

                                total++;
                            }
                        }

                    }
                }

            }

        }

        System.out.println(String.format("%d/%d correct (%f%%) - strict: %d/%d correct (%f%%) - %d mapped rows", 
            correct, 
            total, 
            (correct/(double)total) * 100,
            correctStrict,
            total,
            (correctStrict/(double)total) * 100,
            mappedRows
            ));
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