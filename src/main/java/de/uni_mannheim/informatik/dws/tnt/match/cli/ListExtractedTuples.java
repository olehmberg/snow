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

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ListExtractedTuples extends Executable {

    @Parameter(names = "-relations", required = true)
    private String relationsLocation;

    @Parameter(names = "-results", required = true)
    private String resultLocation;

    @Parameter(names = "-entities", required = true)
    private String entitiesLocation;

    @Parameter(names = "-join")
    private String joinLocation;

    public static void main(String[] args) throws Exception {
        ListExtractedTuples app = new ListExtractedTuples();

        if(app.parseCommandLine(ListExtractedTuples.class, args)) {
            app.run();
        }
    }

    public void run() throws Exception {

        File webLoc = new File(relationsLocation);

        WebTables entities = WebTables.loadWebTables(new File(entitiesLocation), true, false, false, true);

        BufferedWriter w = new BufferedWriter(new FileWriter(new File(resultLocation)));

        long tuplesTotal = 0;
        File[] files = webLoc.listFiles();
        int fileIndex = 0;

        for(File webFile : files) {
            fileIndex++;
            if(webFile.isFile()) {
                System.out.println(String.format("Processing '%s' (%,d/%,d)", webFile.getName(), fileIndex, files.length));

                try {

                    //System.out.println(String.format("Loading relations '%s'", relationsName));
                    // load all synthesized relations
                    WebTables web = WebTables.loadWebTables(webFile, true, false, false, false);

                    System.out.println("Joining entity table");
                    // join relation tables to entity tables
                    joinEntityTablesToRelationTables(web, entities);

                    // iterate over all relation tables
                    for(Table t : web.getTables().values()) {

                        System.out.println(String.format("\tExtracting tuples for table '%s' (%,d rows)", t.getPath(), t.getRows().size()));

                        
                        if(t.getPath().contains("_rel_")) {
                            // get functional dependencies
                            //Map<Set<TableColumn>, Set<TableColumn>> fds = t.getSchema().getFunctionalDependencies();

                            // get primary key
                            Set<TableColumn> key = Q.firstOrDefault(t.getSchema().getCandidateKeys());

                            // extract tuples if there is a primary key
                            if(key!=null) {
                                System.out.println(String.format("\tPrimary key is {%s}", StringUtils.join(Q.project(key, (c)->c.getHeader()), ",")));
                                Collection<TableColumn> dependants = Q.without(t.getColumns(), key);
                                System.out.println(String.format("\tDependant attributes are {%s}", StringUtils.join(Q.project(dependants, (c)->c.getHeader()), ",")));
                                int tuples = 0;

                                // iterate over all rows
                                for(TableRow r : t.getRows()) {

                                    //for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(fds)) {

                                        // check that the determinant values are not null
                                        // boolean hasNull = false;
                                        // for(TableColumn det : fd.getFirst()) {
                                        //     if(r.get(det.getColumnIndex())==null)
                                        //     {
                                        //         hasNull = true;
                                        //         break;
                                        //     }
                                        // }
                                        
                                        // extract one tuple for each dependent attribute, if it is not null
                                        // for(TableColumn dep : fd.getSecond()) {

                                        // }
                                    // }

                                    for(TableColumn c : dependants) {
                                        if(
                                            !ContextColumns.isContextColumnIgnoreCase(c.getHeader())
                                            && c.getHeader() != null && !"null".equals(c.getHeader())
                                        ) {
                                            if(r.get(c.getColumnIndex())!=null) {
                                                String tuple = formatExtractedTuple(webFile.getName(), r, key, c);
                                                w.write(tuple);
                                                w.write("\n");
                                                tuples++;
                                            }
                                        }
                                    }

                                }

                                System.out.println(String.format("\tExtracted %,d tuples for table '%s'", tuples, t.getPath()));
                                tuplesTotal += (long)tuples;
                            } else {
                                System.out.println(String.format("\tNo primary key for table {%s}", StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")));
                            }
                        }
                    }

                    w.flush();
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

        w.close();

        System.out.println(String.format("Extracted %,d tuples from %,d directories.", tuplesTotal, files.length));

    }        

        
    protected String formatExtractedTuple(String dataset, TableRow r, Set<TableColumn> key, TableColumn value) {
        Table t = r.getTable();

        String cls = "";
        try { cls = t.getMapping().getMappedClass().getFirst(); } catch(Exception e) { e.printStackTrace(); }
        String keyAtts = StringUtils.join(Q.project(key, (c)->c.getHeader()), ",");
        String att = value.getHeader();

        StringBuilder keyValues = new StringBuilder();
        keyValues.append("{");
        for(TableColumn c : key) {
            if(keyValues.length()>1) keyValues.append(", ");
            keyValues.append(c.getHeader());
            keyValues.append(": \"");
            Object o = r.get(c.getColumnIndex());
            String v = null;
            if(o!=null) v = o.toString().replace("\n","").replace("\"",""); else v = "";
            keyValues.append(v);
            keyValues.append("\"");
        }
        keyValues.append("}");

        Object attValue = r.get(value.getColumnIndex());
        String attString = null;
        if(attValue!=null) attString = attValue.toString().replace("\n", "").replace("\"", ""); else attString = "";

        return StringUtils.join(new String[] {
            dataset,
            //t.getPath(),
            Integer.toString(t.getRows().size()),
            cls,
            keyAtts,
            att,
            keyValues.toString(),
            attString
        }, "\t");
    }

    protected void joinEntityTablesToRelationTables(WebTables web, WebTables entities) throws Exception {
		for(Table t : web.getTables().values()) {
			if(t.getPath().contains("_rel_")) {
				String entityTableName = t.getPath().replaceAll("_rel_\\d+", "");
				Integer entityTableId = entities.getTableIndices().get(entityTableName);
				if(entityTableId==null) {
					System.out.println(String.format("Could not find entity table for '%s'", t.getPath()));
				} else {
					Table entityTable = entities.getTables().get(entityTableId);
					
					TableColumn pk = Q.firstOrDefault(Q.where(entityTable.getColumns(), (c)->"PK".equalsIgnoreCase(c.getHeader())));
					TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equalsIgnoreCase(c.getHeader())));
                    
                    Map<TableColumn, TableColumn> inputColumnToOutputColumn = new HashMap<>();
                    Table joined = t.join(entityTable, Q.toList(new Pair<>(fk, pk)), Q.union(Q.without(entityTable.getColumns(), Q.toList(pk)), Q.without(t.getColumns(), Q.toList(fk))), inputColumnToOutputColumn);
                    Set<TableColumn> key = new HashSet<>();
                    for(TableColumn c : entityTable.getColumns()) {
                        if(!"pk".equalsIgnoreCase(c.getHeader())) {
                            key.add(inputColumnToOutputColumn.get(c));
                        }
                    }
                    for(TableColumn c : Q.firstOrDefault(t.getSchema().getCandidateKeys())) {
                        if(!"fk".equalsIgnoreCase(c.getHeader())) {
                            key.add(inputColumnToOutputColumn.get(c));
                        }
                    }
                    joined.getSchema().getCandidateKeys().add(key);
					joined.setTableId(t.getTableId());
					
					System.out.println(String.format("joining relation table #%d %s {%s} with entity table {%s} to {%s} / %d rows", 
							t.getTableId(),
							t.getPath(),
							StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
							StringUtils.join(Q.project(entityTable.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
							StringUtils.join(Q.project(joined.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
							t.getRows().size()));
					
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