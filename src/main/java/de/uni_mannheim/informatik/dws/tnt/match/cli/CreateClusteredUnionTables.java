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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import com.beust.jcommander.Parameter;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableDataSetLoader;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.UnionTables;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CreateClusteredUnionTables extends Executable {

	@Parameter(names = "-web", required=true)
	private String webLocation;

	@Parameter(names = "-results", required=true)
	private String resultLocation;
	
	@Parameter(names = "-correspondences")
	private String correspondenceLocation;

	@Parameter(names = "-serialise")
	private boolean serialise;

	@Parameter(names = "-log")
	private String logLocation;

	@Parameter(names = "-urlStatistics")
	private String urlStatisticsLocation;
	
	public static void main(String[] args) throws URISyntaxException, IOException {
		CreateClusteredUnionTables app = new CreateClusteredUnionTables();
		
		if(app.parseCommandLine(CreateClusteredUnionTables.class, args)) {
			app.run();
		}
	}
	
	public void run() throws URISyntaxException, IOException {
	
		System.err.println("Loading Web Tables");
		// load web tables
		WebTables web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);
		
		UnionTables union = new UnionTables();

		System.err.println("Clustering tables by URL");
		Collection<Collection<Table>> clustering = union.getTableClustersFromURLs(web.getTables().values(), 0.1, true);

		Collection<Table> unionTables = new LinkedList<>();
		Map<Collection<Table>, Collection<Table>> clusterToUnionTables = new HashMap<>();
		int clusterId = 0;
		for(Collection<Table> cluster : clustering) {
			union = new UnionTables(); // reset the union tables class to prevent that tables with the same schema from different clusters are merged
			System.err.println(String.format("Processing cluster %d (%d tables)", clusterId++, cluster.size()));

			System.err.println("Creating Context Attributes");
			Map<String, Integer> contextAttributes = union.generateContextAttributes(cluster, true, false);
			
			System.err.println("Creating Union Tables");
			Collection<Table> unionTablesForCluster = union.create(new ArrayList<>(cluster), contextAttributes);
			unionTables.addAll(unionTablesForCluster);
			clusterToUnionTables.put(cluster, unionTablesForCluster);
		}
		
		File outFile = new File(resultLocation);
		outFile.mkdirs();
		
		System.err.println("Deduplicating Union Tables");
		for(Table t : unionTables) {
			t.deduplicate(t.getColumns());
		}

		System.err.println("Writing Union Tables");
		JsonTableWriter w = new JsonTableWriter();
		
		int tableId = 0;
		int unionColumns = 0, unionColumnsNoContext = 0, unionRows = 0;

		for(Table t : Q.sort(unionTables, (t1,t2)->-Integer.compare(t1.getSize(), t2.getSize()))) {
			t.setTableId(tableId++);
			t.setPath(Integer.toString(t.getTableId()) + ".json");
			w.write(t, new File(outFile, t.getPath()));

			unionColumns += t.getColumns().size();
			unionColumnsNoContext += Q.where(t.getColumns(), new ContextColumns.IsNoContextColumnPredicate()).size();
			unionRows += t.getSize();
		}
		
		if(correspondenceLocation!=null) {

			System.err.println("Creating Correspondences for Context Columns");

			// important: create correspondences after writing the tables, so they will use the updated table ids

			N2NGoldStandard cors = new N2NGoldStandard();

			Map<String, Set<String>> staticCorrespondences = new HashMap<>();

			for(Collection<Table> cluster : clustering) {

				List<Table> tbls = new ArrayList<>(clusterToUnionTables.get(cluster));

				System.out.println(String.format("\tCluster {%s} corresponding to union tables {%s}",
					StringUtils.join(Q.project(cluster, (t)->t.getTableId()), ","),
					StringUtils.join(Q.project(tbls, (t)->t.getTableId()), ",")
				));

				WebTableDataSetLoader loader = new WebTableDataSetLoader();
				DataSet<MatchableTableRow, MatchableTableColumn> data = loader.loadRowDataSet(tbls);

				Map<String, Set<String>> t2tCorrespondences = new HashMap<>();
				Map<String, Distribution<String>> valueDistributions = new HashMap<>();

				for(int i = 0; i < tbls.size(); i++) {
					Table t1 = tbls.get(i);
					Collection<TableColumn> t1Ctx = Q.where(t1.getColumns(), new ContextColumns.IsContextColumnPredicate());

					for(TableColumn c1 : t1Ctx) {
						if(ContextColumns.isStaticContextColumn(c1.getHeader())) {
							Set<String> cols = MapUtils.get(staticCorrespondences, c1.getHeader(), new HashSet<>());
							cols.add(c1.getIdentifier());
						} else {
							Set<String> cols = MapUtils.get(t2tCorrespondences, c1.getHeader(), new HashSet<>());
							cols.add(c1.getIdentifier());

							Distribution<String> values = MapUtils.get(valueDistributions, c1.getHeader(), new Distribution<String>());
							for(TableRow r : t1.getRows()) {
								values.add((String)r.get(c1.getColumnIndex()));
							}
						}
					}
				}

				for(Pair<String, Set<String>> p : Pair.fromMap(t2tCorrespondences)) {
					System.out.println(String.format("\t\t%s\t%s",
						p.getFirst(),
						StringUtils.join(p.getSecond(), ",")
					));
					Distribution<String> values = MapUtils.get(valueDistributions, p.getFirst(), new Distribution<String>());
					cors.getCorrespondenceClusters().put(p.getSecond(), String.format("%s (%s)",
						p.getFirst(),
						StringUtils.join(Q.take(values.getElements(), 5), ",")
					));
				}
			}

			for(Pair<String, Set<String>> p : Pair.fromMap(staticCorrespondences)) {
				System.out.println(String.format("\t\t%s\t%s",
					p.getFirst(),
					StringUtils.join(p.getSecond(), ",")
				));
				cors.getCorrespondenceClusters().put(p.getSecond(), p.getFirst());
			}

			System.err.println("Writing Correspondences");
			cors.writeToTSV(new File(correspondenceLocation));
		}

		if(logLocation!=null) {
			BufferedWriter logWriter = new BufferedWriter(new FileWriter(new File(logLocation), true));

			logWriter.write(String.format("%s\n", StringUtils.join(new String[] {
				new File(webLocation).getName(),				// host
				Integer.toString(web.getTables().size()),		// original web tables
				Integer.toString(web.getSchema().size()),		// original columns
				Integer.toString(web.getRecords().size()),		// original rows
				Integer.toString(unionTables.size()),			// union tables
				Integer.toString(unionColumns),					// union table columns
				Integer.toString(unionColumnsNoContext),		// union table columns without context columns
				Integer.toString(unionRows)						// union table rows
			}, "\t")));

			logWriter.close();
		}

		if(urlStatisticsLocation!=null) {
			BufferedWriter statisticsWriter = new BufferedWriter(new FileWriter(new File(urlStatisticsLocation)));

			Map<String, Collection<Table>> groupedByURL = Q.group(web.getTables().values(), (t)->t.getContext().getUrl());

			for(String url : groupedByURL.keySet()) {
				Collection<Table> tbls = groupedByURL.get(url);
				statisticsWriter.write(String.format("%s\n", StringUtils.join(new String[] {
					url,
					Integer.toString(tbls.size())
				}, "\t")));
			}

			statisticsWriter.close();
		}

		System.err.println("Done.");
	}
}
