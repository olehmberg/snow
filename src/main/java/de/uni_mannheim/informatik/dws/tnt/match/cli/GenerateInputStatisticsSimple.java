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

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.snow.SnowPerformance;
import de.uni_mannheim.informatik.dws.snow.SnowRuntime;
import de.uni_mannheim.informatik.dws.snow.SnowStatisticsWriter;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.webtables.preprocessing.TableNumberingExtractor;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.parallel.Parallel;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.preprocessing.TableDisambiguationExtractor;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class GenerateInputStatisticsSimple extends Executable {

	
	@Parameter(names = "-web", required=true)
	private String webLocation;
	
	private boolean serialise = false;
	

	public static void main(String[] args) throws Exception {
		GenerateInputStatisticsSimple app = new GenerateInputStatisticsSimple();
		
		if(app.parseCommandLine(GenerateInputStatisticsSimple.class, args)) {
			app.run();
		}
	}


	private void sayHello() {

		
		System.out.println("\t\t   .      .                         .      .      ");
		System.out.println("\t\t   _\\/  \\/_                         _\\/  \\/_      ");
		System.out.println("\t\t    _\\/\\/_                           _\\/\\/_       ");
		System.out.println("\t\t_\\_\\_\\/\\/_/_/_    SNoW v1.0       _\\_\\_\\/\\/_/_/_  ");
		System.out.println("\t\t/ /_/\\/\\_\\ \\                      / /_/\\/\\_\\ \\    ");
		System.out.println("\t\t    _/\\/\\_                           _/\\/\\_       ");
		System.out.println("\t\t    /\\  /\\                           /\\  /\\       ");
		System.out.println("\t\t   '      '                         '      '      ");

	}

	public void run() throws Exception {
		Parallel.setReportIfStuck(false);

		sayHello();

		System.out.println();

		int firstTableId = 0;
		// if(entityDefinitionLocation!=null) {
		// 	// make sure the web table ids and the entity definition ids do not overlap
		// 	firstTableId = new File(entityDefinitionLocation, "tables/").list().length;
		// }
		
		File webLocationFile = new File(webLocation);
		File logDirectory = webLocationFile.getParentFile();
		// File csvLocationFile = new File(csvLocation);
		// csvLocationFile.mkdirs();
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
		 * TABLE PRE-PROCESSING
		 ****************************************************************************************/
		rt = runtime.startMeasurement("Web table preprocessing");
		Map<Table, Integer> tableColumnsBeforePreprocessing = new HashMap<>();
		for(Table t : web.getTables().values()) {
			tableColumnsBeforePreprocessing.put(t, t.getColumns().size());
		}
		TableDisambiguationExtractor dis = new TableDisambiguationExtractor();
		//TODO add square brackets to disambiguation extraction
		Map<Integer, Map<Integer, TableColumn>> tableToColumnToDisambiguation = dis.extractDisambiguations(web.getTables().values());
		TableNumberingExtractor num = new TableNumberingExtractor();
		Map<Integer, Map<Integer, TableColumn>> tableToColumnToNumbering = num.extractNumbering(web.getTables().values());

		// convert values into their data types after extracting the additional columns
		// also infer the data type for the new columns
		for(Table t : web.getTables().values()) {
			t.inferSchemaAndConvertValues();
		}

		web.reloadSchema();
		web.reloadRecords();
		rt.endMeasurement();
		// web.printSchemata(true);
		
		web.printSchemata(true);

		SnowStatisticsWriter.writeInputStatistics(logDirectory, taskName, web.getTables().values(), originalWebTablesCount, originalWebTableColumnCountByTable, tableColumnsBeforePreprocessing);
		

		System.err.println("Done.");
	}

}
