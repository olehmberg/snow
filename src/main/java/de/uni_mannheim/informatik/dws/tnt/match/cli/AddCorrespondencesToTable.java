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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence.RecordId;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.CsvTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.LodCsvTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class AddCorrespondencesToTable extends Executable {

	@Parameter(names="-table", required=true)
	private String tableLocation;
	
	@Parameter(names="-cors", required=true)
	private String corsLocation;
	
	@Parameter(names="-ref", required=true)
	private String refLocation;
	
	public static void main(String[] args) throws IOException {
		AddCorrespondencesToTable app = new AddCorrespondencesToTable();
		
		if(app.parseCommandLine(AddCorrespondencesToTable.class, args)) {
			
			app.run();
			
		}
		
	}
	
	public void run() throws IOException {
		
		Processable<Correspondence<RecordId, RecordId>> cors = Correspondence.loadFromCsv(new File(corsLocation));
		
		CsvTableParser parser = new CsvTableParser();
		Table t = parser.parseTable(new File(tableLocation));
		
		LodCsvTableParser lodParser = new LodCsvTableParser();
		Table ref = lodParser.parseTable(new File(refLocation));
		
		Map<String, TableRow> rowsById = new HashMap<>();
		for(TableRow r : t.getRows()) {
			rowsById.put(r.getIdentifier().replace("csv", "json"), r);
		}
		
		Map<String, TableRow> refById = new HashMap<>();
		for(TableRow r : ref.getRows()) {
			refById.put(r.getIdentifier(), r);
		}
		
//		TableColumn corCol = new TableColumn(t.getColumns().size(), t);
//		corCol.setDataType(DataType.link);
//		corCol.setHeader("Correspondence");
//		t.insertColumn(corCol.getColumnIndex(), corCol);
		
		Map<TableColumn, TableColumn> newColumns = new HashMap<>();
		for(TableColumn c : ref.getColumns()) {
			TableColumn c2 = new TableColumn(t.getColumns().size(), t);
			c2.setDataType(c.getDataType());
			c2.setHeader("Correspondence " + c.getHeader());
			t.insertColumn(c2.getColumnIndex(), c2);
			newColumns.put(c2, c);
		}
		
		for(Correspondence<RecordId, RecordId> cor : cors.get()) {
			TableRow r = rowsById.get(cor.getFirstRecord().getIdentifier());
			TableRow r2 = refById.get(cor.getSecondRecord().getIdentifier());
			
			for(TableColumn c : newColumns.keySet()) {
				r.set(c.getColumnIndex(), r2.get(newColumns.get(c).getColumnIndex()));
			}
		}
		
		CSVTableWriter writer = new CSVTableWriter();
		writer.write(t, new File(tableLocation + "_with_correspondences.csv"));
	}
}
