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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.ListHandler;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.LodCsvTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.LodCsvTableWriter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ExtractEntitiesFromDBpedia extends Executable {

	@Parameter(names = "-class")
	private String classLocation;
	
	@Parameter(names = "-projection")
	private String projection;
	
	@Parameter(names = "-out")
	private String outputLocation;
	
	@Parameter(names = "-removeDisambiguations")
	private boolean removeDisambiguations = false;
	
	@Parameter(names = "-unique")
	private boolean makeUnique = false;
	
	@Parameter(names = "-key")
	private String key;
	
	@Parameter(names = "-notnull")
	private boolean makeNotNull = false;
	
	public static void main(String[] args) throws IOException {
		
		ExtractEntitiesFromDBpedia app = new ExtractEntitiesFromDBpedia();
		
		if(app.parseCommandLine(ExtractEntitiesFromDBpedia.class, args)) {
			
			app.run();
			
		}
		
	}
	
	private Pattern disambiguationPattern = Pattern.compile("^[^(]+\\(([^)]+)\\)");
	private Pattern bracketsPattern = Pattern.compile("\\([^)]*\\)");
	
	public void run() throws IOException {
		
		System.out.println(String.format("Parsing file %s", classLocation));
		LodCsvTableParser parser = new LodCsvTableParser();
		parser.setParseLists(true);
		parser.setConvertValues(true);
		parser.setUseRowIndexFromFile(false);
		
		Set<String> columns = Q.toSet(projection.split(","));
		
		Table t = parser.parseTable(new File(classLocation));
		
		System.out.println(String.format("%s has %d entities", classLocation, t.getRows().size()));
		
		System.out.println(String.format("Projecting columns %s", projection));
		List<TableColumn> toRemove = new LinkedList<>();
		
		for(TableColumn c : t.getColumns()) {
			if(!columns.contains(c.getHeader())) {
//			if(!columns.contains(c.getIdentifier())) {
				toRemove.add(c);
			}
		}
		
		for(TableColumn c : toRemove) {
			t.removeColumn(c);
		}
		
		// get the key
		Collection<TableColumn> keyColumns = null;
		if(key!=null) {
			Set<String> keyColumnNames = Q.toSet(key.split(","));
			keyColumns = Q.where(t.getColumns(), (c)->keyColumnNames.contains(c.getHeader()));
		} else {
			keyColumns = new ArrayList<>(t.getColumns());
		}
		
		System.out.println(String.format("New Schema is {%s}", Q.project(t.getSchema().getRecords(), new TableColumn.ColumnHeaderProjection())));
		System.out.println(String.format("\twith key {%s}", Q.project(keyColumns, new TableColumn.ColumnHeaderProjection())));
		
		LinkedList<TableRow> rowsToRemove = new LinkedList<>();
		System.out.println("Removing disambiguations");
		for(TableRow r : t.getRows()) {
			boolean hasNull = false;
			for(TableColumn c : t.getColumns()) {
				
				if(r.get(c.getColumnIndex())==null && keyColumns.contains(c)) {
					hasNull = true;
				}
				if(c.getDataType()==DataType.string) {
					if(ListHandler.isArray(r.get(c.getColumnIndex()))) {
						
						Object[] values = (Object[])r.get(c.getColumnIndex());
						
						for(int i=0; i<values.length; i++) {
							if(values[i]!=null && disambiguationPattern.matcher((String)values[i]).matches()) {
								values[i] = bracketsPattern.matcher((String)values[i]).replaceAll("").trim();
							}
						}
						
						r.set(c.getColumnIndex(), values);
						
					} else {
						String value = (String)r.get(c.getColumnIndex());
						if(value!=null && disambiguationPattern.matcher(value).matches()) {
							value = bracketsPattern.matcher(value).replaceAll("").trim();
							r.set(c.getColumnIndex(), value);
						}
					}
				}
			}
			
			// check if there's a null value
			if(hasNull && makeNotNull) {
				rowsToRemove.push(r);
			}
		}
		
		
		if(makeNotNull) {
			System.out.println(String.format("Removing %d/%d rows with null values", rowsToRemove.size(), t.getRows().size()));
			LinkedList<TableRow> rows = new LinkedList<>(t.getRows());
			// remove rows with NULLs
			for(TableRow r : Q.sort(rowsToRemove, (r1, r2)->-Integer.compare(r1.getRowNumber(), r2.getRowNumber()))) {
//				System.out.println(String.format("Removing row %d", r.getRowNumber()));
				rows.remove(r.getRowNumber());
				
//				if(!removed.get(0).equals(r.get(0))) {
//					System.out.println("that's bad");
//				}
			}
			t.setRows(new ArrayList<>(rows));
			
//			t.getRows().removeAll(rowsToRemove);
//			for(TableRow r : rowsToRemove) {
//				t.getRows().remove(r);
//			}
//			t.reorganiseRowNumbers();
		}

		if(makeUnique) {
			System.out.println(String.format("Enforcing uniqueness for %d rows", t.getRows().size()));
			
			Collection<TableColumn> cols = new LinkedList<>(keyColumns);
			
			cols.removeIf((c)->c.getHeader().equals("URI"));
			
			// enforce key
			t.deduplicate(cols);
			
			System.out.println(String.format("%d rows after enforcing uniqueness", t.getRows().size()));
		}
		
		System.out.println(String.format("Writing file %s", outputLocation));
		LodCsvTableWriter writer = new LodCsvTableWriter();
		writer.write(t, new File(outputLocation));
	}
}
