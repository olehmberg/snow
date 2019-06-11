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
package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableDataSetLoader {

	public FusibleDataSet<MatchableTableCell, MatchableTableColumn> loadCellDataSet(Collection<Table> tables, boolean deduplicate) {
		
		FusibleDataSet<MatchableTableCell, MatchableTableColumn> records = new FusibleParallelHashedDataSet<>();
		
		for(Table t : tables) {
			
			MatchableTableColumn[] schema = new MatchableTableColumn[t.getSchema().getSize()];
			
			Set<String> values = new HashSet<>();

			for(TableColumn c : t.getColumns()) {
				MatchableTableColumn col = new MatchableTableColumn(t.getTableId(), c);
				schema[c.getColumnIndex()] = col;
				records.addAttribute(col);

				for(TableRow r : t.getRows()) {
					MatchableTableRow row = new MatchableTableRow(r, t.getTableId(), schema);

					if(deduplicate) {
						Object value = row.get(c.getColumnIndex());

						if(value!=null) {
							Object[] listValues = null;
							if(value.getClass().isArray()) {
								listValues = (Object[])value;
							} else {
								listValues = new Object[] { value };
							}
							String v = StringUtils.join(listValues, "|");
							if(values.contains(v)) {
								continue;
							} else {
								values.add(v);
							}
						}
					}
				
					MatchableTableCell cell = new MatchableTableCell(row, col);

					records.add(cell);
				}
			}
			
		}
		
		
		return records;
	}

	public FusibleDataSet<MatchableTableRow, MatchableTableColumn> loadRowDataSet(Collection<Table> tables) {
		
		FusibleDataSet<MatchableTableRow, MatchableTableColumn> records = new FusibleParallelHashedDataSet<>();
		
		for(Table t : tables) {
			
			MatchableTableColumn[] schema = new MatchableTableColumn[t.getSchema().getSize()];
			
			for(TableColumn c : t.getColumns()) {
				MatchableTableColumn matchableColumn = new MatchableTableColumn(t.getTableId(), c);
				schema[c.getColumnIndex()] = matchableColumn;
				records.addAttribute(matchableColumn);
			}
			
			for(TableRow r : t.getRows()) {
				
				MatchableTableRow row = new MatchableTableRow(r, t.getTableId(), schema);
				records.add(row);
			}
			
		}
		
		
		return records;
	}
	
	public DataSet<MatchableTableRow, MatchableTableColumn> loadDataSet(Table t) {
		
		DataSet<MatchableTableRow, MatchableTableColumn> records = new ParallelHashedDataSet<>();
		
		MatchableTableColumn[] schema = new MatchableTableColumn[t.getSchema().getSize()];
		
		for(TableColumn c : t.getColumns()) {
			MatchableTableColumn matchableColumn = new MatchableTableColumn(t.getTableId(), c);
			schema[c.getColumnIndex()] = matchableColumn;
			records.addAttribute(matchableColumn);
		}
		
		for(TableRow r : t.getRows()) {
			
			MatchableTableRow row = new MatchableTableRow(r, t.getTableId(), schema);
			records.add(row);
		}
		
		return records;
	}
	
	public DataSet<MatchableTableDeterminant, MatchableTableColumn> loadCandidateKeyDataSet(Collection<Table> tables, DataSet<MatchableTableRow, MatchableTableColumn> tableDataSet) {
		
		DataSet<MatchableTableDeterminant, MatchableTableColumn> keys = new ParallelHashedDataSet<>();
		
		for(Table t : tables) {
		
	    	for(Set<TableColumn> candKey : t.getSchema().getCandidateKeys()) {
	    		
	    		Set<MatchableTableColumn> columns = new HashSet<>(); 
	    		for(TableColumn keyCol : candKey) {
	    			MatchableTableColumn mc = tableDataSet.getAttribute(keyCol.getIdentifier());
	    			
	    			columns.add(mc);
	    		}
	    		
	    		MatchableTableDeterminant k = new MatchableTableDeterminant(t.getTableId(), columns);
	    		
	    		keys.add(k);
	    	}
			
		}
		
		return keys;
	}
	
	public DataSet<MatchableTableDeterminant, MatchableTableColumn> loadSuperKeyDataSet(Collection<Table> tables, DataSet<MatchableTableRow, MatchableTableColumn> tableDataSet) {
		
		DataSet<MatchableTableDeterminant, MatchableTableColumn> keys = new ParallelHashedDataSet<>();
		
		for(Table t : tables) {
		
	    		Set<MatchableTableColumn> columns = new HashSet<>(); 
	    		for(TableColumn keyCol : t.getColumns()) {
	    			MatchableTableColumn mc = tableDataSet.getAttribute(keyCol.getIdentifier());
	    			
	    			columns.add(mc);
	    		}
	    		
	    		MatchableTableDeterminant k = new MatchableTableDeterminant(t.getTableId(), columns);
	    		
	    		keys.add(k);
	    	
		}
		
		return keys;
	}
	
	/***
	 * Creates a dataset of {@link MatchableTableDeterminant}s that contains all Foreign Keys of the tables
	 * @param tables	the tables
	 * @param tableDataSet	the dataset that contains the {@link MatchableTableColumn}s
	 * @return
	 */
	public DataSet<MatchableTableDeterminant, MatchableTableColumn> loadForeignKeyDataSet(Collection<Table> tables, DataSet<MatchableTableRow, MatchableTableColumn> tableDataSet) {
		
		DataSet<MatchableTableDeterminant, MatchableTableColumn> keys = new ParallelHashedDataSet<>();
		
		for(Table t : tables) {
		
	    		Set<MatchableTableColumn> columns = new HashSet<>(); 
	    		for(TableColumn keyCol : t.getColumns()) {
	    			
	    			if(keyCol.getHeader().equals("FK")) {
		    			MatchableTableColumn mc = tableDataSet.getAttribute(keyCol.getIdentifier());
		    			
		    			columns.add(mc);
	    			}
	    		}
	    		
	    		if(columns.size()>0) {
		    		MatchableTableDeterminant k = new MatchableTableDeterminant(t.getTableId(), columns);
		    		
		    		keys.add(k);
	    		}
	    	
		}
		
		return keys;
	}

	public DataSet<MatchableTableDeterminant, MatchableTableColumn> loadFunctionalDependencyDataSet(Collection<Table> tables, DataSet<MatchableTableRow, MatchableTableColumn> tableDataSet) {
		
		DataSet<MatchableTableDeterminant, MatchableTableColumn> fds = new ParallelHashedDataSet<>();
		
		for(Table t : tables) {
		
	    	for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(t.getSchema().getFunctionalDependencies())) {
	    		
	    		Set<MatchableTableColumn> det = new HashSet<>(); 
	    		for(TableColumn c : fd.getFirst()) {
	    			MatchableTableColumn mc = tableDataSet.getAttribute(c.getIdentifier());
	    			det.add(mc);
	    		}
				
				Set<MatchableTableColumn> dep = new HashSet<>(); 
	    		for(TableColumn c : fd.getSecond()) {
	    			MatchableTableColumn mc = tableDataSet.getAttribute(c.getIdentifier());
	    			dep.add(mc);
	    		}

	    		MatchableTableDeterminant k = new MatchableTableDeterminant(t.getTableId(), det, dep);
	    		
	    		fds.add(k);
	    	}
			
		}
		
		return fds;
	}

	/**
	 * Loads a dataset of all functional dependencies where every dependant is the closure of its determinant
	 */
	public DataSet<MatchableTableDeterminant, MatchableTableColumn> loadFunctionalClosureDataSet(Collection<Table> tables, DataSet<MatchableTableRow, MatchableTableColumn> tableDataSet) {
		
		DataSet<MatchableTableDeterminant, MatchableTableColumn> fds = new ParallelHashedDataSet<>();
		
		for(Table t : tables) {
		
	    	for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(t.getSchema().getFunctionalDependencies())) {
	    		
	    		Set<MatchableTableColumn> det = new HashSet<>(); 
	    		for(TableColumn c : fd.getFirst()) {
	    			MatchableTableColumn mc = tableDataSet.getAttribute(c.getIdentifier());
	    			det.add(mc);
	    		}
				
				Set<MatchableTableColumn> dep = new HashSet<>(); 
	    		for(TableColumn c : FunctionalDependencyUtils.closure(fd.getFirst(), t.getSchema().getFunctionalDependencies())) {
	    			MatchableTableColumn mc = tableDataSet.getAttribute(c.getIdentifier());
	    			dep.add(mc);
	    		}

	    		MatchableTableDeterminant k = new MatchableTableDeterminant(t.getTableId(), det, dep);
	    		
	    		fds.add(k);
	    	}
			
		}
		
		return fds;
	}
}
