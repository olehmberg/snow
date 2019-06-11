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
package de.uni_mannheim.informatik.dws.tnt.match.entitystitching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * Merges duplicate rows in a table based on correspondences
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EntityDeduplication {
	
	private boolean removeKeyViolations = true;
	
	public EntityDeduplication(boolean removeKeyViolations) {
		this.removeKeyViolations = removeKeyViolations;
	}

	/**
	 * Merges the values of duplicate records by replacing NULLs with values.
	 * @param t
	 * @param duplicates
	 */
	public void mergeDuplicates(Table t, Processable<Correspondence<MatchableTableRow, Matchable>> duplicates) {
		
		List<TableRow> toRemove = new LinkedList<>();
		
		// first cluster the correspondences to get all groups of corresponding records
		ConnectedComponentClusterer<MatchableTableRow> clusterer = new ConnectedComponentClusterer<>();
		for(Correspondence<MatchableTableRow, Matchable> cor : duplicates.get()) {
			clusterer.addEdge(new Triple<MatchableTableRow, MatchableTableRow, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		Map<Collection<MatchableTableRow>, MatchableTableRow> clusters = clusterer.createResult();
		
		System.out.println(String.format("Found %d duplicate clusters", clusters.size()));
		
		int duplicateCount = 0;
		
		// check each cluster
		for(Collection<MatchableTableRow> clu : clusters.keySet()) {
			
			duplicateCount += clu.size()-1;
			
			List<MatchableTableRow> byRow = Q.sort(clu, new MatchableTableRow.RowNumberComparator());
			
			TableRow row1 = t.get(Q.firstOrDefault(byRow).getRowNumber());
			
			// go through all records in the cluster
			for(int idx = 1; idx < byRow.size(); idx++) {
				TableRow row2 = t.get(byRow.get(idx).getRowNumber());
				
				boolean hasConflict = false;
				
				Object[] values = row1.getValueArray();
				
				// and check the values for conflicts
				for(TableColumn c : t.getColumns()) {
					
					if(!ContextColumns.isContextColumn(c) && !"PK".equals(c.getHeader())) {
					
						Object value1 = row1.get(c.getColumnIndex());
						Object value2 = row2.get(c.getColumnIndex());
						
						if(value1!=null && value2 != null && !value1.equals(value2)) {
							// report conflicts
							System.err.println(String.format("Duplicate with non-matching values!\n\t%s\n\t%s", row1.format(20), row2.format(20)));
							hasConflict = true;
							break;
						} else if(value1==null && value2!=null){
							// replace NULLs with values
							values[c.getColumnIndex()] =  value2;
						}
					
					}
				}
				
				if(!hasConflict) {
					// if there were no conflicts, use the combined values (NULLs replaced)
					row1.set(values);
					toRemove.add(row2);
					row1.addProvenanceForRow(row2);
				} else if(removeKeyViolations) {
					toRemove.add(row2);
				}
				
			}
		}
		
		// remove the duplicate records
		toRemove  = Q.sort(toRemove, new Comparator<TableRow>() {

			@Override
			public int compare(TableRow o1, TableRow o2) {
				return -Integer.compare(o1.getRowNumber(), o2.getRowNumber());
			}
		});
		
		System.out.println(String.format("Removing %d/%d duplicate rows (removed/duplicates)", toRemove.size(),duplicateCount));
		for(TableRow r : toRemove) {
			t.getRows().remove(r.getRowNumber());
		}
		
	}
	
	/**
	 * Remove all records that have a NULL value in one of the candidate keys.
	 * 
	 * @param t The Table
	 */
	public void enforceCandidateKeys(Table t) {
		
		List<TableRow> rows = new ArrayList<>(t.getRows());
		
		// list all records with NULLs in any candidate key (rows)
		Iterator<TableRow> rowIt = rows.iterator();
		while(rowIt.hasNext()) {
			
			TableRow r = rowIt.next();
			
			for(Collection<TableColumn> key : t.getSchema().getCandidateKeys()) {
				
				boolean hasNull = false;
				for(TableColumn c : key) {
					
					Object value = r.get(c.getColumnIndex());
					if(value==null || value.toString().trim().isEmpty()) {
						hasNull=true;
						break;
					}
					
				}
				
				if(!hasNull) {
					rowIt.remove();
				}
			}
			
		}
		
		// sort the validating records by row number descending
		rows = Q.sort(rows, new Comparator<TableRow>() {

			@Override
			public int compare(TableRow o1, TableRow o2) {
				return -Integer.compare(o1.getRowNumber(), o2.getRowNumber());
			}
		});
		
		// remove the validating records from the table
		// (in descending order, so the removal of a record does not affect the row indices of other records)
		for(TableRow r : rows) {
			t.getRows().remove(r.getRowNumber());
		}
		
		// re-assign new row numbers to the remaining records in t
		t.reorganiseRowNumbers();
		
	}
}
