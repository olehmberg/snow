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
package de.uni_mannheim.informatik.dws.tnt.match.recordmatching;

import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * Compares a Web Table row to another Web Table row
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowComparator implements Comparator<MatchableTableRow, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;
	
	private MatchableTableColumn column;
	
	public MatchableTableRowComparator(MatchableTableColumn columnToCompare) {
		column = columnToCompare;
	}

	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator#compare(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@Override
	public double compare(MatchableTableRow record1, MatchableTableRow record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {

		Object value1 = record1.get(column.getColumnIndex());
		Object value2 = record2.get(column.getColumnIndex());
		
		if(Q.equals(value1, value2, false)) {
			// the values are equal
			return 1.0;
		} else {
			// the values are not equal
			if(value1==null && value2==null) {
				// because both are null
				return 0.0;
			} else {
				
				// if only one value is null, look for it in the context attributes
				if(value1==null) {
					// because value1 is null
					
					Set<Object> contextValues1 = new HashSet<>();
					for(MatchableTableColumn col : record1.getSchema()) {
						if(ContextColumns.isContextColumn(col)) {
							contextValues1.add(record1.get(col.getColumnIndex()));
						}
					}
					
					return contextValues1.contains(value2) ? 1.0 : 0.0;
				} else if(value2==null) {
					// because value2 is null
					
					Set<Object> contextValues2 = new HashSet<>();
					for(MatchableTableColumn col : record2.getSchema()) {
						if(ContextColumns.isContextColumn(col)) {
							contextValues2.add(record2.get(col.getColumnIndex()));
						}
					}
					
					return contextValues2.contains(value1) ? 1.0 : 0.0;
				} else {
					// the values are just not equal
					return 0.0;
				}
			}
		}
	}

}
