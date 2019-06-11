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
package de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableCellBlockingKeyGenerator
		extends BlockingKeyGenerator<MatchableTableRow, MatchableTableColumn, MatchableTableRow> {

	private static final long serialVersionUID = 1L;

	private Map<Integer, String> tables = null;

	public TableCellBlockingKeyGenerator() {
	}

	public TableCellBlockingKeyGenerator(Map<Integer, String> tables) {
		this.tables = tables;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator#generateBlockingKeys(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable, de.uni_mannheim.informatik.dws.winter.processing.DataIterator)
	 */
	@Override
	public void generateBlockingKeys(MatchableTableRow record,
			Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableRow>> resultCollector) {
		
		// get all values
		Object[] values = record.getValues();
		
		if(correspondences!=null) {
			Set<MatchableTableColumn> blockedColumns = new HashSet<>();
			for(Correspondence<MatchableTableColumn, Matchable> cor : correspondences.get()) {
				MatchableTableColumn col;
				
				if(record.getTableId() == cor.getFirstRecord().getTableId()) {
					col = cor.getFirstRecord();
				} else if(record.getTableId() == cor.getSecondRecord().getTableId()) {
					col = cor.getSecondRecord();
				} else {
					continue;
				}

				if(col.getHeader().equals(KnowledgeBase.RDFS_LABEL)) {
					blockedColumns.add(col);
					createBlockingKey(record.get(col.getColumnIndex()), record, cor.getSecondRecord(), resultCollector);
				}

			}
			
		} 
		// else {
		// 	for(int i = 0; i < values.length; i++) {
		// 		createBlockingKey(values[i], record, resultCollector);
		// 	}
		// }
		
	}

	protected void createBlockingKey(Object v, MatchableTableRow record, MatchableTableColumn rhsColumn, DataIterator<Pair<String, MatchableTableRow>> resultCollector) {
		if(v!=null) {
			String value = v.toString();
			value = rhsColumn.getIdentifier() + value;
			if(tables!=null) {
				String tblName = tables.get(record.getTableId());
				value = tblName + value;
			}			resultCollector.next(new Pair<String, MatchableTableRow>(value, record));

		}
	}

	protected void createBlockingKey(Object v, MatchableTableRow record, DataIterator<Pair<String, MatchableTableRow>> resultCollector) {
		if(v==null) {
			v = "null";
		}
		
		resultCollector.next(new Pair<String, MatchableTableRow>(v.toString(), record));
	}

}

