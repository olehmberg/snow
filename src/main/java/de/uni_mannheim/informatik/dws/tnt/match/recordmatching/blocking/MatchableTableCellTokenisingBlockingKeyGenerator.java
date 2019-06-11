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

import java.util.Collection;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableCell;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * Generates the values of the blocked columns as blocking keys
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableCellTokenisingBlockingKeyGenerator extends BlockingKeyGenerator<MatchableTableCell, MatchableTableColumn, MatchableTableCell> {

	private static final long serialVersionUID = 1L;
	
	private boolean prefixWithCorrespondence = false;
	private boolean prefixWithTableId = false;

	public MatchableTableCellTokenisingBlockingKeyGenerator() {
	}
	
	public MatchableTableCellTokenisingBlockingKeyGenerator(boolean prefixWithCorrespondence, boolean prefixWithTableId) {
		this.prefixWithCorrespondence = prefixWithCorrespondence;
		this.prefixWithTableId = prefixWithTableId;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator#generateBlockingKeys(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable, de.uni_mannheim.informatik.dws.winter.processing.DataIterator)
	 */
	@Override
	public void generateBlockingKeys(MatchableTableCell record,
			Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableCell>> resultCollector) {

			if(prefixWithCorrespondence && correspondences!=null && correspondences.size()>0) {
				for(Correspondence<MatchableTableColumn, Matchable> cor : correspondences.get()) {
					if(cor.getFirstRecord().equals(record.getColumn()) || cor.getSecondRecord().equals(record.getColumn())) {
						createBlockingKey(record, String.format("cls%d~", cor.getSecondRecord().getTableId()), resultCollector);
						// createBlockingKey(record, String.format("cls%d~", cor.getFirstRecord().getTableId()), resultCollector);
					}
				}
			} else if(prefixWithTableId) {
				createBlockingKey(record, String.format("cls%d~", record.getDataSourceIdentifier()), resultCollector);
			} else {
				createBlockingKey(record, "", resultCollector);
			}
		
	}

	protected void createBlockingKey(MatchableTableCell record, String prefix, DataIterator<Pair<String, MatchableTableCell>> resultCollector) {
		Object value = record.getValue();
			
		if(value!=null) {                
			Object[] listValues = null;
			if(value.getClass().isArray()) {
				listValues = (Object[])value;
			} else {
				listValues = new Object[] { value };
			}
			
			for(Object o : listValues) {
				String s = o.toString();
				
				s = s.toLowerCase();
				s = s.replaceAll("[.!?]+", " ");
				s = s.replaceAll("[^a-z0-9\\s]", "");

				for(String token : WebTablesStringNormalizer.tokenise(s, true)) {
					resultCollector.next(new Pair<>(prefix + token, record));
				}
			}
		}
	}
}
