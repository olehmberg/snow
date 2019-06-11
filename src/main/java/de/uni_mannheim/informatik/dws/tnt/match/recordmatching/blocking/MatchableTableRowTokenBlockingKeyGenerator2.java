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

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * 
 * Generates tokens from the blocked columns, which are then used as blocking keys
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowTokenBlockingKeyGenerator2 extends BlockingKeyGenerator<MatchableTableRow, MatchableTableColumn, MatchableTableRow> {

	private static final long serialVersionUID = 1L;

	private Collection<Pair<String, MatchableTableColumn>> blockedColumns;
	
	public MatchableTableRowTokenBlockingKeyGenerator2(Collection<Pair<String, MatchableTableColumn>> blockedColumns) {
		this.blockedColumns = blockedColumns;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator#generateBlockingKeys(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable, de.uni_mannheim.informatik.dws.winter.processing.DataIterator)
	 */
	@Override
	public void generateBlockingKeys(MatchableTableRow record,
			Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableRow>> resultCollector) {

		for(Pair<String, MatchableTableColumn> p : blockedColumns) {
			if(record.getTableId()==p.getSecond().getTableId()) {
				MatchableTableColumn col = p.getSecond();
				Object value = record.get(col.getColumnIndex());
				
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
							if(p.getFirst()!=null) {
								resultCollector.next(new Pair<String, MatchableTableRow>(p.getFirst() + token, record));
							} else {
								resultCollector.next(new Pair<String, MatchableTableRow>(token, record));
							}
						}
					}
				}
			}
		}
		
	}

}
