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

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableEntity;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * 
 * Generates tokens from the blocked columns, which are then used as blocking keys for MatchableEntity
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowTokenToEntityBlockingKeyGenerator extends BlockingKeyGenerator<MatchableTableRow, MatchableTableColumn, MatchableEntity> {

	private static final long serialVersionUID = 1L;

	private Collection<Pair<String, MatchableTableColumn>> blockedColumns;
	
	public MatchableTableRowTokenToEntityBlockingKeyGenerator(Collection<Pair<String, MatchableTableColumn>> blockedColumns) {
		this.blockedColumns = blockedColumns;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator#generateBlockingKeys(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable, de.uni_mannheim.informatik.dws.winter.processing.DataIterator)
	 */
	@Override
	public void generateBlockingKeys(MatchableTableRow record,
			Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableEntity>> resultCollector) {

		// get all blocking columns that are available for record
		Collection<Pair<String, MatchableTableColumn>> existingColumns = Q.where(blockedColumns, (p)->p.getSecond().getTableId()==record.getTableId());
		
		for(Pair<String, MatchableTableColumn> p : existingColumns) {
			// this blocking key generator is designed to only use rdfs:label columns to generate blocking keys
			// so, we can simply generate one entity for every blocked column
			// create the MatchableEntity
			MatchableEntity entity = new MatchableEntity(record, Q.toList(p.getSecond()));
			
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
							resultCollector.next(new Pair<>(p.getFirst() + token, entity));
						} else {
							resultCollector.next(new Pair<>(token, entity));
						}
					}
				}
			}
		}
		
	}

}
