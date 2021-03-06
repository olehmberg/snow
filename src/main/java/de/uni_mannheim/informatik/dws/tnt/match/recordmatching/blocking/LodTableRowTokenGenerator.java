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

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class LodTableRowTokenGenerator
		extends BlockingKeyGenerator<MatchableTableRow, MatchableValue, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator#generateBlockingKeys(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable, de.uni_mannheim.informatik.dws.winter.processing.DataIterator)
	 */
	@Override
	public void generateBlockingKeys(MatchableTableRow record,
			Processable<Correspondence<MatchableValue, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableColumn>> resultCollector) {
		MatchableTableRow row = record;
		
		for(MatchableTableColumn col : row.getSchema()) {
			int colIdx = col.getColumnIndex();
			
			if(colIdx>0 && row.get(colIdx)!=null) {
				
				Object value = row.get(colIdx);
				
				Object[] values = null;
				
				if(value.getClass().isArray()) {
					values = (Object[])value;
				} else {
					values = new Object[] { value };
				}
				
				for(Object o : values) {
				
					String val = o.toString().toLowerCase();
						
					if(!val.isEmpty()) {
						
						val = WebTablesStringNormalizer.normaliseValue(val, true);
						
						for(String v : new HashSet<>(WebTablesStringNormalizer.tokenise(val, true))) {
							resultCollector.next(new Pair<>(col.getType().toString() + v, col));
						}
					}
				
				}

			}
		}
	}

}
