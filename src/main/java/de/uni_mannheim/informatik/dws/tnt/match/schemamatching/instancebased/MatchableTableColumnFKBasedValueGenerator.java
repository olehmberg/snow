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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.instancebased;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.SchemaValueGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import java.util.Set;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableColumnFKBasedValueGenerator extends SchemaValueGenerator<MatchableTableRow, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;

    private Set<MatchableTableColumn> colsInCors;

    public MatchableTableColumnFKBasedValueGenerator(Set<MatchableTableColumn> colsInCors) {
        this.colsInCors = colsInCors;
    }

	@Override
	public void generateBlockingKeys(MatchableTableRow record,
			Processable<Correspondence<MatchableValue, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableColumn>> resultCollector) {

        // get the FK value
        String fkValue = null;
        for(MatchableTableColumn c : record.getSchema()) {
            if("FK".equals(c.getHeader())) {
                fkValue = record.get(c.getColumnIndex()).toString();
            }
        }

		for(MatchableTableColumn c : record.getSchema()) {
			
			if(record.hasValue(c) && colsInCors.contains(c)) {
				
				Object value = record.get(c.getColumnIndex());
				
				if(value!=null) {
                    String prefixedValue = String.format("%s%s%s", fkValue, c.getType().toString(), value.toString());
					resultCollector.next(new Pair<String, MatchableTableColumn>(prefixedValue, c));
				}
				
			}
			
		}
		
	}	
		
}