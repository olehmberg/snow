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
package de.uni_mannheim.informatik.dws.tnt.match.stitching;

import java.util.*;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.EqualHeaderComparator;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.RuleBasedMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.*;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class UnionTablesCorrespondenceGenerator {

	public Processable<Correspondence<MatchableTableColumn, Matchable>> GenerateCorrespondencesForOriginalUnionTables(WebTables web) {
        
        // block by table schema: only create correspondences between columns if the schemata are identical
        StandardBlocker<MatchableTableColumn, MatchableTableColumn, MatchableTableColumn, MatchableTableColumn> blocker = new StandardBlocker<>(
            new BlockingKeyGenerator<MatchableTableColumn, MatchableTableColumn, MatchableTableColumn>() {

                private static final long serialVersionUID = 1L;
            
                /* (non-Javadoc)
                 * @see de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator#generateBlockingKeys(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable, de.uni_mannheim.informatik.dws.winter.processing.DataIterator)
                 */
                @Override
                public void generateBlockingKeys(MatchableTableColumn record,
                        Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences,
                        DataIterator<Pair<String, MatchableTableColumn>> resultCollector) {
                    
                    Table t = web.getTables().get(record.getTableId());
                    String schema = generateSchemaString(t);

                    resultCollector.next(new Pair<String, MatchableTableColumn>(schema, record));
                    
                }
            
            }
        );

        // create schema correspondences if the column headers are equal
        LinearCombinationMatchingRule<MatchableTableColumn, MatchableTableColumn> rule = new LinearCombinationMatchingRule<>(1.0);
        try {
            EqualHeaderComparator comp = new EqualHeaderComparator();
            comp.setNullEqualsNull(true);
            rule.addComparator(comp, 1.0);

            // also check the column index (while ignoring context columns) in case two columns in one table have the same header
            Comparator<MatchableTableColumn, MatchableTableColumn> indexComparator = new Comparator<MatchableTableColumn,MatchableTableColumn>() {

				@Override
				public double compare(MatchableTableColumn record1, MatchableTableColumn record2,
						Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {
					if(getColumnIndexWithoutContext(record1, web)==getColumnIndexWithoutContext(record2, web)) {
                        return 1.0;
                    } else {
                        return 0.0;
                    }
				}
            };
            rule.addComparator(indexComparator, 1.0);
            rule.normalizeWeights();
        } catch(Exception ex) {}
		
		RuleBasedMatchingAlgorithm<MatchableTableColumn, MatchableTableColumn, MatchableTableColumn> algorithm = new RuleBasedMatchingAlgorithm<>(web.getSchema(), web.getSchema(), null, rule, blocker);
		algorithm.setTaskName("Schema Matching");
		
		algorithm.run();

		return Correspondence.toMatchable(algorithm.getResult());
	}

    public int getColumnIndexWithoutContext(MatchableTableColumn c, WebTables web) {
        Table t = web.getTables().get(c.getTableId());

        int idx = 0;
        for(TableColumn col : t.getColumns()) {
            if(col.getIdentifier().equals(c.getIdentifier())) {
                return idx;
            }
            if(!ContextColumns.isContextColumn(col)) {
                idx++;
            }
        }
        return -1;
    }
	public static String generateSchemaString(Table t) {
		StringBuilder sb = new StringBuilder();
		
		boolean first = true;
		
		for(TableColumn col : t.getColumns()) {
            if(!ContextColumns.isContextColumn(col)) {
                
                if(!first) {
                    sb.append('+');
                }
                first = false;
                
                sb.append(col.getHeader());
            }
		}
		
		return sb.toString();
	}

}