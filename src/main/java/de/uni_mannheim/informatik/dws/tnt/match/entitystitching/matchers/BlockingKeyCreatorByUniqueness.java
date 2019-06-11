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
package de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * 
 * Creates a blocking key from a set of columns based on the uniqueness of column values
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class BlockingKeyCreatorByUniqueness {

	public Map<TableColumn, Double> calculateColumnUniqueness(Table t) {
		Map<TableColumn, Double> uniq = new HashMap<>();
		for(TableColumn c : t.getColumns()) {
			Set<Object> domain = new HashSet<>();
			for(TableRow r : t.getRows()) {
				domain.add(r.get(c.getColumnIndex()));
			}
			double uniqueness = domain.size() / (double)t.getSize();
			System.out.println(String.format("[BlockingKeyCreator] Table '%s' column '%s' domain size: %d (uniqueness: %f)", t.getPath(), c.getHeader(), domain.size(), uniqueness));
			uniq.put(c, uniqueness);
		}
		
		return uniq;
	}
	
	public Set<TableColumn> createBlockingKey(Collection<TableColumn> columns, Map<TableColumn, Double> columnUniqueness) {
		Set<TableColumn> blockingKeys = new HashSet<>();
		
		// remove columns from blocking key which have a too small domain
		double maxUnique = 0.0;
		TableColumn maxUniqueCol = null;
		for(TableColumn c : columns) {
			double uniqueness = columnUniqueness.get(c);
			
			if(uniqueness>=0.1) {
				blockingKeys.add(c);
			}
			
			if(uniqueness>maxUnique) {
				maxUnique = uniqueness;
				maxUniqueCol=c;
			}
		}
		
		if(blockingKeys.size()==0) {
			if(maxUniqueCol!=null) {
				blockingKeys.add(maxUniqueCol);
			} else {
				blockingKeys.addAll(columns);
			}
		}
		
		return blockingKeys;
	}
	
}
