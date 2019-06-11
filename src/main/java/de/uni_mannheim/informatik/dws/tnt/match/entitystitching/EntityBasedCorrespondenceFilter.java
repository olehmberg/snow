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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import de.uni_mannheim.informatik.dws.tnt.match.data.EntityTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EntityBasedCorrespondenceFilter {

	public Processable<Correspondence<MatchableTableColumn, Matchable>> filterCorrespondences(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			Collection<EntityTable> entityGroups) {
		
		Map<Integer, Integer> tableToEntityId = new HashMap<>();
		int entityId = 0;
		for(EntityTable group : entityGroups) {
			if(!group.isNonEntity()) {
				Set<Integer> tables = new HashSet<>();
				for(Collection<MatchableTableColumn> cluster : group.getAttributes().values()) {
					for(MatchableTableColumn col : cluster) {
						tableToEntityId.put(col.getTableId(), entityId);
						tables.add(col.getTableId());
					}
				}
				System.out.println(String.format("[EntityBasedCorrespondenceFilter] Entity Type [%d] '%s' table cluster: %s",
					entityId,
					group.getEntityName(),
					StringUtils.join(tables, ",")
				));
				entityId++;
			}
		}
		
		// we have to check the entity-relatedness on table level
		// if we check on column level, a correspondence between two columns which are not entity-related will be kept, even if they are from unrelated tables 
		
		return schemaCorrespondences.where(
				(c)-> {
					Integer id1 = tableToEntityId.get(c.getFirstRecord().getDataSourceIdentifier());
					Integer id2 = tableToEntityId.get(c.getSecondRecord().getDataSourceIdentifier());
					
					// keep correspondences between the same entity or between two non-entity columns
					return id1!=null && id2!=null && id1.intValue()==id2.intValue();
				});
		
	}
	
}
