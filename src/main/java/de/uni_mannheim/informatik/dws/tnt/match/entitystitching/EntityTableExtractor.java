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

import de.uni_mannheim.informatik.dws.tnt.match.data.EntityTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.StitchedModel;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import java.util.*;
import java.util.Map.Entry;
import org.apache.commons.lang.StringUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EntityTableExtractor {


	public Map<Table, EntityTable> extractEntityTables(String taskName, StitchedModel model) throws Exception {
		/*****************************************************************************************
		 * CREATE ENTITY TABLES
		 ****************************************************************************************/
    	System.out.println("*** Reconstructing Entity Tables ***");
    	EntityReconstructor entityReconstructor = new EntityReconstructor();
    	Map<Table, EntityTable> entityTables = entityReconstructor.reconstruct(
    			Q.<Integer>max(
    					Q.project(model.getTables().values(), (t)->t.getTableId())
					)+1
    			, model.getTables()
    			, model.getRecords()
    			, model.getEntityGroups()
    			, taskName);
    	for(Table t : entityTables.keySet()) {
    		System.out.println(String.format("Table %s: {%s} / %d rows", t.getPath(), StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","), t.getRows().size()));
    	}

    	System.out.println("*** Deduplicating Entity Tables ***");
    	// deduplicate entities
    	entityReconstructor.deduplicateReconstructedTables(entityTables);
    	
    	for(Table t : entityTables.keySet()) {
    		System.out.println(String.format("Entity Table #%d %s: {%s}", 
    				t.getTableId(),
    				t.getPath(),
    				StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")
    				));
    		for(TableRow r : Q.take(t.getRows(), 10)) {
    			System.out.println(String.format("\t%s", r.format(20)));
    		}
    	}
    	
    	return entityTables;
	}
	
	public void filterEntityTables(Map<Table, EntityTable> entityTables, double minEntityTableSize) {
		
		Iterator<Entry<Table, EntityTable>> it = entityTables.entrySet().iterator();
		while(it.hasNext()) {
			Entry<Table, EntityTable> entry = it.next();
			Table t = entry.getKey();
			
			if(t.getRows().size() < minEntityTableSize) {
				System.out.println(String.format("[filterEntityTables] filtering out table #%d %s: {%s} / %d rows", 
						t.getTableId(),
						t.getPath(),
						StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ","),
						t.getRows().size()));
				it.remove();
			}
		}
		
	}	

}