/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * A MatchableTableRow that is identified by its source table and values. Allows for implicit deduplication during matching.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableEntity extends MatchableTableRow {

	protected Collection<MatchableTableColumn> entityKey;

	public MatchableEntity(MatchableTableRow fromRow, Collection<MatchableTableColumn> key) {
		super("");
		
		this.tableId = fromRow.tableId;
		this.rowNumber = 0;
		
		if(fromRow instanceof MatchableLodRow) {
			// kb rows already have unique identifiers, if we re-create them here based on a certain column combination, we actually create more entities than the kb contains!
			// this.id = fromRow.id;
			this.id = String.format("%s~%s~%s", 
				fromRow.id,
				StringUtils.join(Q.project(key, (c)->c.getColumnIndex()),","),
				StringUtils.join(
						Q.project(key, (c)->toString(fromRow.get(c.getColumnIndex())))
						, "/")
				);
		} else {
			// original version:
			// this.id = String.format("entity%d~%s", 
			// 	fromRow.tableId,
			// 	StringUtils.join(
			// 			Q.project(key, (c)->toString(fromRow.get(c.getColumnIndex())))
			// 			, "/")
			// 	);
			this.id = String.format("entity%d~%s~%s", 
				fromRow.tableId,
				StringUtils.join(Q.project(key, (c)->c.getColumnIndex()),","),
				StringUtils.join(
						Q.project(key, (c)->toString(fromRow.get(c.getColumnIndex())))
						, "/")
				);
		}
		this.rowLength = fromRow.rowLength;
		this.types = fromRow.types;
		this.values = fromRow.values;
		this.indices = fromRow.indices;
		this.schema = fromRow.schema;
		this.keys = fromRow.keys;

		this.entityKey = key;
	}

	protected String toString(Object cellValue) {
		if(cellValue==null) {
			return "";
		} else if(cellValue.getClass().isArray()) {
			return StringUtils.join((Object[])cellValue, "|");
		} else {
			return cellValue.toString();
		}
	}

	private static final long serialVersionUID = 1L;

	/**
	 * Returns the key that was used to created the entity
	 * 
	 * @return the entityKey
	 */
	public Collection<MatchableTableColumn> getEntityKey() {
		return entityKey;
	}
}
