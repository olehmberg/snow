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
package de.uni_mannheim.informatik.dws.tnt.match.recordmatching;

import java.util.Collection;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * Compares a Web Table row to another Web Table row
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ClusteredMatchableTableRowComparator<DataType> implements Comparator<MatchableTableRow, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;
	
	private Collection<MatchableTableColumn> clusterToCompare;
	private SimilarityMeasure<DataType> similarityMeasure;
	
	public ClusteredMatchableTableRowComparator(Collection<MatchableTableColumn> clusterToCompare, SimilarityMeasure<DataType> similarityMeasure) {
		this.clusterToCompare = clusterToCompare;
		this.similarityMeasure = similarityMeasure;
	}
	
	@Override
	public MatchableTableColumn getFirstSchemaElement(MatchableTableRow record) {
		MatchableTableColumn result = Q.firstOrDefault(Q.where(clusterToCompare, (c)->c.getTableId()==record.getTableId()));
		return result;
	}

	@Override
	public MatchableTableColumn getSecondSchemaElement(MatchableTableRow record) {
		return Q.firstOrDefault(Q.where(clusterToCompare, (c)->c.getTableId()==record.getTableId()));
	}

	@Override
	public String toString() {
		return String.format("%s(%s)", 
			similarityMeasure.toString(), 
			StringUtils.join(Q.project(clusterToCompare, new MatchableTableColumn.ColumnHeaderProjection()), ",")
			);
	}

//	private static final Pattern numberingPattern = Pattern.compile("\\d*\\. (.+)");
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator#compare(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public double compare(MatchableTableRow record1, MatchableTableRow record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {

		MatchableTableColumn column1 = null;
		MatchableTableColumn column2 = null;

		if(record1.getTableId()==record2.getTableId()) {
			return 0.0;
		}

		for(MatchableTableColumn c : record1.getSchema()) {
			if(Q.any(clusterToCompare, (c0)->c0.getIdentifier().equals(c.getIdentifier()))) {
				column1 = c;
				break;
			}
		}

		for(MatchableTableColumn c : record2.getSchema()) {
			if(Q.any(clusterToCompare, (c0)->c0.getIdentifier().equals(c.getIdentifier()))) {
				column2 = c;
				break;
			}
		}

		if(column1==null || column2==null) {
			return 0.0;
		}

		Object value1 = record1.get(column1.getColumnIndex());
		Object value2 = record2.get(column2.getColumnIndex());
		
		// we don't expect lists here, so directly compare the two values
		return similarityMeasure.calculate((DataType)value1, (DataType)value2);
	}

}
