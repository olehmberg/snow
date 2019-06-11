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

import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableEntity;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * Compares Web Table rows based on schema correspondences
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CorrespondenceBasedEntityComparator<DataType> implements Comparator<MatchableEntity, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;
	
	private SimilarityMeasure<DataType> measure;
	private KnowledgeBase kb;

	public CorrespondenceBasedEntityComparator(SimilarityMeasure<DataType> measure, KnowledgeBase kb) {
		this.measure = measure;
		this.kb = kb;
	}
	
	@Override
	public MatchableTableColumn getFirstSchemaElement(MatchableEntity record) {
		// return Comparator.super.getFirstSchemaElement(record);
		return Q.firstOrDefault(record.getEntityKey());
	}

	@Override
	public MatchableTableColumn getSecondSchemaElement(MatchableEntity record) {
		// return Comparator.super.getSecondSchemaElement(record);
		return kb.getSchema().where((c)->c.getTableId()==record.getTableId() && KnowledgeBase.RDFS_LABEL.equals(c.getHeader())).firstOrNull();
	}

//	private static final Pattern numberingPattern = Pattern.compile("\\d*\\. (.+)");
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator#compare(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@Override
	public double compare(MatchableEntity record1, MatchableEntity record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {

        if(schemaCorrespondence==null) {
            return 0.0;
        }

        MatchableTableColumn col1 = schemaCorrespondence.getFirstRecord();
        MatchableTableColumn col2 = schemaCorrespondence.getSecondRecord();

        if(col1.getDataSourceIdentifier()!=record1.getDataSourceIdentifier() || col2.getDataSourceIdentifier()!=record2.getDataSourceIdentifier()) {
            return 0.0;
        }

		Object value1 = record1.get(col1.getColumnIndex());
		Object value2 = record2.get(col2.getColumnIndex());
		
		Object[] listValues1 = null;
		if(value1==null) {
			listValues1 = new Object[] { null };
		} else if(value1.getClass().isArray()) {
			listValues1 = (Object[])value1;
		} else {
			listValues1 = new Object[] { value1 };
		}

		Object[] listValues2 = null;
		if(value2==null) {
			listValues2 = new Object[] { null };
		} else if(value2.getClass().isArray()) {
			listValues2 = (Object[])value2;
		} else {
			listValues2 = new Object[] { value2 };
		}

		double maxSim = 0.0;
		for(Object v1 : listValues1) {
			for(Object v2 : listValues2) {
				maxSim = Math.max(maxSim, compareValues(v1, v2));
			}
		}

		return maxSim;
		
	}

	protected double compareValues(Object value1, Object value2) {
		if(Q.equals(value1, value2, false)) {
			// the values are equal
			return 1.0;
		} else {
			// the values are not equal
			if(value1==null && value2==null) {
				// because both are null
				return 0.0;
			} else {
				return measure.calculate((DataType)value1, (DataType)value2);
			}
		}
	}

}
