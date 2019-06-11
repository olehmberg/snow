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

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.FilteringMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CellOverlapMatchingRule extends FilteringMatchingRule<MatchableTableRow, MatchableTableColumn> {

	private boolean evaluateNulls;

	/**
	 * @param finalThreshold
	 */
	public CellOverlapMatchingRule(double finalThreshold, boolean evaluateNulls) {
		super(finalThreshold);
		this.evaluateNulls = evaluateNulls;
	}

	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator#compare(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@Override
	public double compare(MatchableTableRow record1, MatchableTableRow record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {
		return 0;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.FilteringMatchingRule#apply(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable)
	 */
	@Override
	public Correspondence<MatchableTableRow, MatchableTableColumn> apply(MatchableTableRow record1,
			MatchableTableRow record2,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {

				Set<MatchableTableColumn> r2Mapped = new HashSet<>();

		if(schemaCorrespondences==null || schemaCorrespondences.size()==0) {
			
			for(MatchableTableColumn c1 : record1.getSchema()) {
				for(MatchableTableColumn c2 : record2.getSchema()) {
					if(!r2Mapped.contains(c2)) {
						
						Object v1 = record1.get(c1.getColumnIndex());
						Object v2 = record2.get(c2.getColumnIndex());
						
						if(Q.equals(v1, v2, evaluateNulls)) {
							r2Mapped.add(c2);
							break;
						}
						
					}
				}
			}

		} else {
			for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
				MatchableTableColumn c1 = cor.getFirstRecord();
				MatchableTableColumn c2 = cor.getSecondRecord();

				Object v1 = record1.get(c1.getColumnIndex());
				Object v2 = record2.get(c2.getColumnIndex());
				
				if(Q.equals(v1, v2, evaluateNulls)) {
					r2Mapped.add(c2);
				}
			}
		}
		
		return new Correspondence<MatchableTableRow, MatchableTableColumn>(record1, record2, r2Mapped.size());
	}

}
