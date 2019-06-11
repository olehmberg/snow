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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.duplicatebased;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Pattern;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableEntity;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.matching.rules.VotingMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableEntityToKBVotingRule extends VotingMatchingRule<MatchableTableColumn, MatchableEntity> {

	private SimilarityMeasure<Double> numericSimilarity = null;
	private SimilarityMeasure<LocalDateTime> dateSimilarity = null;
	private SimilarityMeasure<String> stringSimilarity = null;
	
	private boolean logVotes = false;
	public void setLogVotes(boolean logVotes) {
		this.logVotes = logVotes;
	}
	
	/**
	 * @param finalThreshold
	 */
	public TableEntityToKBVotingRule(double finalThreshold, SimilarityMeasure<Double> numericSimilarity, SimilarityMeasure<LocalDateTime> dateSimilarity, SimilarityMeasure<String> stringSimilarity) {
		super(finalThreshold);
		this.numericSimilarity = numericSimilarity;
		this.dateSimilarity = dateSimilarity;
		this.stringSimilarity = stringSimilarity;
	}

	private static final long serialVersionUID = 1L;

	private static final Pattern toWhiteSpacePattern = Pattern.compile("[.!?]+");
	private static final Pattern removePattern = Pattern.compile("[^a-z0-9\\s]");
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.VotingMatchingRule#compare(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@Override
	public double compare(MatchableTableColumn record1, MatchableTableColumn record2,
			Correspondence<MatchableEntity, Matchable> correspondence) {

		double similarity = 0.0;
		
		if(record1.getTableId()==correspondence.getFirstRecord().getTableId() && record2.getTableId()==correspondence.getSecondRecord().getTableId()) {
			Object value1 = correspondence.getFirstRecord().get(record1.getColumnIndex());
			Object value2 = correspondence.getSecondRecord().get(record2.getColumnIndex());
			
			if(value1!=null && value2!=null) {
				switch (record1.getType()) {
				case numeric:
					similarity = compareNumeric(value1, value2);
					break;
				case date:
					similarity = compareDate(value1, value2);
					break;
				default:
					similarity = compareString(value1, value2);
					break;
				}
			}
			
			if(logVotes && similarity>=getFinalThreshold()) {
				System.out.println(String.format("[Vote] '%s' votes for %s <-> %s (%f) with '%s'='%s'", correspondence.getFirstRecord().get(0), record1, record2, similarity, value1, value2));
			}
		}
		
		return similarity;
	}

	protected double compareNumeric(Object value1, Object value2) {
		Collection<Double> rightValues = null;
		
		if(value2.getClass().isArray()) {
			rightValues = new HashSet<>();
			for(Object v : (Object[])value2) {
				rightValues.add((Double)v);
			}
		} else {
			rightValues = Q.toSet((Double)value2);
		}
		
		double similarity = 0.0;
		
		for(Double v2 : rightValues) {
			similarity = Math.max(similarity, numericSimilarity.calculate((Double)value1, v2));
		}
		
		return similarity;
	}
	
	protected double compareDate(Object value1, Object value2) {
		Collection<LocalDateTime> rightValues = null;
		
		if(value2.getClass().isArray()) {
			rightValues = new HashSet<>();
			for(Object v : (Object[])value2) {
				rightValues.add((LocalDateTime)v);
			}
		} else {
			rightValues = Q.toSet((LocalDateTime)value2);
		}
		
		double similarity = 0.0;
		for(LocalDateTime v2 : rightValues) {
			similarity = Math.max(similarity, dateSimilarity.calculate((LocalDateTime)value1, v2));
		}
		
		return similarity;
	}
	
	protected double compareString(Object value1, Object value2) {
		// pre-process value1
		String stringValue1 = value1.toString();
		stringValue1 = stringValue1.toLowerCase();
		stringValue1 = toWhiteSpacePattern.matcher(stringValue1).replaceAll(" ");
		stringValue1 = removePattern.matcher(stringValue1).replaceAll("");
		stringValue1 = WebTablesStringNormalizer.normalise(stringValue1, false);
		value1 = stringValue1;
		
		Collection<String> rightValues = null;
		
		if(value2.getClass().isArray()) {
			rightValues = new HashSet<>();
			for(Object v : (Object[])value2) {
				// pre-process value
				String stringValue2 = v.toString();
				stringValue2 = stringValue2.toLowerCase();
				stringValue2 = toWhiteSpacePattern.matcher(stringValue2).replaceAll(" ");
				stringValue2 = removePattern.matcher(stringValue2).replaceAll("");
				stringValue2 = WebTablesStringNormalizer.normalise(stringValue2, false);
				rightValues.add(stringValue2);
			}
		} else {
			// pre-process value
			String stringValue2 = value2.toString();
			stringValue2 = stringValue2.toLowerCase();
			stringValue2 = toWhiteSpacePattern.matcher(stringValue2).replaceAll(" ");
			stringValue2 = removePattern.matcher(stringValue2).replaceAll("");
			stringValue2 = WebTablesStringNormalizer.normalise(stringValue2, false);
			rightValues = Q.toSet(stringValue2);
		}
		
		double similarity = 0.0;
		for(String v2 : rightValues) {
			similarity = Math.max(similarity, stringSimilarity.calculate(stringValue1, v2));
		}
		
		return similarity;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.AggregableMatchingRule#generateAggregationKey(de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@Override
	protected Pair<MatchableTableColumn, MatchableTableColumn> generateAggregationKey(
			Correspondence<MatchableTableColumn, MatchableEntity> cor) {
		return new Pair<MatchableTableColumn, MatchableTableColumn>(cor.getFirstRecord(), cor.getSecondRecord());
	}
}
