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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.Normalizer;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * Compares a Web Table row to a KB row
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowToKBComparator<DataType> implements Comparator<MatchableTableRow, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;
	
	private MatchableTableColumn column2;
	
	private boolean strict = false;
	public void setStrict(boolean strict) {
		this.strict = strict;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator#getSecondSchemaElement()
	 */
	@Override
	public MatchableTableColumn getSecondSchemaElement(MatchableTableRow record) {
		return column2;
	}
	
	private SimilarityMeasure<DataType> measure;
	
	private double similarityThreshold = 0.0;
	
	public MatchableTableRowToKBComparator(MatchableTableColumn column2, SimilarityMeasure<DataType> measure, double similarityThreshold) {
		this.column2 = column2;
		this.measure = measure;
		this.similarityThreshold = similarityThreshold;
	}
	
	private static final Pattern bracketsPattern = Pattern.compile("\\(.*\\)|\\[.*\\]");
	private static final Pattern numberingPattern = Pattern.compile("\\d*\\. (.+)");
	
	private static final Pattern toWhiteSpacePattern = Pattern.compile("[.!?]+");
	private static final Pattern removePattern = Pattern.compile("[^a-z0-9\\s]");
	
	public static String preprocessString(String s) {
		String stringValue1 = s;
		// web table values are already lower-cased
		
		stringValue1 = bracketsPattern.matcher(stringValue1).replaceAll("");
		
		Matcher m = numberingPattern.matcher(stringValue1);
		if(m.matches()) {
			stringValue1 = m.group(1);
		}
		
		stringValue1 = toWhiteSpacePattern.matcher(stringValue1).replaceAll(" ");
		stringValue1 = removePattern.matcher(stringValue1).replaceAll("");
		//alternative to removing non-ASCII characters:
		// stringValue1 = Normalizer.normalize(stringValue1, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
					
		stringValue1 = WebTablesStringNormalizer.normalise(stringValue1, false);
		
		return stringValue1;
	}

	public static String preprocessKBString(String s) {
		String stringValue2 = s;
			
		stringValue2 = stringValue2.toLowerCase();
		stringValue2 = toWhiteSpacePattern.matcher(stringValue2).replaceAll(" ");
		stringValue2 = removePattern.matcher(stringValue2).replaceAll("");
		
		stringValue2 = WebTablesStringNormalizer.normalise(stringValue2, false);

		return stringValue2;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator#compare(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public double compare(MatchableTableRow record1, MatchableTableRow record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {

		if(schemaCorrespondence==null) {
			return 0.0;
		}
		
		MatchableTableColumn column1 = schemaCorrespondence.getFirstRecord();
		
		if(record1.getTableId()==column1.getTableId() && record2.getTableId()==column2.getTableId()) {

			Object value1 = record1.get(column1.getColumnIndex());
			Object value2 = record2.get(column2.getColumnIndex());
			
			if(value1==null || value2==null) {
				return 0.0;
			} else {
				
				try {
					DataType cast = (DataType)value1;
				} catch(Exception e) {
					System.out.println(String.format("[MatchableTableRowToKBComparator] Cannot cast web table value '%s' to target type %s (source: %s / %s)", 
							value1.toString(),
							column1.getType(),
							column1,
							record2));
					return 0.0;
				}
				
				// pre-process the web table value
				switch (column1.getType()) {
				case string:
					value1 = preprocessString(value1.toString());
					
					break;
				default:
					break;
				}
				
				// KB values can be lists, calculate similarity for each list value and use the maximum
				Object[] kbValues = null;
				
				if(value2.getClass().isArray()) {
					kbValues = (DataType[])value2;
				} else {
					kbValues = new Object[] { value2 };
				}
				
				double similarity = 0.0;
				
				for(Object v2 : kbValues) {
	
					if(v2!=null) {
						
						try {
							DataType cast = (DataType)v2;
						} catch(Exception e) {
							System.out.println(String.format("[MatchableTableRowToKBComparator] Cannot cast knowledge base value '%s' to target type %s (source: %s / %s)", 
									v2.toString(),
									column2.getType(),
									column2,
									record2));
							continue;
						}
						
						// pre-process the KB value(s)
						switch (column2.getType()) {
						case string:
							v2 = preprocessKBString(v2.toString());
							break;
						default:
							break;
						}
						
						try {
							if(!strict) {
								similarity = Math.max(similarity, measure.calculate((DataType)value1, (DataType)v2));
							} else {
								if(similarity==0.0) {
									similarity = measure.calculate((DataType)value1, (DataType)v2);
								} else {
									similarity = Math.min(similarity, measure.calculate((DataType)value1, (DataType)v2));
								}
							}
						} catch(ClassCastException e) {
							System.out.println(String.format("[MatchableTableRowToKBComparator] Cannot cast values '%s' / '%s' to target type %s (sources: %s/%s, %s/%s)",
									value1.toString(),
									v2.toString(),
									column2.getType(),
									column1,
									record1,
									column2,
									record2));
						}
					}
				}
				
				return similarity>=similarityThreshold ? similarity : 0;
			}
		} else {
			return 0.0;
		}
	}

	@Override
	public String toString() {
		return String.format("%s(x,%s)", measure.getClass().getSimpleName(), column2.toString());
	}
}
