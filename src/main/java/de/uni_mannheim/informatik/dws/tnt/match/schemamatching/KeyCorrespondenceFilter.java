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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * Removes all correspondences between tables that are not part of a candidate key mapping
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KeyCorrespondenceFilter {
	
	public Processable<Correspondence<MatchableTableColumn, Matchable>> runBlocking(
			DataSet<MatchableTableDeterminant, MatchableTableColumn> dataset,
			boolean isSymmetric,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		// join keys and correspondences by table combination
		
		
		Function<Integer, MatchableTableDeterminant> keyToTableId = new Function<Integer, MatchableTableDeterminant>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableDeterminant input) {
				return input.getTableId();
			}
		};
		Function<Integer, Correspondence<MatchableTableColumn, Matchable>> corToLeftTableId = new Function<Integer, Correspondence<MatchableTableColumn,Matchable>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTableColumn, Matchable> input) {
				return input.getFirstRecord().getTableId();
			}
		};
		Processable<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>> firstJoin = dataset.join(schemaCorrespondences, keyToTableId, corToLeftTableId);
		
		Function<Integer, Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>> joinToRightTableId = new Function<Integer, Pair<MatchableTableDeterminant,Correspondence<MatchableTableColumn,Matchable>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>> input) {
				return input.getSecond().getSecondRecord().getTableId();
			}
		};
		Processable<Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>> secondJoin = firstJoin.join(dataset, joinToRightTableId, keyToTableId);
		
		// group by table combination
		
		RecordKeyValueMapper<List<Integer>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>> groupByTableCombination = new RecordKeyValueMapper<List<Integer>, Pair<Pair<MatchableTableDeterminant,Correspondence<MatchableTableColumn,Matchable>>,MatchableTableDeterminant>, Pair<Pair<MatchableTableDeterminant,Correspondence<MatchableTableColumn,Matchable>>,MatchableTableDeterminant>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(
					Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant> record,
					DataIterator<Pair<List<Integer>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>>> resultCollector) {
				resultCollector.next(new Pair<List<Integer>, Pair<Pair<MatchableTableDeterminant,Correspondence<MatchableTableColumn,Matchable>>,MatchableTableDeterminant>>(Q.toList(record.getFirst().getFirst().getTableId(), record.getSecond().getTableId()), record));
			}
		};
		Processable<Group<List<Integer>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>>> grouped = secondJoin.group(groupByTableCombination);
		
		// filter
		
		RecordMapper<Group<List<Integer>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>>, Correspondence<MatchableTableColumn, Matchable>> transformation = new RecordMapper<Group<List<Integer>,Pair<Pair<MatchableTableDeterminant,Correspondence<MatchableTableColumn,Matchable>>,MatchableTableDeterminant>>, Correspondence<MatchableTableColumn,Matchable>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Group<List<Integer>, Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>> record,
					DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) {

				Collection<MatchableTableDeterminant> keys1 = new HashSet<>();
				Collection<MatchableTableDeterminant> keys2 = new HashSet<>();
				Collection<Correspondence<MatchableTableColumn, Matchable>> cors = new HashSet<>();
				Collection<MatchableTableColumn> mappedColumns1 = new HashSet<>();
				Collection<MatchableTableColumn> mappedColumns2 = new HashSet<>();
				Map<MatchableTableColumn,MatchableTableColumn> mapping = new HashMap<>();
				Map<MatchableTableColumn,MatchableTableColumn> mappingReverse = new HashMap<>();
				
				for(Pair<Pair<MatchableTableDeterminant, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant> p : record.getRecords().get()) {
					
					keys1.add(p.getFirst().getFirst());
					keys2.add(p.getSecond());
					
					Correspondence<MatchableTableColumn, Matchable> cor = p.getFirst().getSecond();
					
					cors.add(cor);
					
					mappedColumns1.add(cor.getFirstRecord());
					mappedColumns2.add(cor.getSecondRecord());
					
					mapping.put(cor.getFirstRecord(), cor.getSecondRecord());
					mappingReverse.put(cor.getSecondRecord(), cor.getFirstRecord());
					
				}
				
				Set<MatchableTableColumn> mappedKeyColumns = new HashSet<>();

				for(MatchableTableDeterminant k : keys1) {
					if(mapping.keySet().containsAll(k.getColumns())) {
						
						Set<MatchableTableColumn> otherColumns = new HashSet<>();
						
						for(MatchableTableColumn c : k.getColumns()) {
							otherColumns.add(mapping.get(c));
						}
						
						// check all keys that contain the same set of columns 
						for(MatchableTableDeterminant otherKey : Q.where(keys2, (k2)->k2.getColumns().containsAll(otherColumns))) {
							
							if(otherKey.getColumns().size()>otherColumns.size()) {
								// if the key has additional columns, check if they are mapped, too.
								Collection<MatchableTableColumn> additionalColumns = Q.without(otherKey.getColumns(), otherColumns);
								if(!mappedColumns2.containsAll(additionalColumns)) {
									// the key is not completely mapped
									continue;
								} else {
									// all columns are mapped
									mappedKeyColumns.addAll(Q.project(additionalColumns, (c)->mappingReverse.get(c)));
								}
							}
							mappedKeyColumns.addAll(k.getColumns());
							mappedKeyColumns.addAll(otherKey.getColumns());
							
						}
						
					}

				}
				
				for(Correspondence<MatchableTableColumn, Matchable> cor : cors) {
					
					// check if the correspondence is part of the key mapping
					if(mappedKeyColumns.contains(cor.getFirstRecord()) && mappedKeyColumns.contains(cor.getSecondRecord())) {
						resultCollector.next(cor);
					}
				}
			}
		};
		return grouped.map(transformation);
	}

}
