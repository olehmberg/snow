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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement;

import java.util.Map;

import de.uni_mannheim.informatik.dws.tnt.match.data.ClassHierarchy;
import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableEntity;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableLodColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ClassHierarchyBasedScorePropagation {

	private KnowledgeBase kb;
	private ClassHierarchy hierarchy;
	
	public ClassHierarchyBasedScorePropagation(KnowledgeBase kb) {
		this.kb = kb;
		hierarchy = new ClassHierarchy(kb);
	}
	
	public Processable<Correspondence<MatchableTableColumn, MatchableEntity>> runScorePropagation(Processable<Correspondence<MatchableTableColumn, MatchableEntity>> schemaCorrespondences) {
		
		Map<Pair<Integer, String>, MatchableTableColumn> attributesByClassIdAndURI = Pair.toMap(
				kb.getSchema()
				.map((MatchableTableColumn record, DataIterator<Pair<Pair<Integer, String>, MatchableTableColumn>> resultCollector) 
				->{
					resultCollector.next(
						new Pair<>(
								new Pair<>(record.getTableId(), ((MatchableLodColumn)record).getUri()),
								record));
				})
				.get());
		
//		for(MatchableTableColumn c : kb.getSchema().sort((c)->c.getTableId()).get()) {
//			if(!"URI".equals(c.getHeader())) {
//				System.out.println(String.format("#%d\t[%d] %s\t%s", c.getTableId(), c.getColumnIndex(), ((MatchableLodColumn)c).getUri(), c));
//			}
//		}
		
		// aggregate the scores of all sub class for attributes which exist in the parent node
		// sum up all scores of correspondences to the same URL (property URI) of all sub classes
		Processable<Correspondence<MatchableTableColumn, MatchableEntity>> result = schemaCorrespondences		
			.aggregate(new RecordKeyValueMapper<Pair<MatchableTableColumn, Pair<Integer, String>>, Correspondence<MatchableTableColumn,MatchableEntity>, Correspondence<MatchableTableColumn,MatchableEntity>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void mapRecordToKey(Correspondence<MatchableTableColumn, MatchableEntity> record,
						DataIterator<Pair<Pair<MatchableTableColumn, Pair<Integer, String>>, Correspondence<MatchableTableColumn, MatchableEntity>>> resultCollector) {
					
					// group every correspondence into each of its super classes
					String cls = kb.getClassIndices().get(record.getSecondRecord().getTableId());
					for(String superClass : hierarchy.getSuperClasses(cls)) {
						Integer superClassId = kb.getClassIds().get(superClass);
						MatchableLodColumn property = (MatchableLodColumn)record.getSecondRecord();
			
						resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord(), new Pair<>(superClassId, property.getUri())), record));
					}
					
					// and also into the class itself
					MatchableLodColumn property = (MatchableLodColumn)record.getSecondRecord();
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord(), new Pair<>(record.getSecondRecord().getTableId(), property.getUri())), record));
					
				}
			}, new DataAggregator<Pair<MatchableTableColumn, Pair<Integer, String>>, Correspondence<MatchableTableColumn, MatchableEntity>, Correspondence<MatchableTableColumn, MatchableEntity>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Pair<Correspondence<MatchableTableColumn, MatchableEntity>, Object> initialise(
						Pair<MatchableTableColumn, Pair<Integer, String>> keyValue) {
					
					MatchableTableColumn attribute = attributesByClassIdAndURI.get(keyValue.getSecond());
					
					if(attribute!=null) {
						// if the attribute exists for this class, initialise it with a score of 0 
						return stateless(new Correspondence<MatchableTableColumn, MatchableEntity>(keyValue.getFirst(), attribute, 0.0, new ProcessableCollection<>()));
					} else {
						// otherwise, don't aggregate the scores
//						return stateless(null);
						MatchableTableColumn nonExistingAttribute = new MatchableLodColumn(String.format("%s~Col-1", kb.getClassIndices().get(keyValue.getSecond().getFirst())), keyValue.getSecond().getFirst(), -1, "not existing");
						return stateless(new Correspondence<MatchableTableColumn, MatchableEntity>(keyValue.getFirst(), nonExistingAttribute, 0.0, new ProcessableCollection<>()));
					}
				}

				@Override
				public Pair<Correspondence<MatchableTableColumn, MatchableEntity>, Object> aggregate(
						Correspondence<MatchableTableColumn, MatchableEntity> previousResult,
						Correspondence<MatchableTableColumn, MatchableEntity> record, Object state) {

					Correspondence<MatchableTableColumn, MatchableEntity> result = null;
					
					if(previousResult==null) {
						return stateless(null);
					}
	
					result = new Correspondence<MatchableTableColumn, MatchableEntity>(
							previousResult.getFirstRecord(),
							previousResult.getSecondRecord(),
							record.getSimilarityScore() + previousResult.getSimilarityScore(),
							previousResult.getCausalCorrespondences().append(record.getCausalCorrespondences()));
					
					return stateless(result);
				}

				@Override
				public Pair<Correspondence<MatchableTableColumn, MatchableEntity>, Object> merge(
						Pair<Correspondence<MatchableTableColumn, MatchableEntity>, Object> intermediateResult1,
						Pair<Correspondence<MatchableTableColumn, MatchableEntity>, Object> intermediateResult2) {
					return aggregate(intermediateResult1.getFirst(), intermediateResult2.getFirst(), null);
				}
				
				/* (non-Javadoc)
				 * @see de.uni_mannheim.informatik.dws.winter.processing.DataAggregator#createFinalValue(java.lang.Object, java.lang.Object, java.lang.Object)
				 */
				@Override
				public Correspondence<MatchableTableColumn, MatchableEntity> createFinalValue(
						Pair<MatchableTableColumn, Pair<Integer, String>> keyValue,
						Correspondence<MatchableTableColumn, MatchableEntity> result, Object state) {
					
					// check if the class actually contains the attribute
					MatchableTableColumn attribute = attributesByClassIdAndURI.get(keyValue.getSecond());
					
					// if not, set the score to 0 but keep the correspondence for normalisation
					if(attribute==null) {
						return new Correspondence<MatchableTableColumn, MatchableEntity>(result.getFirstRecord(), result.getSecondRecord(), 0.0, result.getCausalCorrespondences());
					} else {
						return result;
					}
				}
			})
			.map(
				(Pair<Pair<MatchableTableColumn, Pair<Integer, String>>, Correspondence<MatchableTableColumn, MatchableEntity>> record, DataIterator<Correspondence<MatchableTableColumn, MatchableEntity>> resultCollector) 
				-> {
					if(record.getSecond()!=null) {
						resultCollector.next(record.getSecond());
					}
				});
		
		return result;
		
	}
	
}
