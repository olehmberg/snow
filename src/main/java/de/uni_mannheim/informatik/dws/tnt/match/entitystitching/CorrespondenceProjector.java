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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CorrespondenceProjector {

	/**
	 * creates new correspondences which are equivalent to the passed correspondences w.r.t. a table projection based on column provenance
	 * 
	 * @param schemaCorrespondences
	 * @return
	 */
	public static Processable<Correspondence<MatchableTableColumn, Matchable>> projectCorrespondences(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences, DataSet<MatchableTableColumn, MatchableTableColumn> projectedAttributes, Collection<Table> projectedTables, DataSet<MatchableTableColumn, MatchableTableColumn> originalAttributes, boolean verbose) {
		
		System.out.println(String.format("[projectCorrespondences] %d tables", projectedTables.size()));
		
		// first, create a map from the original columns to the projected columns
		Map<MatchableTableColumn, Collection<MatchableTableColumn>> originalToProjected = new HashMap<>();
		for(Table t : projectedTables) {
			for(TableColumn c : t.getColumns()) {
				
				boolean hasOriginal = false;
				
				if(c.getProvenance().size()==0) {
					System.out.println(String.format("[projectCorrespondences] column %s has no provenance information!", c));
				}
				
				for(String prov : new HashSet<>(c.getProvenance())) {
					
					MatchableTableColumn original = originalAttributes.getRecord(prov);
					
					if(original!=null) {
						MatchableTableColumn projected = projectedAttributes.getRecord(c.getIdentifier());
						
						Collection<MatchableTableColumn> projections = originalToProjected.get(original);
						
						if(projections==null) {
							projections = new LinkedList<>();
							originalToProjected.put(original, projections);
						}
						
						projections.add(projected);
						
						int fkCount = Q.where(projections, (c1)->c1.getHeader().equals("FK")).size();
						if(!(fkCount==0 || fkCount==projections.size())) {
							System.out.println("Inconsistent column provenance!");
						}
						
						hasOriginal = true;
					}
//					else {
//						System.out.println(String.format("Missing original attribute %s for %s: %s", prov, c.getIdentifier(), c.getHeader()));
//					}
				}
				
				if(!hasOriginal) {
					
					// maybe the column was not projected at all, check if its ID existed in the original attributes
					MatchableTableColumn original = originalAttributes.getRecord(c.getIdentifier());
					if(original!=null) {
						originalToProjected.put(original, Q.toList(projectedAttributes.getRecord(c.getIdentifier())));
					} else {
						System.out.println(String.format("Column %s has no provenance information for a known column!", c));
					}
				}
			}
		}
		
		System.out.println(String.format("[projectCorrespondences] found translations for %d original columns to projected columns", originalToProjected.size()));
		
		// then, project all correspondences involving the original columns
		Processable<Correspondence<MatchableTableColumn, Matchable>> result = schemaCorrespondences
//			.where((c)->originalColumnIds.containsKey(c.getFirstRecord().getIdentifier()) || originalColumnIds.containsKey(c.getSecondRecord().getIdentifier()))
			.map(
				(Correspondence<MatchableTableColumn, Matchable> cor, DataIterator<Correspondence<MatchableTableColumn, Matchable>> col)
				-> {
					MatchableTableColumn col1 = cor.getFirstRecord();
					MatchableTableColumn col2 = cor.getSecondRecord();
					
					Collection<MatchableTableColumn> cols1 = originalToProjected.get(col1);
					Collection<MatchableTableColumn> cols2 = originalToProjected.get(col2);
					
					if(cols1==null) {
						if(verbose) {
							System.err.println(String.format("No projections for %s: %s", col1.getIdentifier(), col1.getHeader()));
						}
					} else if(cols2==null) {
						if(verbose) {
							System.err.println(String.format("No projections for %s: %s", col2.getIdentifier(), col2.getHeader()));
						}
					} else {
						for(MatchableTableColumn c1 : cols1) {
							
							for(MatchableTableColumn c2 : cols2) {
						
								Correspondence<MatchableTableColumn, Matchable> newCor = new Correspondence<>(c1, c2, cor.getSimilarityScore());
								col.next(newCor);
								
								newCor = new Correspondence<>(c2, c1, cor.getSimilarityScore());
								col.next(newCor);

								if(c1.getHeader().equals("FK") && !c2.getHeader().equals("FK")) {
									System.out.println(String.format("[CorrespondenceProjector] FK column mapped to non-FK column! %s <-> %s (projecting from %s <-> %s)", c1, c2, col1, col2));
								}
								
								if(verbose) {
									System.out.println(String.format("%s <-> %s", c1, c2));
								}
								
//								if(cols1.size()>1 && cols2.size()>1) {
//									System.out.println(String.format("%s <-> %s", c1, c2));
//								}
							}
							
						}
					}
				});
		
		return result;
	}

	/**
	 * given a set of tables and another set of tables that contains projections of the first set of tables, this method creates correspondeces between all projected columns that were created from the same column in the first set
	 * @param schemaCorrespondences
	 * @param projectedAttributes
	 * @param projectedTables
	 * @param originalAttributes
	 * @param verbose
	 * @return
	 */
	public static Processable<Correspondence<MatchableTableColumn, Matchable>> createCorrespondencesFromProjection(Collection<Table> originalTables, Collection<Table> projectedTables, DataSet<MatchableTableColumn, MatchableTableColumn> projectedAttributes) {
		
		Map<String, Set<String>> originalColumnIds = new HashMap<>();
		
		for(Table t : originalTables) {
			for(TableColumn c : t.getColumns()) {
				originalColumnIds.put(c.getIdentifier(), new HashSet<>());
			}
		}
		
		for(Table t : projectedTables) {
			for(TableColumn c : t.getColumns()) {
				for(String prov : c.getProvenance()) {
					Collection<String> projections = originalColumnIds.get(prov);
					if(projections!=null) {
						projections.add(c.getIdentifier());
					}
				}
			}
		}
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> result = new ParallelProcessableCollection<>();
		
		for(Set<String> projections : originalColumnIds.values()) {
			List<String> list = new ArrayList<>(projections);
			
			for(int i = 0; i < list.size(); i++) {
				for(int j = i+1; i!=j && j < list.size(); j++) {
					MatchableTableColumn c1 = projectedAttributes.getRecord(list.get(i));
					MatchableTableColumn c2 = projectedAttributes.getRecord(list.get(j));
					
					Correspondence<MatchableTableColumn, Matchable> cor = new Correspondence<MatchableTableColumn, Matchable>(c1, c2, 1.0);
					result.add(cor);

					cor = new Correspondence<MatchableTableColumn, Matchable>(c2, c1, 1.0);
					result.add(cor);
				}
			}
		}
		
		return result;
	}
	
	/**
	 * given a set of tables and another set of tables that contains projections of the first set of tables, this method creates correspondeces between all projected columns and the columns they were projected from
	 * @param projectedAttributes
	 * @param projectedTables
	 * @param originalAttributes
	 * @param projectedAttributes
	 * @param verbose
	 * @return
	 */
	public static Processable<Correspondence<MatchableTableColumn, Matchable>> createProjectionCorrespondences(Collection<Table> originalTables, Collection<Table> projectedTables, DataSet<MatchableTableColumn, MatchableTableColumn> originalAttributes, DataSet<MatchableTableColumn, MatchableTableColumn> projectedAttributes) {
		
		Map<String, Set<String>> originalColumnIds = new HashMap<>();
		
		for(Table t : originalTables) {
			for(TableColumn c : t.getColumns()) {
				originalColumnIds.put(c.getIdentifier(), new HashSet<>());
			}
		}
		
		for(Table t : projectedTables) {
			for(TableColumn c : t.getColumns()) {
				for(String prov : c.getProvenance()) {
					Collection<String> projections = originalColumnIds.get(prov);
					if(projections!=null) {
						projections.add(c.getIdentifier());
					}
				}
			}
		}
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> result = new ParallelProcessableCollection<>();
		
		for(String originalColumnId : originalColumnIds.keySet()) {
			for(String projection : originalColumnIds.get(originalColumnId)) {
				
				MatchableTableColumn c1 = originalAttributes.getRecord(originalColumnId);
				MatchableTableColumn c2 = projectedAttributes.getRecord(projection);
				
				Correspondence<MatchableTableColumn, Matchable> cor = new Correspondence<MatchableTableColumn, Matchable>(c1, c2, 1.0);
				result.add(cor);

				cor = new Correspondence<MatchableTableColumn, Matchable>(c2, c1, 1.0);
				result.add(cor);
				
			}
		}
		
		return result;
	}
}
