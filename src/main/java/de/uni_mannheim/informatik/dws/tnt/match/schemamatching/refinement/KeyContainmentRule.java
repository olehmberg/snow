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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KeyContainmentRule {

	private KnowledgeBase kb;
	private boolean verbose = false;

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	public KeyContainmentRule(KnowledgeBase kb) {
		this.kb = kb;
	}
	
	/**
	 * 
	 * @param tableToKBCorrespondences
	 * @return	Returns a map: (left=web tables) table id -> excluded candidate keys (right=knowledge base)
	 */
	public Processable<Group<Integer, MatchableTableDeterminant>> getExludedKeys(Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBCorrespondences) {
		
		
		// check if an inclusion dependency holds between two candidate classes
		
		// group table-to-kb correspondences by table/class combination
		// create table/class pairs with keys for class
		Processable<Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>> tableToClassWithKeys = tableToKBCorrespondences
			.group(
				(Correspondence<MatchableTableColumn, Matchable> record, DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				})
			// join the candidate keys from the kb (which will generate the matching rules)
			.join(kb.getCandidateKeys(), 
					(g)->g.getKey().getSecond(),
					(k)->k.getTableId());

		if(verbose) {
			Collection<Pair<Integer, Integer>> tableClassPairs = Q.project(tableToClassWithKeys.get(), 
					(Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant> p)
					-> p.getFirst().getKey()
					);
			tableClassPairs = new HashSet<>(tableClassPairs);
			System.out.println(String.format("[KeyContainmentRule] Applying rule to %d table/class pairs:", tableClassPairs.size()));
			
			for(Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant> p 
					: tableToClassWithKeys.sort((p0)->p0.getFirst().getKey().getFirst()).get()) {
				System.out.println(String.format("\t#%d (%s) / %s", 
						p.getFirst().getKey().getFirst(),
						StringUtils.join(
								new HashSet<>(Q.project(p.getFirst().getRecords().get(), (c)->c.getSecondRecord()))
								, ","),
						String.format("%s (%s)", 
								kb.getClassIndices().get(p.getFirst().getKey().getSecond()),
								StringUtils.join(Q.project(p.getSecond().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ",")
						))
						);
			}
			
			System.out.println(String.format("[KeyContainmentRule] Checking %d inclusion dependencies", kb.getInclusionDependencies().size()));
			for(Correspondence<MatchableTableDeterminant, Matchable> id : kb.getInclusionDependencies().get()) {
				String cls1 = kb.getClassIndices().get(id.getFirstRecord().getTableId());
				String cls2 = kb.getClassIndices().get(id.getSecondRecord().getTableId());
				
				System.out.println(String.format("\t%s[%s] <= %s[%s]", 
					cls1,
					StringUtils.join(Q.project(id.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ","),
					cls2,
					StringUtils.join(Q.project(id.getSecondRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ",")
					));
			}
		}
		
		// find class pairs which are connected by an inclusion dependency
		Processable<Pair<Pair<Correspondence<MatchableTableDeterminant, Matchable>, Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>>, Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>>> idJoin = kb.getInclusionDependencies()
		// join ID's LHS via class id to the table/class pairs (LHS of an ID is not a key)
		.join(tableToClassWithKeys, 
				(id)->id.getFirstRecord().getTableId(),
				(p)->p.getFirst().getKey().getSecond())
		// join ID's RHS via class id to the table/class pairs and make sure both LHS and RHS are joined to the same table id
		.join(tableToClassWithKeys, 
				(p)->new Pair<>(p.getSecond().getFirst().getKey().getFirst(), p.getFirst().getSecondRecord().getTableId()),
				(p)->new Pair<>(p.getFirst().getKey().getFirst(), p.getSecond().getTableId()));

		if(verbose) {
			System.out.println(String.format("[KeyContainmentRule] Found %d potentially covered inclusion dependencies", idJoin.size()));
		}
		
		Processable<Group<Integer, MatchableTableDeterminant>> excludedKeys = idJoin
			// exclude all keys including the RHS if both the LHS and the RHS key are covered by correspondences and the RHS key contains the RHS
			.group(
				(Pair<Pair<Correspondence<MatchableTableDeterminant, Matchable>, Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>>, Pair<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>, MatchableTableDeterminant>> record,
					DataIterator<Pair<Integer, MatchableTableDeterminant>> resultCollector) 
				-> {
					// get the join result
					int tableId = record.getSecond().getFirst().getKey().getFirst();
					Correspondence<MatchableTableDeterminant, Matchable> inclusionDependency = record.getFirst().getFirst();
					MatchableTableDeterminant rhsKey = record.getSecond().getSecond();
					
					// check if the RHS key contains the RHS
					if(rhsKey.getColumns().containsAll(inclusionDependency.getSecondRecord().getColumns())) {
					
						// check if both sides of the ID are covered by correspondences
						Processable<Correspondence<MatchableTableColumn, Matchable>> correspondencesLHS = record.getFirst().getSecond().getFirst().getRecords();
						Processable<Correspondence<MatchableTableColumn, Matchable>> correspondencesRHS = record.getSecond().getFirst().getRecords();
						Set<MatchableTableColumn> lhsColumns = new HashSet<>(Q.project(correspondencesLHS.get(), (c)->c.getSecondRecord()));
						Set<MatchableTableColumn> rhsColumns = new HashSet<>(Q.project(correspondencesRHS.get(), (c)->c.getSecondRecord()));
						
						if(lhsColumns.containsAll(inclusionDependency.getFirstRecord().getColumns()) && rhsColumns.containsAll(rhsKey.getColumns())) {							
							// the key of the RHS class of the ID has to be excluded for tableId
							resultCollector.next(new Pair<>(tableId, rhsKey));
						}
					}
				});
	

		System.out.println(String.format("[KeyContainmentRule] Excluded %d candidate keys based on inclusion dependencies", excludedKeys.size()));
		if(verbose) {
			for(Group<Integer, MatchableTableDeterminant> g : excludedKeys
					.sort((g)->g.getKey())
					.get()
					) {
				
				for(MatchableTableDeterminant k : g.getRecords().get()) {
					String clsName = kb.getClassIndices().get(k.getTableId());
					System.out.println(String.format("\t#%d -> %s: {%s}",
							g.getKey(),
							clsName, 
							StringUtils.join(Q.project(k.getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ",")
							));
				}
			}
		}
		
		 return excludedKeys.distinct();
	}
	
}
