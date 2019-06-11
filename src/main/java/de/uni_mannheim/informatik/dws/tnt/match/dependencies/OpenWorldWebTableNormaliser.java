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
package de.uni_mannheim.informatik.dws.tnt.match.dependencies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.Table.ConflictHandling;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * Performs normalisation of web tables for differnt normal forms.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class OpenWorldWebTableNormaliser {


	/**
	 * Normalises the given table into 3NF.
	 * Requires the table's schema (Table.getSchema().getFunctionalDependencies()) to contain a canonical cover of the tables functional dependencies.
	 * Requires the table's schema (Table.getSchema().getCandidateKeys()) to contain at least one candidate key of the table
	 */
	public Collection<Table> normaliseTo3NF(Table tableToNormalise, int nextTableId, TableColumn foreignKey, boolean addFullClosure, Set<TableColumn> nullFreeSubscheme) throws Exception {
		/***********************************************
    	 * Normalisation to 3NF
    	 ***********************************************/
		
		Collection<Table> result = new LinkedList<>();
		
		Func<String, TableColumn> headerP = new TableColumn.ColumnHeaderProjection();
		
		StringBuilder logData = new StringBuilder();

		Table t = tableToNormalise;

		logData.append(String.format("Table #%d '%s' with schema '%s'\n", t.getTableId(), t.getPath(), StringUtils.join(Q.project(t.getColumns(), headerP), ",")));
		logData.append(String.format("Table #%d '%s' with null-free subscheme '%s'\n", t.getTableId(), t.getPath(), StringUtils.join(Q.project(nullFreeSubscheme, headerP), ",")));
		
		boolean hasCandidateKeyDecomposition = false;

		List<Pair<Set<TableColumn>,Set<TableColumn>>> violations = new LinkedList<>();
		// iterate over all functional dependencies
		for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {

			Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
			Collection<TableColumn> closure = OpenWorldFunctionalDependencyUtils.closure(new HashSet<>(det), t.getSchema().getFunctionalDependencies(), nullFreeSubscheme);

			// 3NF decomposition: create a new relation for every FD in the canonical cover

			// only use strong FDs for decomposition?
			if(nullFreeSubscheme.containsAll(det)) {
				violations.add(new Pair<>(new HashSet<>(det), new HashSet<>(dep)));
			
				
				logData.append(String.format("\t3NF violation: {%s}->{%s}\n", StringUtils.join(Q.project(det, headerP), ","), StringUtils.join(Q.project(t.getSchema().getFunctionalDependencies().get(det), headerP), ",")));
				if(closure.size()==tableToNormalise.getColumns().size()) {
					hasCandidateKeyDecomposition = true;
				}
			}
		}


		FunctionalDependencySet H = new FunctionalDependencySet(t.getSchema().getFunctionalDependencies());
		H.setTableId("H");
		H.setVerbose(true);

		// let J = {}
		FunctionalDependencySet J = new FunctionalDependencySet(new HashMap<>());
		J.setTableId("J");
		J.setVerbose(true);

		// merge all violations which are bijections:
		// - if two violations X -> V and Y -> W exists 
		// - and the closure of the table to normalise contains X <-> Y
		// - then merge the violations

		// Merge equivalent keys
		// for each pair of groups, say H1 and H2, with left sides X and Y, respectively, merge H1 and H2 together if there is a bijection X <-> Y in H+
		List<Pair<Set<TableColumn>, Set<TableColumn>>> violationList = new LinkedList<>(violations);
		List<Set<TableColumn>> violationClosures = new LinkedList<>();
		for(int i=0; i<violationList.size(); i++) {
			Pair<Set<TableColumn>, Set<TableColumn>> v1 = violationList.get(i);
			Set<TableColumn> v1Closure = OpenWorldFunctionalDependencyUtils.closure(v1.getFirst(), H.getFunctionalDependencies(), nullFreeSubscheme);
			violationClosures.add(v1Closure);
		}

		
		Map<Set<TableColumn>, Collection<Pair<Set<TableColumn>, Set<TableColumn>>>> grouped = new HashMap<>();
		for(int i=0; i<violationList.size(); i++) {
			Pair<Set<TableColumn>, Set<TableColumn>> H1 = violationList.get(i);
			Set<TableColumn> H1Closure = violationClosures.get(i);

			Collection<Pair<Set<TableColumn>, Set<TableColumn>>> group = new HashSet<>();
			group.add(H1);
			for(int j=i+1; j<violationList.size(); j++) {
				Pair<Set<TableColumn>, Set<TableColumn>> H2 = violationList.get(j);
				Set<TableColumn> H2Closure = violationClosures.get(j);

				// check if there is a bijection
				if(H1Closure.containsAll(H2Closure) && H2Closure.containsAll(H1Closure)) {
					group.add(H2);
				}
			}
			grouped.put(H1.getFirst(), group);
		}

		Set<Set<Set<TableColumn>>> groups = new HashSet<>();
		for(Set<TableColumn> group : grouped.keySet()) {
			Set<Set<TableColumn>> merged = new HashSet<>();
			for(Pair<Set<TableColumn>, Set<TableColumn>> v : grouped.get(group)) {
				merged.add(v.getFirst());
			}
			groups.add(merged);

			if(merged.size()>1) {

				logData.append(String.format("\tmerging bijection: {%s}\n",
					StringUtils.join(
						Q.project(merged, (cols)->StringUtils.join(Q.project(cols,(col)->col.getHeader()), ",")),
						"}<->{"
					)
				));

				List<Pair<Set<TableColumn>, Set<TableColumn>>> mergedGroups = new LinkedList<>(grouped.get(group));

				// for every pair of merged groups H1, H2 with LHSs X and Y respectively
				for(int i = 0; i < mergedGroups.size(); i++) {
					Pair<Set<TableColumn>, Set<TableColumn>> H1 = mergedGroups.get(i);
					Set<TableColumn> X = H1.getFirst();
					for(int j = i+1; j < mergedGroups.size(); j++) {
						Pair<Set<TableColumn>, Set<TableColumn>> H2 = mergedGroups.get(j);
						Set<TableColumn> Y = H2.getFirst();

						// add X -> Y and Y -> X to J
						J.addFunctionalDependency(new Pair<>(X, Y));
						J.addFunctionalDependency(new Pair<>(Y, X));

						// for each A in Y, if X -> A in H (the closure of the input table), then delete it from H
						for(TableColumn A : Y) {
							H.removeFunctionalDependency(new Pair<>(X, Q.toSet(A)));
						}
						// do the same for each Y -> B in H with B in X
						for(TableColumn B : X) {
							H.removeFunctionalDependency(new Pair<>(Y, Q.toSet(B)));
						}
					}
				}
			}
		}
		
		// eliminate transitive FDs
		// find an H' subset of H such that (H' + J)+ = (H + J)+ and no proper subset of H' has this property
		FunctionalDependencySet HPrime = new FunctionalDependencySet(H.getFunctionalDependencies());
		HPrime.setTableId("H'");
		FunctionalDependencySet HplusJ = new FunctionalDependencySet(new HashMap<>());
		HplusJ.add(H);
		HplusJ.add(J);
		for(Pair<Set<TableColumn>,Set<TableColumn>> fdToCheck : FunctionalDependencyUtils.split(Pair.fromMap(HPrime.getFunctionalDependencies()))) {
			FunctionalDependencySet newHPrime = new FunctionalDependencySet(H.getFunctionalDependencies());
			newHPrime.removeFunctionalDependency(fdToCheck);
			FunctionalDependencySet newHPrimePlusJ = new FunctionalDependencySet(newHPrime.getFunctionalDependencies());
			newHPrimePlusJ.add(J);

			if(OpenWorldFunctionalDependencyUtils.areEquivalentSets(newHPrimePlusJ.getFunctionalDependencies(), HplusJ.getFunctionalDependencies(), nullFreeSubscheme)) {
				HPrime = newHPrime;
			}
		}

		// if an FK was specified, only keep violations which contain the FK
		List<Pair<Set<TableColumn>, Set<TableColumn>>> filteredViolations = new LinkedList<>();
		for(Pair<Set<TableColumn>, Set<TableColumn>> v : violations) {
			if(foreignKey==null || Q.union(v.getFirst(), v.getSecond()).contains(foreignKey)) {
				filteredViolations.add(v);
			}
		}
		violations = filteredViolations;

		for(Set<Set<TableColumn>> group : groups) {			
			Collection<TableColumn> columns = new HashSet<>();
			FunctionalDependencySet fds = new FunctionalDependencySet(new HashMap<>());
			for(Set<TableColumn> det : group) {
				columns.addAll(det);
				Set<TableColumn> dep = HPrime.getFunctionalDependencies().get(det);
				if(dep!=null) {
					fds.addFunctionalDependency(new Pair<>(det, dep));
					columns.addAll(dep);
				}
			}
			// add each FD of J into its corresponding group of H'
			fds.add(J);

			// create the normalised relation
			columns = Q.sort(columns, new TableColumn.TableColumnByIndexComparator());
			Set<TableColumn> decompositonNFS = Q.intersection(columns, nullFreeSubscheme);
			Map<Set<TableColumn>, Set<TableColumn>> originalFDs = t.getSchema().getFunctionalDependencies();
			// set the updated FDs
			t.getSchema().setFunctionalDependencies(fds.getFunctionalDependencies());
			Map<Integer, Integer> columnProjection = t.projectColumnIndices(columns);
			Table nf = t.project(columns, true);

			// project the null-free subscheme
			// decompositonNFS = new HashSet<>(Q.project(decompositonNFS, (c)->nf.getSchema().get(columnProjection.get(c.getColumnIndex()))));

			// re-create the canonical cover
			// nf.getSchema().setFunctionalDependencies(OpenWorldFunctionalDependencyUtils.canonicalCover(nf.getSchema().getFunctionalDependencies(),decompositonNFS));
			// re-set the dependencies in t
			t.getSchema().setFunctionalDependencies(originalFDs);

			// give it a new name
			nf.setPath(Integer.toString(nextTableId));
			nf.setTableId(nextTableId++);
			logData.append(String.format("\tTable #%d: '%s' in 3NF\n", nf.getTableId(), StringUtils.join(Q.project(nf.getColumns(), headerP), ",")));

			// set the candidate keys
			nf.getSchema().setCandidateKeys(OpenWorldFunctionalDependencyUtils.listCandidateKeys(nf,decompositonNFS));
			for(Set<TableColumn> key : nf.getSchema().getCandidateKeys()) {
				logData.append(String.format("\t\tSynthesized candidate key: {%s}\n", StringUtils.join(Q.project(key, headerP), ",")));
			}

			// deduplicate it
			Set<TableColumn> dedupKey = Q.firstOrDefault(nf.getSchema().getCandidateKeys());
			if(dedupKey==null) {
				dedupKey = new HashSet<>(nf.getColumns());
			}
			int rowsBefore = nf.getSize();
			// nf.deduplicate(Q.project(columns, (c)->nf.getSchema().get(columnProjection.get(c.getColumnIndex()))), ConflictHandling.KeepFirst);
			nf.deduplicate(dedupKey, ConflictHandling.ReplaceNULLs);
			logData.append(String.format("\t\tDeduplication resulted in %d/%d rows\n", nf.getSize(), rowsBefore));


			// add it to the results
			result.add(nf);
		} 
		
		if(result.size()==0) {

			logData.append("\tNo 3NF violations\n");
			// table was already in 3NF
			Table nf = t.project(t.getColumns(), true);
			nf.setTableId(nextTableId);
			nf.setPath(Integer.toString(nextTableId));
			result.add(nf);
			logData.append(String.format("\tTable #%d: '%s' in 3NF\n", nf.getTableId(), StringUtils.join(Q.project(nf.getColumns(), headerP), ",")));

			for(Set<TableColumn> key : nf.getSchema().getCandidateKeys()) {
				logData.append(String.format("\t\tCandidate key: {%s}\n", StringUtils.join(Q.project(key, headerP), ",")));
			}

			// deduplicate it
			Set<TableColumn> dedupKey = Q.firstOrDefault(nf.getSchema().getCandidateKeys());
			if(dedupKey==null) {
				dedupKey = new HashSet<>(nf.getColumns());
			}
			int rowsBefore = nf.getSize();
			// nf.deduplicate(Q.project(columns, (c)->nf.getSchema().get(columnProjection.get(c.getColumnIndex()))), ConflictHandling.KeepFirst);
			nf.deduplicate(dedupKey, ConflictHandling.ReplaceNULLs);
			logData.append(String.format("\t\tDeduplication resulted in %d/%d rows\n", nf.getSize(), rowsBefore));
		} else {
			// check if any of the created tables contains a candidate key of the original table
			if(!hasCandidateKeyDecomposition) {
				logData.append("\tKey-preserving decomposition:\n");
				// if not, create a new table from a candidate key
				Collection<TableColumn> columns = Q.firstOrDefault(t.getSchema().getCandidateKeys());
				Table nf = t.project(columns, true);
				nf.setPath(Integer.toString(nextTableId));
				nf.setTableId(nextTableId++);
				logData.append(String.format("\tTable #%d: '%s' in 3NF\n", nf.getTableId(), StringUtils.join(Q.project(nf.getColumns(), headerP), ",")));				 
				result.add(nf);

				// deduplicate it
				Set<TableColumn> dedupKey = new HashSet<>(nf.getColumns());
				int rowsBefore = nf.getSize();
				// nf.deduplicate(Q.project(columns, (c)->nf.getSchema().get(columnProjection.get(c.getColumnIndex()))), ConflictHandling.KeepFirst);
				nf.deduplicate(dedupKey, ConflictHandling.ReplaceNULLs);
				logData.append(String.format("\t\tDeduplication resulted in %d/%d rows\n", nf.getSize(), rowsBefore));
			}
		}
		
		System.out.println(logData.toString());
		
		return result;
	}

	
}
