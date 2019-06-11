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
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
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
public class WebTableNormaliser {

	// private Func<Boolean, TableColumn> columnSelector = (c)->true;
	// /**
	//  * Specifies a function that determines which columns should be ignored during normalisation.
	//  * 
	//  * @param columnSelector the columnSelector to set
	//  */
	// public void setColumnSelector(Func<Boolean, TableColumn> columnSelector) {
	// 	this.columnSelector = columnSelector;
	// }
	
	/**
	 * Normalises the given table into 2NF.
	 */
	public Collection<Table> normaliseTo2NF(Table tableToNormalise, int nextTableId, TableColumn foreignKey, boolean addFullClosure) throws Exception {

		Func<String, TableColumn> headerP = new TableColumn.ColumnHeaderProjection();
		StringBuilder logData = new StringBuilder();
		List<Pair<Set<TableColumn>,Set<TableColumn>>> violations = new LinkedList<>();
		Table t = tableToNormalise;
		Collection<Table> result = new LinkedList<>();

		Set<TableColumn> remainingAttributes = new HashSet<>(t.getColumns());

		Set<Set<TableColumn>> keys = null;
		if(foreignKey!=null) {
			keys = new HashSet<>(Q.where(t.getSchema().getCandidateKeys(), (k)->k.contains(foreignKey)));
		} else {
			keys = new HashSet<>(t.getSchema().getCandidateKeys());
		}

		// list all key attributes
		Set<TableColumn> keyAttributes = new HashSet<>();
		for(Set<TableColumn> key : keys) {
			keyAttributes.addAll(key);
		}
		Set<TableColumn> nonKeyAttributes = new HashSet<>(Q.without(t.getColumns(), keyAttributes));

		logData.append(String.format("\tprime attributes: {%s} / non-prime attributes {%s}\n",
			StringUtils.join(Q.project(keyAttributes, (c)->c.getHeader()), ","),
			StringUtils.join(Q.project(nonKeyAttributes, (c)->c.getHeader()), ",")
		));

		Set<Pair<Set<TableColumn>,Set<TableColumn>>> uniqueViolations = new HashSet<>();
		for(Set<TableColumn> key : keys) {
			for(TableColumn attribute : nonKeyAttributes) {
				Pair<Set<TableColumn>,Set<TableColumn>> fdToVerify = new Pair<>(key, Q.toSet(attribute));
				Pair<Set<TableColumn>,Set<TableColumn>> generalised = FunctionalDependencyUtils.reduceLHS(fdToVerify, t.getSchema().getFunctionalDependencies());

				if(!uniqueViolations.contains(generalised)) {
					if(generalised.getFirst().size()<fdToVerify.getFirst().size()) {
						logData.append(String.format("\t2NF violation: {%s}->{%s}\n", 
							StringUtils.join(Q.project(generalised.getFirst(), (c)->c.getHeader()), ","), 
							StringUtils.join(Q.project(generalised.getSecond(), (c)->c.getHeader()), ",")));

						violations.add(generalised);
						uniqueViolations.add(generalised);
						remainingAttributes.removeAll(fdToVerify.getSecond());
					} else {
						logData.append(String.format("\tno generalisation of: {%s}->{%s}\n", 
							StringUtils.join(Q.project(fdToVerify.getFirst(), (c)->c.getHeader()), ","), 
							StringUtils.join(Q.project(fdToVerify.getSecond(), (c)->c.getHeader()), ",")));
					}
				}
			}
		}

		// merge all violations which are bijections:
		// - if two violations X -> V and Y -> W exists 
		// - and the closure of the table to normalise contains X <-> Y
		// - then merge the violations
		Map<Set<TableColumn>, Collection<Pair<Set<TableColumn>, Set<TableColumn>>>> grouped = Q.<Set<TableColumn>,Pair<Set<TableColumn>, Set<TableColumn>>>group(
			violations, 
			(fd)->FunctionalDependencyUtils.closure(fd.getFirst(), t.getSchema().getFunctionalDependencies())
			);
		violations.clear();
		for(Set<TableColumn> closure : grouped.keySet()) {
			Set<TableColumn> determinant = new HashSet<>();
			Set<TableColumn> dependant = new HashSet<>();
			Set<Set<TableColumn>> merged = new HashSet<>();
			for(Pair<Set<TableColumn>, Set<TableColumn>> v : grouped.get(closure)) {
				merged.add(v.getFirst());
				determinant.addAll(v.getFirst());
				dependant.addAll(v.getSecond());
			}
			if(merged.size()>1) {
				logData.append(String.format("\tmerging bijection: {%s}\n",
					StringUtils.join(
						Q.project(merged, (cols)->StringUtils.join(Q.project(cols,(col)->col.getHeader()), ",")),
						"}<->{"
					)
				));
			}
			violations.add(new Pair<>(determinant, dependant));
		}

		// remove violations which are contained in other violations
		List<Pair<Set<TableColumn>, Set<TableColumn>>> filteredViolations = new LinkedList<>();
		for(Pair<Set<TableColumn>, Set<TableColumn>> v : violations) {
			boolean isContainedInOtherViolation = false;
			for(Pair<Set<TableColumn>, Set<TableColumn>> other : violations) {
				if(other!=v) {
					if(Q.union(other.getFirst(), other.getSecond()).containsAll(Q.union(v.getFirst(), v.getSecond()))) {
						logData.append(String.format("\tremoving violation {%s}->{%s}: contained in {%s}->{%s}\n",
							StringUtils.join(Q.project(v.getFirst(), headerP), ","),
							StringUtils.join(Q.project(v.getSecond(), headerP), ","),
							StringUtils.join(Q.project(other.getFirst(), headerP), ","),
							StringUtils.join(Q.project(other.getSecond(), headerP), ",")
						));
						isContainedInOtherViolation = true;
						break;
					}
				}
			}
			if(!isContainedInOtherViolation) {
				filteredViolations.add(v);
			}
		}
		violations = filteredViolations;

		// if an FK was specified, only keep violations which contain the FK in the determinant
		filteredViolations = new LinkedList<>();
		for(Pair<Set<TableColumn>, Set<TableColumn>> v : violations) {
			if(foreignKey==null || v.getFirst().contains(foreignKey)) {
				filteredViolations.add(v);
			}
		}
		violations = filteredViolations;

		for(Pair<Set<TableColumn>,Set<TableColumn>> violation : violations) {
			Collection<TableColumn> determinant = violation.getFirst();
			Collection<TableColumn> dependant = violation.getSecond();
			
			if(addFullClosure) {
				dependant = FunctionalDependencyUtils.closure(violation.getFirst(), t.getSchema().getFunctionalDependencies());
			}

			// create the normalised relation
			Collection<TableColumn> columns = Q.sort(Q.union(determinant, dependant), new TableColumn.TableColumnByIndexComparator());
			// to preserve all depencies in the projected relation (which will contain a copy of the FDs), create the closure of every FD
			// - this way, we preserve dependencies which are only visible via transitivity in the canonical cover
			Map<Set<TableColumn>, Set<TableColumn>> originalFDs = t.getSchema().getFunctionalDependencies();
			t.getSchema().setFunctionalDependencies(FunctionalDependencyUtils.allClosures(originalFDs));
			
			Table nf = t.project(columns, true);
			// re-create the canonical cover
			nf.getSchema().setFunctionalDependencies(FunctionalDependencyUtils.canonicalCover(nf.getSchema().getFunctionalDependencies()));
			// re-set the dependencies in t
			t.getSchema().setFunctionalDependencies(originalFDs);

			// give it a new name
			nf.setPath(Integer.toString(nextTableId));
			nf.setTableId(nextTableId++);
			logData.append(String.format("\tTable #%d: '%s' in 2NF\n", nf.getTableId(), StringUtils.join(Q.project(nf.getColumns(), (c)->c.getHeader()), ",")));

			// set the candidate keys
			nf.getSchema().setCandidateKeys(FunctionalDependencyUtils.listCandidateKeys(nf));
			for(Set<TableColumn> key : nf.getSchema().getCandidateKeys()) {
				logData.append(String.format("\t\tSynthesized candidate key: {%s}\n", StringUtils.join(Q.project(key, headerP), ",")));
			}

			// deduplicate it
			Set<TableColumn> dedupKey = Q.firstOrDefault(nf.getSchema().getCandidateKeys());
			if(dedupKey==null) {
				dedupKey = new HashSet<>(nf.getColumns());
			}
			int rowsBefore = nf.getSize();
			
			nf.deduplicate(dedupKey, ConflictHandling.ReplaceNULLs);
			logData.append(String.format("\t\tDeduplication resulted in %d/%d rows\n", nf.getSize(), rowsBefore));

			// add it to the results
			result.add(nf);
		} 
		
		if(result.size()==0) {
			// table was already in 2NF
			Table nf = t.project(t.getColumns(), true);
			nf.setTableId(nextTableId);
			nf.setPath(Integer.toString(nextTableId));
			result.add(nf);
			logData.append(String.format("\tTable #%d: '%s' in 2NF\n", nf.getTableId(), StringUtils.join(Q.project(nf.getColumns(), (c)->c.getHeader()), ",")));
		} else if(remainingAttributes.size()>0) {
			// create a new table from the remaining attributes of the original table
			Collection<TableColumn> columns = remainingAttributes;
			Table nf = t.project(columns, true);
			nf.setPath(Integer.toString(nextTableId));
			nf.setTableId(nextTableId++);
			logData.append(String.format("\tTable #%d: '%s' in 2NF\n", nf.getTableId(), StringUtils.join(Q.project(nf.getColumns(), (c)->c.getHeader()), ",")));				 
			result.add(nf);

			// set the candidate keys
			nf.getSchema().setCandidateKeys(FunctionalDependencyUtils.listCandidateKeys(nf));
			for(Set<TableColumn> key : nf.getSchema().getCandidateKeys()) {
				logData.append(String.format("\t\tSynthesized candidate key: {%s}\n", StringUtils.join(Q.project(key, headerP), ",")));
			}

			// deduplicate it
			Set<TableColumn> dedupKey = Q.firstOrDefault(nf.getSchema().getCandidateKeys());
			if(dedupKey==null) {
				dedupKey = new HashSet<>(nf.getColumns());
			}
			int rowsBefore = nf.getSize();
			nf.deduplicate(dedupKey, ConflictHandling.ReplaceNULLs);
			logData.append(String.format("\t\tDeduplication resulted in %d/%d rows\n", nf.getSize(), rowsBefore));
		}

		System.out.println(logData.toString());

		return result;
	}

	/**
	 * Normalises the given table into 3NF.
	 * Requires the table's schema (Table.getSchema().getFunctionalDependencies()) to contain a canonical cover of the tables functional dependencies.
	 * Requires the table's schema (Table.getSchema().getCandidateKeys()) to contain at least one candidate key of the table
	 */
	public Collection<Table> normaliseTo3NF(Table tableToNormalise, int nextTableId, TableColumn foreignKey, boolean addFullClosure) throws Exception {
		/***********************************************
    	 * Normalisation to 3NF
    	 ***********************************************/
		
		Collection<Table> result = new LinkedList<>();
		
		Func<String, TableColumn> headerP = new TableColumn.ColumnHeaderProjection();
		
		StringBuilder logData = new StringBuilder();

		Table t = tableToNormalise;

		logData.append(String.format("Table #%d '%s' with schema '%s'\n", t.getTableId(), t.getPath(), StringUtils.join(Q.project(t.getColumns(), headerP), ",")));
		
		boolean hasCandidateKeyDecomposition = false;

		List<Pair<Set<TableColumn>,Set<TableColumn>>> violations = new LinkedList<>();
		// iterate over all functional dependencies
		for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
			Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
			Collection<TableColumn> closure = FunctionalDependencyUtils.closure(new HashSet<>(det), t.getSchema().getFunctionalDependencies());

			// 3NF decomposition: create a new relation for every FD in the canonical cover
			violations.add(new Pair<>(new HashSet<>(det), new HashSet<>(dep)));
				
			logData.append(String.format("\t3NF violation: {%s}->{%s}\n", StringUtils.join(Q.project(det, headerP), ","), StringUtils.join(Q.project(t.getSchema().getFunctionalDependencies().get(det), headerP), ",")));
			if(closure.size()==tableToNormalise.getColumns().size()) {
				hasCandidateKeyDecomposition = true;
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
		Map<Set<TableColumn>, Collection<Pair<Set<TableColumn>, Set<TableColumn>>>> grouped = Q.<Set<TableColumn>,Pair<Set<TableColumn>, Set<TableColumn>>>group(
			violations, 
			(fd)->FunctionalDependencyUtils.closure(fd.getFirst(), t.getSchema().getFunctionalDependencies())
			);
		Set<Set<Set<TableColumn>>> groups = new HashSet<>();
		for(Set<TableColumn> closure : grouped.keySet()) {
			Set<Set<TableColumn>> merged = new HashSet<>();
			for(Pair<Set<TableColumn>, Set<TableColumn>> v : grouped.get(closure)) {
				merged.add(v.getFirst());

				logData.append(String.format("H1: {%s} with closure {%s}\n",
					StringUtils.join(Q.project(v.getFirst(), (c)->c.getHeader()), ","),
					StringUtils.join(Q.project(closure, (c)->c.getHeader()), ",")
				));
			}
			groups.add(merged);

			if(merged.size()>1) {

				logData.append(String.format("\tmerging bijection: {%s}\n",
					StringUtils.join(
						Q.project(merged, (cols)->StringUtils.join(Q.project(cols,(col)->col.getHeader()), ",")),
						"}<->{"
					)
				));

				// eliminate transitive FDs
				List<Pair<Set<TableColumn>, Set<TableColumn>>> mergedGroups = new LinkedList<>(grouped.get(closure));

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

			if(FunctionalDependencyUtils.areEquivalentSets(newHPrimePlusJ.getFunctionalDependencies(), HplusJ.getFunctionalDependencies())) {
				HPrime = newHPrime;
			}
		}

		// System.out.println(H.formatFunctionalDependencies());		
		// System.out.println(J.formatFunctionalDependencies());		
		// System.out.println(HPrime.formatFunctionalDependencies());

		// if an FK was specified, only keep violations which contain the FK
		List<Pair<Set<TableColumn>, Set<TableColumn>>> filteredViolations = new LinkedList<>();
		for(Pair<Set<TableColumn>, Set<TableColumn>> v : violations) {
			if(foreignKey==null || Q.union(v.getFirst(), v.getSecond()).contains(foreignKey)) {
				filteredViolations.add(v);
			}
		}
		violations = filteredViolations;

		// for each group, construct a relation consisting of all the attributes appearing in that group
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
				logData.append(String.format("\tDecomposition for violation {%s}\n", StringUtils.join(Q.project(det, headerP), ",")));
			}
			// add each FD of J into its corresponding group of H'
			fds.add(J);
			
			// create the normalised relation
			columns = Q.sort(columns, new TableColumn.TableColumnByIndexComparator());

			Map<Set<TableColumn>, Set<TableColumn>> originalFDs = t.getSchema().getFunctionalDependencies();

			// set the updated FDs
			t.getSchema().setFunctionalDependencies(fds.getFunctionalDependencies());
			
			Map<Integer, Integer> columnProjection = t.projectColumnIndices(columns);
			Table nf = t.project(columns, true);
			
			// re-set the dependencies in t
			t.getSchema().setFunctionalDependencies(originalFDs);

			// give it a new name
			nf.setPath(Integer.toString(nextTableId));
			nf.setTableId(nextTableId++);
			logData.append(String.format("\tTable #%d: '%s' in 3NF\n", nf.getTableId(), StringUtils.join(Q.project(nf.getColumns(), headerP), ",")));

			// set the candidate keys
			nf.getSchema().setCandidateKeys(FunctionalDependencyUtils.listCandidateKeys(nf));
			for(Set<TableColumn> key : nf.getSchema().getCandidateKeys()) {
				logData.append(String.format("\t\tSynthesized candidate key: {%s}\n", StringUtils.join(Q.project(key, headerP), ",")));
			}

			// deduplicate it
			Set<TableColumn> dedupKey = Q.firstOrDefault(nf.getSchema().getCandidateKeys());
			if(dedupKey==null) {
				dedupKey = new HashSet<>(nf.getColumns());
			}
			int rowsBefore = nf.getSize();
			
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
			nf.deduplicate(dedupKey, ConflictHandling.ReplaceNULLs);
			logData.append(String.format("\t\tDeduplication resulted in %d/%d rows\n", nf.getSize(), rowsBefore));
		} else {
			// check if any of the created tables contains a candidate key of the original table
			if(!hasCandidateKeyDecomposition) {
				logData.append("\tKey-preserving decomposition:\n");
				// if not, create a new table from a candidate key

				// to preserve all depencies in the projected relation (which will contain a copy of the FDs), create the closure of every FD
				// - this way, we preserve dependencies which are only visible via transitivity in the canonical cover and use columns which are not in the projection
				Map<Set<TableColumn>, Set<TableColumn>> originalFDs = t.getSchema().getFunctionalDependencies();
				t.getSchema().setFunctionalDependencies(FunctionalDependencyUtils.allClosures(originalFDs));

				Collection<TableColumn> columns = Q.firstOrDefault(t.getSchema().getCandidateKeys());
				Table nf = t.project(columns, true);
				nf.setPath(Integer.toString(nextTableId));
				nf.setTableId(nextTableId++);
				logData.append(String.format("\tTable #%d: '%s' in 3NF\n", nf.getTableId(), StringUtils.join(Q.project(nf.getColumns(), headerP), ",")));				 
				result.add(nf);

				// re-create the canonical cover
				nf.getSchema().setFunctionalDependencies(FunctionalDependencyUtils.canonicalCover(nf.getSchema().getFunctionalDependencies()));
				// re-set the dependencies in t
				t.getSchema().setFunctionalDependencies(originalFDs);

				nf.getSchema().setCandidateKeys(FunctionalDependencyUtils.listCandidateKeys(nf));

				for(Set<TableColumn> key : nf.getSchema().getCandidateKeys()) {
					logData.append(String.format("\t\tCandidate key: {%s}\n", StringUtils.join(Q.project(key, headerP), ",")));
				}
	
				// deduplicate it
				Set<TableColumn> dedupKey = Q.firstOrDefault(nf.getSchema().getCandidateKeys());
				if(dedupKey==null) {
					dedupKey = new HashSet<>(nf.getColumns());
				}
				int rowsBefore = nf.getSize();
				
				nf.deduplicate(dedupKey, ConflictHandling.ReplaceNULLs);
				logData.append(String.format("\t\tDeduplication resulted in %d/%d rows\n", nf.getSize(), rowsBefore));
			}
		}
		
		System.out.println(logData.toString());
		
		return result;
	}

	/**
	 * Normalises the given table into 3NF.
	 * Requires the table's schema (Table.getSchema().getFunctionalDependencies()) to contain a canonical cover of the tables functional dependencies.
	 */
	public Collection<Table> normaliseToBCNF(Table tableToNormalise, int nextTableId, TableColumn foreignKey) throws Exception {
		return normaliseToBCNF(tableToNormalise, nextTableId, foreignKey, new Comparator<Pair<Collection<TableColumn>,Collection<TableColumn>>>() {

					@Override
					public int compare(Pair<Collection<TableColumn>,Collection<TableColumn>> o1, Pair<Collection<TableColumn>,Collection<TableColumn>> o2) {
						return Integer.compare(o1.getFirst().size(), o2.getFirst().size());
					}
				});
	}

	/**
	 * Normalises the given table into 3NF.
	 * Requires the table's schema (Table.getSchema().getFunctionalDependencies()) to contain a canonical cover of the tables functional dependencies.
	 * Accepts a heuristic to determine in which order BCNF violations are chosen.
	 */
	public Collection<Table> normaliseToBCNF(Table tableToNormalise, int nextTableId, TableColumn foreignKey, Comparator<Pair<Collection<TableColumn>,Collection<TableColumn>>> violationSelector) throws Exception {
		/***********************************************
    	 * Normalisation to BCNF
    	 ***********************************************/
		
		Collection<Table> result = new LinkedList<>();
		
		Func<String, TableColumn> headerP = new TableColumn.ColumnHeaderProjection();
		
		StringBuilder logData = new StringBuilder();

		Table t = tableToNormalise;
		
		while(t!=null) {
			logData.append(String.format("Table #%d '%s' with schema '%s'\n", t.getTableId(), t.getPath(), StringUtils.join(Q.project(t.getColumns(), headerP), ",")));
			
			List<Pair<Collection<TableColumn>,Collection<TableColumn>>> violations = new LinkedList<>();
			// iterate over all functional dependencies
			 for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {

				 // if a foreign key was specified, only consider determinants that contain the foreign key
				 if(foreignKey==null || det.contains(foreignKey)) {
				 
					Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
					Collection<TableColumn> closure = FunctionalDependencyUtils.closure(new HashSet<>(det), t.getSchema().getFunctionalDependencies());
					 

					// check if the determinant is a candidate key
					if(closure.size()<t.getColumns().size()) {
						violations.add(new Pair<>(det, dep));
					}
				 }
			 }
			 if(violations.size()>0) {
				 // choose a violating functional dependency for decomposition
				 
				 Collections.sort(violations, violationSelector);
				 for(Pair<Collection<TableColumn>, Collection<TableColumn>> p : violations) {
					Collection<TableColumn> det = p.getFirst();
					// Collection<TableColumn> dep = p.getSecond();
					logData.append(String.format("\tBCNF violation: {%s}->{%s}\n", StringUtils.join(Q.project(det, headerP), ","), StringUtils.join(Q.project(t.getSchema().getFunctionalDependencies().get(det), headerP), ",")));
				 }
				 Pair<Collection<TableColumn>,Collection<TableColumn>> violation = Q.firstOrDefault(violations); 
				 Collection<TableColumn> determinant = violation.getFirst();
				 Collection<TableColumn> dependant = violation.getSecond();
				 Collection<TableColumn> missing = Q.without(t.getColumns(), dependant);
				 
				 logData.append(String.format("\tChoose {%s}->{%s} for decomposition with remainder {%s}\n", StringUtils.join(Q.project(determinant, headerP), ","), StringUtils.join(Q.project(dependant, headerP), ","), StringUtils.join(Q.project(missing, headerP), ",")));
				 
				 // only add provenance in the first projection (add the provenance of tableToNormalise)
				 Collection<TableColumn> bcnfColumns = Q.sort(Q.union(determinant, dependant), new TableColumn.TableColumnByIndexComparator());
				 Table bcnf = t.project(bcnfColumns, t==tableToNormalise);
				 Map<Integer, Integer> columnProjection = t.projectColumnIndices(bcnfColumns);
				 int rowsBefore = bcnf.getSize();
				 bcnf.deduplicate(Q.project(bcnfColumns, (c)->bcnf.getSchema().get(columnProjection.get(c.getColumnIndex()))), ConflictHandling.KeepFirst);
				 logData.append(String.format("\tDeduplication resulted in %d/%d rows\n", bcnf.getSize(), rowsBefore));
				 bcnf.setPath(Integer.toString(nextTableId));
				 bcnf.setTableId(nextTableId++);
				 logData.append(String.format("\tTable #%d: '%s' in BCNF with entity label '%s'\n", bcnf.getTableId(), StringUtils.join(Q.project(bcnf.getColumns(), headerP), ","), bcnf.getSubjectColumnIndex()==-1?"?":bcnf.getSubjectColumn().getHeader()));

				 Table rest = t.project(Q.sort(Q.union(determinant, missing), new TableColumn.TableColumnByIndexComparator()), t==tableToNormalise);
				 
				 result.add(bcnf);

				 t = rest;
				 
			 } else {
				 // table is in BCNF
				 
				 // we change the table id, so it's a good idea to add provenance information for the old table id
				 if(t==tableToNormalise) {
					 t = t.project(t.getColumns(), true);
				 } 
				 result.add(t);
				 t.setTableId(nextTableId);
				 t.setPath(Integer.toString(nextTableId));
				 
				 logData.append(String.format("\t'Table #%d: %s' in BCNF with entity label '%s'\n", t.getTableId(), StringUtils.join(Q.project(t.getColumns(), headerP), ","), t.getSubjectColumnIndex()==-1?"?":t.getSubjectColumn().getHeader()));

				 t = null;
			 }
		}
		
		System.out.println(logData.toString());
		
		return result;
	}
	
}
