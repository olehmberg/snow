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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.StitchedModel;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.ProgressReporter;
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
public class StitchedWebTableNormaliser {

	private Func<Boolean, TableColumn> columnSelector = (c)->true;
	/**
	 * Specifies a function that determines which columns should be ignored during normalisation.
	 * 
	 * @param columnSelector the columnSelector to set
	 */
	public void setColumnSelector(Func<Boolean, TableColumn> columnSelector) {
		this.columnSelector = columnSelector;
	}
	
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
		Set<TableColumn> keyAttributes = new HashSet<>();
		for(Set<TableColumn> key : t.getSchema().getCandidateKeys()) {
			keyAttributes.addAll(key);
		}

		for(Pair<Set<TableColumn>, Set<TableColumn>> fd : FunctionalDependencyUtils.split(Pair.fromMap(t.getSchema().getFunctionalDependencies()))) {
			// check for a violation of 2NF
			if(
				!Q.any(t.getSchema().getCandidateKeys(), (key)->key.containsAll(fd.getSecond()))		// only normalise if the dependant is a non-key attribute
				// if the dependant is in the closure of any proper subset of any candidate key
				&& Q.any(
					t.getSchema().getCandidateKeys(), 
					(key)->Q.any(
						Q.getAllProperSubsets(key), 
						(subset)->FunctionalDependencyUtils.closure(subset, t.getSchema().getFunctionalDependencies()).containsAll(fd.getSecond())))
				&& (!addFullClosure || fd.getFirst().size()!=0)		// if we add the full closure to all decompositions, don't decompose {} -> X (which is added to all decompositions in this case)
			) {

				logData.append(String.format("\t2NF violation: {%s}->{%s}\n", 
					StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","), 
					StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",")));

				remainingAttributes.removeAll(fd.getSecond());

				violations.add(new Pair<>(fd.getFirst(), fd.getSecond()));
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
		} else {
			// create a new table from the remaining attributes of the original table
			Collection<TableColumn> columns = remainingAttributes;
			Table nf = t.project(columns, true);
			nf.setPath(Integer.toString(nextTableId));
			nf.setTableId(nextTableId++);
			logData.append(String.format("\tTable #%d: '%s' in 2NF\n", nf.getTableId(), StringUtils.join(Q.project(nf.getColumns(), (c)->c.getHeader()), ",")));				 
			result.add(nf);
		}

		System.out.println(logData.toString());

		return result;
	}

	/**
	 * Normalises the given table into 3NF.
	 * Requires the table's schema (Table.getSchema().getFunctionalDependencies()) to contain a canonical cover of the tables functional dependencies.
	 * Requires the table's schema (Table.getSchema().getCandidateKeys()) to contain at least one candidate key of the table
	 */
	public Collection<Table> normaliseTo3NF(Table tableToNormalise, int nextTableId, TableColumn foreignKey, boolean addFullClosure, StitchedModel model) throws Exception {
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
		List<Pair<Set<TableColumn>,Set<TableColumn>>> weakViolations = new LinkedList<>();
		// iterate over all functional dependencies
		for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
			Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
			Collection<TableColumn> closure = StitchedFunctionalDependencyUtils.closure(new HashSet<>(det), t.getSchema().getFunctionalDependencies(), model);

			// 3NF decomposition: create a new relation for every FD in the canonical cover
			violations.add(new Pair<>(new HashSet<>(det), new HashSet<>(dep)));
				
			logData.append(String.format("\t3NF violation: {%s}->{%s}\n", StringUtils.join(Q.project(det, headerP), ","), StringUtils.join(Q.project(dep, headerP), ",")));
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

		// Merge equivalent keys
		// for each pair of groups, say H1 and H2, with left sides X and Y, respectively, merge H1 and H2 together if there is a bijection X <-> Y in H+
		List<Pair<Set<TableColumn>, Set<TableColumn>>> violationList = new LinkedList<>(violations);
		Map<Pair<Set<TableColumn>, Set<TableColumn>>,Set<TableColumn>> violationClosures = new HashMap<>();
		new ParallelProcessableCollection<>(violationList).foreach(
			(v1)
			-> {
				Set<TableColumn> v1Closure = StitchedFunctionalDependencyUtils.closure(v1.getFirst(), H.getFunctionalDependencies(), model);
				synchronized(violationClosures) {
					violationClosures.put(v1,v1Closure);
				}
			}
		);
		
		Map<Set<TableColumn>, Collection<Pair<Set<TableColumn>, Set<TableColumn>>>> grouped = new HashMap<>();
		while(violationList.size()>0) {
			Pair<Set<TableColumn>, Set<TableColumn>> H1 = violationList.get(0);
			Set<TableColumn> H1Closure = violationClosures.get(H1);

			logData.append(String.format("H1: {%s} with closure {%s}\n",
				StringUtils.join(Q.project(H1.getFirst(), (c)->c.getHeader()), ","),
				StringUtils.join(Q.project(H1Closure, (c)->c.getHeader()), ",")
			));

			Collection<Pair<Set<TableColumn>, Set<TableColumn>>> group = new HashSet<>();
			group.add(H1);
			for(int j=1; j<violationList.size(); j++) {
				Pair<Set<TableColumn>, Set<TableColumn>> H2 = violationList.get(j);
				Set<TableColumn> H2Closure = violationClosures.get(H2);

				// check if there is a bijection
				if(H1Closure.containsAll(H2Closure) && H2Closure.containsAll(H1Closure)) {
					group.add(H2);
				}
			}
			grouped.put(H1.getFirst(), group);
			violationList.removeAll(group);
		}

		Set<Set<Set<TableColumn>>> groups = new HashSet<>();
		for(Set<TableColumn> closure : grouped.keySet()) {
			Set<Set<TableColumn>> merged = new HashSet<>();
			for(Pair<Set<TableColumn>, Set<TableColumn>> v : grouped.get(closure)) {
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
		
		System.out.println("Finding H'");
		// find an H' subset of H such that (H' + J)+ = (H + J)+ and no proper subset of H' has this property
		FunctionalDependencySet HPrime = new FunctionalDependencySet(H.getFunctionalDependencies());
		HPrime.setTableId("H'");
		FunctionalDependencySet HplusJ = new FunctionalDependencySet(new HashMap<>());
		HplusJ.add(H);
		HplusJ.add(J);
		Collection<Pair<Set<TableColumn>,Set<TableColumn>>> splitHPrime = FunctionalDependencyUtils.split(Pair.fromMap(HPrime.getFunctionalDependencies()));
		System.out.println(String.format("Finding H' for %d FDs", splitHPrime.size()));
		ProgressReporter prg = new ProgressReporter(splitHPrime.size(), "Finding H'");
		for(Pair<Set<TableColumn>,Set<TableColumn>> fdToCheck : splitHPrime) {
			FunctionalDependencySet newHPrime = new FunctionalDependencySet(H.getFunctionalDependencies());
			newHPrime.removeFunctionalDependency(fdToCheck);
			FunctionalDependencySet newHPrimePlusJ = new FunctionalDependencySet(newHPrime.getFunctionalDependencies());
			newHPrimePlusJ.add(J);

			if(FunctionalDependencyUtils.areEquivalentSets(newHPrimePlusJ.getFunctionalDependencies(), HplusJ.getFunctionalDependencies())) {
				HPrime = newHPrime;
			}
			prg.incrementProgress();
			prg.report();
		}

		// create the decompositions
		Map<Set<Set<TableColumn>>, Set<TableColumn>> groupSchema = new HashMap<>();
		Map<Set<Set<TableColumn>>, FunctionalDependencySet> groupFDs = new HashMap<>();
		Map<Set<Set<TableColumn>>, Set<Set<TableColumn>>> groupKeys = new HashMap<>();
		for(Set<Set<TableColumn>> group : groups) {
			FunctionalDependencySet fds = new FunctionalDependencySet(new HashMap<>());
			Set<TableColumn> columns = new HashSet<>();
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

			// generate the candidate keys before we add the weak FDs
			groupKeys.put(group, StitchedFunctionalDependencyUtils.listCandidateKeys(fds.projectFunctionalDependencies(columns), columns, model, false));

			groupSchema.put(group, columns);
			groupFDs.put(group, fds);
		}

		// System.out.println(H.formatFunctionalDependencies());		
		// System.out.println(J.formatFunctionalDependencies());		
		// System.out.println(HPrime.formatFunctionalDependencies());

		// for each group, construct a relation consisting of all the attributes appearing in that group
		for(Set<Set<TableColumn>> group : groups) {						
			Collection<TableColumn> columns = groupSchema.get(group);
			FunctionalDependencySet fds = groupFDs.get(group);
			
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
			nf.getSchema().getCandidateKeys().clear();
			for(Set<TableColumn> key : groupKeys.get(group)) {
				Set<TableColumn> projectedKey = new HashSet<>(Q.project(key, (c)->nf.getSchema().get(columnProjection.get(c.getColumnIndex()))));
				nf.getSchema().getCandidateKeys().add(projectedKey);
			}
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
				Collection<TableColumn> columns = Q.firstOrDefault(t.getSchema().getCandidateKeys());
				Table nf = t.project(columns, true);
				nf.setPath(Integer.toString(nextTableId));
				nf.setTableId(nextTableId++);
				logData.append(String.format("\tTable #%d: '%s' in 3NF\n", nf.getTableId(), StringUtils.join(Q.project(nf.getColumns(), headerP), ",")));				 
				result.add(nf);

				// deduplicate it
				Set<TableColumn> dedupKey = new HashSet<>(nf.getColumns());
				int rowsBefore = nf.getSize();
				nf.deduplicate(dedupKey, ConflictHandling.ReplaceNULLs);
				logData.append(String.format("\t\tDeduplication resulted in %d/%d rows\n", nf.getSize(), rowsBefore));
			}
		}
		
		System.out.println(logData.toString());
		
		return result;
	}

	/**
	 * Normalises the given table into BCNF.
	 * Requires the table's schema (Table.getSchema().getFunctionalDependencies()) to contain a canonical cover of the tables functional dependencies.
	 * Accepts a heuristic to determine in which order BCNF violations are chosen.
	 */
	public Collection<Table> normaliseToBCNF(Table tableToNormalise, int nextTableId, TableColumn foreignKey, StitchedModel model, FDScorer violationSelector) throws Exception {
		/***********************************************
    	 * Normalisation to BCNF
    	 ***********************************************/
		
		Collection<Table> result = new LinkedList<>();
		
		Func<String, TableColumn> headerP = new TableColumn.ColumnHeaderProjection();
		
		StringBuilder logData = new StringBuilder();


		Map<Set<TableColumn>, Set<TableColumn>> allClosures = StitchedFunctionalDependencyUtils.allClosures(tableToNormalise.getSchema().getFunctionalDependencies(), model);

		// remove trivial dependants & independent attributes
		Set<TableColumn> independent = tableToNormalise.getSchema().getFunctionalDependencies().get(new HashSet<>());
		if(independent==null) {
			independent = new HashSet<>();
		}
		for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(allClosures)) {
			fd.getSecond().removeAll(fd.getFirst());
			fd.getSecond().removeAll(independent);
			allClosures.put(fd.getFirst(), fd.getSecond());
		}

		// keep a queue of tables (decompositions) and their attributes in the original table (which are referenced by the FDs)
		Queue<Pair<Table,Set<TableColumn>>> tablesToNormalise = new LinkedList<>();
		tablesToNormalise.add(new Pair<>(tableToNormalise, new HashSet<>(Q.without(tableToNormalise.getColumns(), independent))));

		while(tablesToNormalise.size()>0) {
			Pair<Table, Set<TableColumn>> p = tablesToNormalise.poll();
			Table t = p.getFirst();
			Set<TableColumn> attributes = p.getSecond();

			logData.append(String.format("Table #%d '%s' with schema '%s'\n", t.getTableId(), t.getPath(), StringUtils.join(Q.project(attributes, headerP), ",")));
			
			List<Pair<Set<TableColumn>,Set<TableColumn>>> violations = new LinkedList<>();
			// iterate over all functional dependencies

			Set<TableColumn> primeAttributes = new HashSet<>();
			Set<Set<TableColumn>> candidateKeys = new HashSet<>();
			
			for(Set<TableColumn> det : allClosures.keySet()) {
				 // if a foreign key was specified, only consider determinants that contain the foreign key
				 if(foreignKey==null || det.contains(foreignKey)
				 	&& attributes.containsAll(det)
				 ) {
				 
					// Set<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
					Set<TableColumn> dep = allClosures.get(det);
					dep = new HashSet<>(Q.intersection(dep, attributes));
					if(dep.size()>0) {
						// dep is taken from the extended FDs, which already contain the closure
						if(!dep.containsAll(Q.without(attributes, det))) {
							violations.add(new Pair<>(det, dep));
							//  logData.append(String.format("\tBCNF violation: {%s}->{%s} [Closure: {%s}]\n", StringUtils.join(Q.project(det, headerP), ","), StringUtils.join(Q.project(t.getSchema().getFunctionalDependencies().get(det), headerP), ","), StringUtils.join(Q.project(closure, headerP), ",")));
						} else {
							logData.append(String.format("No violation: {%s}->{%s}\n",
								StringUtils.join(Q.project(det, (c)->c.getHeader()), ","),
								StringUtils.join(Q.project(dep, (c)->c.getHeader()), ",")
							));
							primeAttributes.addAll(det);
							candidateKeys.add(det);
						}
					}
				 }
			 }
			 logData.append(String.format("Prime attributes: {%s}\n",
			 	StringUtils.join(Q.project(primeAttributes, (c)->c.getHeader()), ",")
			 ));
			 for(Set<TableColumn> key : candidateKeys) {
				logData.append(String.format("Candidate Key: {%s}\n",
					StringUtils.join(Q.project(key, (c)->c.getHeader()), ",")
				));
			 }

			List<Pair<Set<TableColumn>,Set<TableColumn>>> filteredViolations = new LinkedList<>();

			for(Pair<Set<TableColumn>, Set<TableColumn>> violation : violations) {
					// the violation must be a proper subset of a candidate key (2NF)
					if(candidateKeys.size()>0 && !Q.any(candidateKeys, (key)->key.containsAll(violation.getFirst()) && key.size()>violation.getFirst().size())) {
						
					} else {
						Set<TableColumn> primeInDep = Q.intersection(violation.getSecond(), primeAttributes);
						// the violation must not split a candidate key
						if(primeInDep.size()>0) {
							logData.append(String.format("\tprime attributes {%s} in violation {%s}->{%s}\n",
								StringUtils.join(Q.project(primeInDep, (c)->c.getHeader()), ","),
								StringUtils.join(Q.project(violation.getFirst(), (c)->c.getHeader()), ","),
								StringUtils.join(Q.project(violation.getSecond(), (c)->c.getHeader()), ",")
							));

							Set<TableColumn> reducedDet = new HashSet<>(Q.without(violation.getSecond(), primeAttributes));
							logData.append(String.format("\t\tviolation {%s}->{%s}\n",
								StringUtils.join(Q.project(violation.getFirst(), (c)->c.getHeader()), ","),
								StringUtils.join(Q.project(reducedDet, (c)->c.getHeader()), ",")
							));
							if(reducedDet.size()>0) {
								filteredViolations.add(new Pair<>(violation.getFirst(), reducedDet));
							}
						} else {
							logData.append(String.format("\t\tviolation {%s}->{%s}\n",
								StringUtils.join(Q.project(violation.getFirst(), (c)->c.getHeader()), ","),
								StringUtils.join(Q.project(violation.getSecond(), (c)->c.getHeader()), ",")
							));
							filteredViolations.add(violation);
						}
					}
			}
			violations = filteredViolations;
			 if(violations.size()>0) {
				 // choose a violating functional dependency for decomposition
				 List<Pair<Pair<Set<TableColumn>, Set<TableColumn>>, Double>> rankedViolations = violationSelector.scoreFunctionalDependenciesForDecomposition(violations, tableToNormalise, model);
				 logData.append(violationSelector.formatScores());

				Pair<Set<TableColumn>,Set<TableColumn>> violation = Q.firstOrDefault(rankedViolations).getFirst();
				 Collection<TableColumn> determinant = violation.getFirst();
				 Collection<TableColumn> dependant = violation.getSecond();
				
				Set<TableColumn> remainingColumns = new HashSet<>(attributes);
				remainingColumns.removeAll(dependant);
				 
				 logData.append(String.format("\tChoose {%s}->{%s} for decomposition with remainder {%s}\n", 
					 StringUtils.join(Q.project(determinant, headerP), ","), 
					 StringUtils.join(Q.project(dependant, headerP), ","), 
					 StringUtils.join(Q.project(remainingColumns, headerP), ",")));
				 
				 Collection<TableColumn> bcnfColumns = Q.sort(Q.union(determinant, dependant), new TableColumn.TableColumnByIndexComparator());
				
				Table bcnf = tableToNormalise.project(bcnfColumns, true);
				 Map<Integer, Integer> columnProjection = tableToNormalise.projectColumnIndices(bcnfColumns);

				 bcnf.setPath(Integer.toString(nextTableId));
				 bcnf.setTableId(nextTableId++);
				 logData.append(String.format("\tDecomposition: Table #%d: '%s'\n", bcnf.getTableId(), StringUtils.join(Q.project(bcnf.getColumns(), headerP), ",")));

				 int rowsBefore = bcnf.getSize();
				 Set<TableColumn> key = new HashSet<>(Q.project(determinant, (c)->bcnf.getSchema().get(columnProjection.get(c.getColumnIndex()))));
				 bcnf.getSchema().getCandidateKeys().add(key);
				 bcnf.deduplicate(key, ConflictHandling.ReplaceNULLs);
				 logData.append(String.format("\tDeduplication resulted in %d/%d rows\n", bcnf.getSize(), rowsBefore));

				 tablesToNormalise.add(new Pair<>(bcnf, new HashSet<>(Q.union(determinant, dependant))));

				 Table rest = tableToNormalise.project(remainingColumns, true);
				 Map<Integer, Integer> columnProjectionRest = tableToNormalise.projectColumnIndices(remainingColumns);
				 rest.setPath(Integer.toString(nextTableId));
				 rest.setTableId(nextTableId++);
				 rest.getSchema().getCandidateKeys().clear();
				 for(Set<TableColumn> cand : candidateKeys) {
					rest.getSchema().getCandidateKeys().add(new HashSet<>(Q.project(cand, (c)->rest.getSchema().get(columnProjectionRest.get(c.getColumnIndex())))));
				 }
				 logData.append(String.format("\tDecomposition: Table #%d: '%s'\n", rest.getTableId(), StringUtils.join(Q.project(rest.getColumns(), headerP), ",")));
				 tablesToNormalise.add(new Pair<>(rest, remainingColumns));
				 
			 } else {
				 // table is in BCNF
				 
				 // we change the table id, so it's a good idea to add provenance information for the old table id
				 if(t==tableToNormalise) {
					 Table bcnf = tableToNormalise.project(attributes, true);
					 Map<Integer, Integer> columnProjection = tableToNormalise.projectColumnIndices(attributes);
					 bcnf.setPath(Integer.toString(nextTableId));
					 bcnf.setTableId(nextTableId++);

					 bcnf.getSchema().getCandidateKeys().clear();
					for(Set<TableColumn> key : candidateKeys) {
						bcnf.getSchema().getCandidateKeys().add(new HashSet<>(Q.project(key, (c)->bcnf.getSchema().get(columnProjection.get(c.getColumnIndex())))));
					}

					t = bcnf;
				 } 

				for(Set<TableColumn> key : t.getSchema().getCandidateKeys()) {
					logData.append(String.format("\t\tCandidate key {%s}\n",
						StringUtils.join(Q.project(key, (c)->c.getHeader()), ",")
					));
				}

				if(t.getSchema().getCandidateKeys().size()==0) {
					t.getSchema().getCandidateKeys().add(new HashSet<>(t.getColumns()));
				}

				Set<TableColumn> selectedKey = Q.firstOrDefault(t.getSchema().getCandidateKeys());
				int rowsBefore = t.getSize();
				t.deduplicate(selectedKey, ConflictHandling.ReplaceNULLs);
				logData.append(String.format("\tDeduplication with candidate key {%s} resulted in %d/%d rows\n", 
					StringUtils.join(Q.project(selectedKey, (c)->c.getHeader()), ","),
					t.getSize(), 
					rowsBefore
				));

				logData.append(String.format("\t'Table #%d: %s' in BCNF\n", t.getTableId(), StringUtils.join(Q.project(t.getColumns(), headerP), ",")));
				result.add(t);
			 }
		}
		
		System.out.println(logData.toString());
		
		return result;
	}
	
}
