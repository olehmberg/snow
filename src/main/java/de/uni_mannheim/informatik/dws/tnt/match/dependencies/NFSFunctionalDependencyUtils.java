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
package de.uni_mannheim.informatik.dws.tnt.match.dependencies;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * Utility class for functional dependencies with nulls and null-free subschemes using the null = no information interpretation.
 * 
 * A null-free subscheme is a set of attributes which are not allowed to have null values.
 * 
 *  uses the closure algorithm for NFDs with null-free subschemes from: Atzeni, Paolo, and Nicola M. Morfuni. "Functional dependencies and constraints on null values in database relations." Information and Control 70.1 (1986): 1-31.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class NFSFunctionalDependencyUtils {

	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, Set<TableColumn> nullFreeSubscheme) {
		return closure(forColumns, Pair.fromMap(functionalDependencies), nullFreeSubscheme, false);
	}

	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> functionalDependencies, Set<TableColumn> nullFreeSubscheme) {
		return closure(forColumns, functionalDependencies, nullFreeSubscheme, false);
	}

	/**
	 * Calculates the closure of the given subset of columns under the given set of functional dependencies with null and the given null-free subscheme.

	 * 
	 * from: Atzeni, Paolo, and Nicola M. Morfuni. "Functional dependencies and constraints on null values in database relations." Information and Control 70.1 (1986): 1-31.
	 * 
	 * @param forColumns the subset of columns
	 * @param functionalDependencies the set of functional dependencies
	 * @return Returns a collection of all columns that are in the calculated closure (and are hence determined by the given subset of columns)
	 */
	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> functionalDependencies, Set<TableColumn> nullFreeSubscheme, boolean log) {
		
		Set<TableColumn> result = new HashSet<>();
		
		List<Pair<Collection<TableColumn>, Collection<TableColumn>>> trace = new LinkedList<>();

		if(functionalDependencies==null) {
			result = null;
		} else {
			result.addAll(forColumns);
			trace.add(new Pair<>(forColumns, null));
			
			int lastCount;
			do {
				lastCount = result.size();
			
				for(Pair<Set<TableColumn>,Set<TableColumn>> fd : functionalDependencies) {
					Collection<TableColumn> determinant = fd.getFirst();

					if(Q.intersection(result, nullFreeSubscheme).containsAll(determinant)) {
						Collection<TableColumn> dependant = fd.getSecond();
			
						Collection<TableColumn> newColumns = Q.without(dependant, result);

						result.addAll(dependant);

						
						if(newColumns.size()>0) {
							trace.add(new Pair<>(determinant, newColumns));
						}
					}
					
				}
				
			} while(result.size()!=lastCount);

			for(Pair<Set<TableColumn>,Set<TableColumn>> fd : functionalDependencies) {
				Collection<TableColumn> determinant = fd.getFirst();

				if(forColumns.containsAll(determinant)) {
					Collection<TableColumn> dependant = fd.getSecond();
			
					Collection<TableColumn> newColumns = Q.without(dependant, result);

					result.addAll(dependant);

					
					if(newColumns.size()>0) {
						trace.add(new Pair<>(determinant, newColumns));
					}
				}
			}
		}
		
		if(log) {
			StringBuilder sb = new StringBuilder();
			for(Pair<Collection<TableColumn>,Collection<TableColumn>> step : trace) {
				if(step.getSecond()==null) {
					sb.append(String.format("{%s}", StringUtils.join(Q.project(step.getFirst(), (c)->c.toString()), ",")));
				} else {
					sb.append(String.format(" / {%s}->{%s}", 
						StringUtils.join(Q.project(step.getFirst(), (c)->c.toString()), ","),
						StringUtils.join(Q.project(step.getSecond(), (c)->c.toString()), ",")
					));
				}
			}
			System.out.println(String.format("[FunctionalDepdencyUtils.closure] %s", sb.toString()));
		}

		return result;
	}


	public static Map<Set<TableColumn>, Set<TableColumn>> canonicalCover(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, Set<TableColumn> nullFreeSubscheme) {
		return canonicalCover(functionalDependencies, nullFreeSubscheme, false);
	}

	public static Map<Set<TableColumn>, Set<TableColumn>> canonicalCover(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, Set<TableColumn> nullFreeSubscheme, boolean verbose) {
		// create a deep copy of the functional dependencies, otherwise we would change the input data when modifying the sets!
		Map<Set<TableColumn>, Set<TableColumn>> copy = FunctionalDependencyUtils.copy(functionalDependencies);

		Collection<Pair<Set<TableColumn>, Set<TableColumn>>> fds = Pair.fromMap(copy);

		Iterator<Pair<Set<TableColumn>,Set<TableColumn>>> fdIt = fds.iterator();
		while(fdIt.hasNext()) {
			Pair<Set<TableColumn>, Set<TableColumn>> fd = fdIt.next();

			// Linksreduktion
			Iterator<TableColumn> it = fd.getFirst().iterator();
			while(it.hasNext()) {
				// choose an element of the LHS
				TableColumn candidate = it.next();

				// remove it
				Set<TableColumn> reduced = new HashSet<>(fd.getFirst());
				reduced.remove(candidate);

				if(verbose) System.out.println(String.format("[FunctionalDependencyUtils.canonicalCover] (Linksreduktion) Testing for {%s}->{%s} if {%s}+ -> {%s}",
					StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
					StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
					StringUtils.join(Q.project(reduced, (c)->c.getHeader()), ","),
					StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",")
				));

				// if the closure of the remaining LHS still contains the RHS, the removed element was redundant
				// if(closure(reduced, functionalDependencies).containsAll(fd.getSecond())) {
				if(closure(reduced, fds, nullFreeSubscheme, verbose).containsAll(fd.getSecond())) {
					it.remove();

					if(verbose) System.out.println(String.format("[FunctionalDependencyUtils.canonicalCover] (Linksreduktion) \t{%s}+ -> {%s}, reducing to {%s}->{%s}",
						StringUtils.join(Q.project(reduced, (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(reduced, (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",")
					));
				}
			}
		}

		fdIt = fds.iterator();
		while(fdIt.hasNext()) {
			Pair<Set<TableColumn>, Set<TableColumn>> fd = fdIt.next();

			// Rechtsreduktion
			Iterator<TableColumn> it = fd.getSecond().iterator();
			while(it.hasNext()) {
				// choose an element of the RHS
				TableColumn candidate = it.next();

				// remove it
				Set<TableColumn> reduced = new HashSet<>(fd.getSecond());
				reduced.remove(candidate);

				// create a new set of FDs
				Collection<Pair<Set<TableColumn>, Set<TableColumn>>> reducedFDs = new HashSet<>(fds);
				reducedFDs.remove(fd);
				reducedFDs.add(new Pair<>(fd.getFirst(), reduced));

				if(verbose) System.out.println(String.format("[FunctionalDependencyUtils.canonicalCover] (Rechtsreduktion) Testing for {%s}->{%s} if ({%s}-> {%s})+ -> %s",
					StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
					StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
					StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
					StringUtils.join(Q.project(reduced, (c)->c.getHeader()), ","),
					candidate.getHeader()
				));

				// if the closure of the LHS w.r.t. to the new set of FDs still contains the removed element, it was redundant
				if(closure(fd.getFirst(), reducedFDs, nullFreeSubscheme, verbose).contains(candidate)) {
					it.remove();

					if(verbose) System.out.println(String.format("[FunctionalDependencyUtils.canonicalCover] (Rechtsreduktion) \t({%s}-> {%s})+ -> %s, reducing to {%s}->{%s}",
						StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(reduced, (c)->c.getHeader()), ","),
						candidate.getHeader(),
						StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(reduced, (c)->c.getHeader()), ",")
					));
				}
			}
		}

		fdIt = fds.iterator();
		while(fdIt.hasNext()) {
			Pair<Set<TableColumn>, Set<TableColumn>> fd = fdIt.next();

			// Leere Klauseln
			if(fd.getSecond().size()==0) {
				// remove all FDs with an empty RHS
				fdIt.remove();
			}
		}

		Map<Set<TableColumn>, Set<TableColumn>> result = new HashMap<>();

		// Zusammenfassen
		// group all FDs by their LHS
		Map<Set<TableColumn>, Collection<Pair<Set<TableColumn>, Set<TableColumn>>>> groupedByLHS = Q.group(fds, (p)->p.getFirst());
		for(Set<TableColumn> lhs : groupedByLHS.keySet()) {
			// merge the RHSs
			Collection<Pair<Set<TableColumn>, Set<TableColumn>>> allRHS = groupedByLHS.get(lhs);
			Set<TableColumn> combinedRHS = Q.union(Q.project(allRHS, (p)->p.getSecond()));
			result.put(lhs, combinedRHS);

			if(verbose) System.out.println(String.format("[FunctionalDependencyUtils.canonicalCover] (Zusammenfassen) merging %s",
				StringUtils.join(
					Q.project(allRHS, (fd)->String.format("{%s}->{%s}",
						StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),	
						StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",")
						)
					),
					" + ")
				));
		}

		return result;
	}


	public static Set<TableColumn> minimise(Set<TableColumn> attributes, Table t, Set<TableColumn> nullFreeSubscheme) {
		Set<TableColumn> result = new HashSet<>(attributes);
		Iterator<TableColumn> it = result.iterator();

		while(it.hasNext()) {
			TableColumn candidate = it.next();
			Set<TableColumn> reduced = new HashSet<>(result);
			reduced.remove(candidate);
			if(closure(reduced, t.getSchema().getFunctionalDependencies(), nullFreeSubscheme).size()==t.getColumns().size()) {
				it.remove();
			}
		}

		return result;
	}

	public static Set<Set<TableColumn>> listCandidateKeys(Table t, Set<TableColumn> nullFreeSubscheme) {
		
		List<Set<TableColumn>> candidates = new LinkedList<>();

		Set<TableColumn> firstKey = minimise(new HashSet<>(t.getColumns()), t, nullFreeSubscheme);

		candidates.add(firstKey);

		int known = 1;
		int current = 0;

		while(current < known) {
			for(Set<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
				if(det.size()>0) {
					Set<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
					Set<TableColumn> key = Q.union(det, Q.without(candidates.get(current), dep));

					boolean superKey = Q.any(candidates, (k)->key.containsAll(k));

					if(!superKey) {
						Set<TableColumn> newKey = minimise(key, t, nullFreeSubscheme);
						candidates.add(newKey);
						known++;
					}
				}
			}
			current++;
		}

		return new HashSet<>(candidates);
	}

	public static boolean areEquivalentSets(Map<Set<TableColumn>,Set<TableColumn>> f1, Map<Set<TableColumn>,Set<TableColumn>> f2, Set<TableColumn> nullFreeSubscheme) {
		// check if f1 a subset of f2
		// check if f2 a subset of f1

		return isSubsetOf(f1,f2, nullFreeSubscheme) && isSubsetOf(f2,f1, nullFreeSubscheme);
	}

	public static boolean isSubsetOf(Map<Set<TableColumn>,Set<TableColumn>> f1, Map<Set<TableColumn>,Set<TableColumn>> f2, Set<TableColumn> nullFreeSubscheme) {
		// for each FD in F1, check if its closure for F1 is contained its closure for F2

		for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(f1)) {
			Set<TableColumn> closure1 = closure(fd.getFirst(), f1, nullFreeSubscheme);
			Set<TableColumn> closure2 = closure(fd.getFirst(), f2, nullFreeSubscheme);

			if(!closure2.containsAll(closure1)) {
				return false;
			}
		}

		return true;
	}

}
