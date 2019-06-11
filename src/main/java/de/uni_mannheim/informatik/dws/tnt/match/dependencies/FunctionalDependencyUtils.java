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

import java.io.File;
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
import de.uni_mannheim.informatik.dws.winter.webtables.FunctionalDependencyDiscovery;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * Utility class for functional dependencies.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class FunctionalDependencyUtils {

/**
	 * Calculates the closure of the given subset of columns under the given set of functional dependencies.
	 * 
	 *  
	 * Let H be a heading, let F be a set of FDs with respect to H, and let Z be a subset of H. Then the closure Z+ of Z under F is the maximal subset C of H such that Z → C is implied by the FDs in F.
	 * 
	 * Z+ := Z ;
	 * do "forever" ;
	 * for each FD X → Y in F
	 * do ;
	 * if X is a subset of Z+
	 * then replace Z+ by the union of Z+ and Y ;
	 * end ;
	 * if Z+ did not change on this iteration
	 * then quit ;  // computation complete 
	 * end ;
	 * 
	 * from: "Database Design and Relational Theory", Chapter 7, O'Reilly
	 * 
	 * @param forColumns the subset of columns
	 * @param functionalDependencies the set of functional dependencies
	 * @return Returns a collection of all columns that are in the calculated closure (and are hence determined by the given subset of columns)
	 */
	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies) {
		return closure(forColumns, functionalDependencies, false);
	}

	/**
	 * Calculates the closure of the given subset of columns under the given set of functional dependencies.
	 * 
	 *  
	 * Let H be a heading, let F be a set of FDs with respect to H, and let Z be a subset of H. Then the closure Z+ of Z under F is the maximal subset C of H such that Z → C is implied by the FDs in F.
	 * 
	 * Z+ := Z ;
	 * do "forever" ;
	 * for each FD X → Y in F
	 * do ;
	 * if X is a subset of Z+
	 * then replace Z+ by the union of Z+ and Y ;
	 * end ;
	 * if Z+ did not change on this iteration
	 * then quit ;  // computation complete 
	 * end ;
	 * 
	 * from: "Database Design and Relational Theory", Chapter 7, O'Reilly
	 * 
	 * @param forColumns the subset of columns
	 * @param functionalDependencies the set of functional dependencies
	 * @return Returns a collection of all columns that are in the calculated closure (and are hence determined by the given subset of columns)
	 */
	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, boolean log) {
		return closure(forColumns, Pair.fromMap(functionalDependencies), log);
	}
	
	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> functionalDependencies, boolean log) {
		
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

					if(result.containsAll(determinant)) {
						Collection<TableColumn> dependant = fd.getSecond();
						
						Collection<TableColumn> newColumns = Q.without(dependant, result);

						result.addAll(dependant);

						if(newColumns.size()>0) {
							trace.add(new Pair<>(determinant, newColumns));
						}
					}
					
				}
				
			} while(result.size()!=lastCount);
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

	public static List<Pair<Set<TableColumn>,Set<TableColumn>>> trace(Set<TableColumn> fromColumns, TableColumn toColumn, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> functionalDependencies) {
		
		Set<TableColumn> result = new HashSet<>();
		
		List<Pair<Set<TableColumn>, Set<TableColumn>>> trace = new LinkedList<>();

		if(functionalDependencies==null) {
			result = null;
		} else {
			result.addAll(fromColumns);
			
			int lastCount;
			do {
				lastCount = result.size();
			
				for(Pair<Set<TableColumn>,Set<TableColumn>> fd : functionalDependencies) {
					Collection<TableColumn> determinant = fd.getFirst();

					if(result.containsAll(determinant)) {
						Collection<TableColumn> dependant = fd.getSecond();
						
						Collection<TableColumn> newColumns = Q.without(dependant, result);

						result.addAll(dependant);
						
						if(newColumns.size()>0) {
							List<Pair<Set<TableColumn>, Set<TableColumn>>> subtrace = followTrace(fromColumns, fd, toColumn, functionalDependencies);
							if(subtrace!=null) {
								trace.add(fd);
								trace.addAll(subtrace);
								return trace;
							}
							

						}
					}
					
				}
				
			} while(result.size()!=lastCount);
		}
		

		if(result.contains(toColumn)) {
			return trace;
		} else {
			return null;
		}
	}

	public static List<Pair<Set<TableColumn>,Set<TableColumn>>> followTrace(Set<TableColumn> fromColumns, Pair<Set<TableColumn>,Set<TableColumn>> via, TableColumn toColumn, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> functionalDependencies) {
		Set<TableColumn> result = new HashSet<>();
		
		List<Pair<Set<TableColumn>, Set<TableColumn>>> trace = new LinkedList<>();

		if(functionalDependencies==null) {
			result = null;
		} else {
			result.addAll(fromColumns);
			result.addAll(via.getSecond());
			
			for(Pair<Set<TableColumn>,Set<TableColumn>> fd : functionalDependencies) {
				Collection<TableColumn> determinant = fd.getFirst();
				Collection<TableColumn> dependant = fd.getSecond();

				// follow an FD only if
				// - it does not lead to an already encountered column
				// - does not have the same determinant as the FD which we are currently following
				if(result.containsAll(determinant) && !result.containsAll(dependant) && !via.getFirst().equals(fd.getFirst())) {
										
					Collection<TableColumn> newColumns = Q.without(dependant, result);

					result.addAll(dependant);

					if(newColumns.size()>0) {
						List<Pair<Set<TableColumn>, Set<TableColumn>>> subtrace = followTrace(result, fd, toColumn, functionalDependencies);
						if(subtrace!=null) {
							trace.add(fd);
							trace.addAll(subtrace);
							return trace;
						}
					}
				}
				
			}
		}
		

		if(result.contains(toColumn)) {
			return trace;
		} else {
			return null;
		}
	}

	public static Map<Set<TableColumn>, Set<TableColumn>> copy(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies) {
		Map<Set<TableColumn>, Set<TableColumn>> result = new HashMap<>();

		for(Set<TableColumn> det : functionalDependencies.keySet()) {
			result.put(new HashSet<>(det), new HashSet<>(functionalDependencies.get(det)));
		}

		return result;
	}

	public static boolean isSuperKey(Set<TableColumn> key, Table t) {
		Set<TableColumn> closure = closure(key, t.getSchema().getFunctionalDependencies());
		
		return closure.equals(new HashSet<>(t.getColumns()));
	}

	public static Map<Set<TableColumn>, Set<TableColumn>> expandToClosure(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies) {
		Collection<Pair<Set<TableColumn>, Set<TableColumn>>> fds = Pair.fromMap(functionalDependencies);

		Map<Set<TableColumn>, Set<TableColumn>> result = new HashMap<>();

		for(Pair<Set<TableColumn>, Set<TableColumn>> fd : fds) {
			Set<TableColumn> closure = closure(fd.getFirst(), functionalDependencies);
			result.put(fd.getFirst(), closure);
		}

		return result;
	}

	public static Set<Pair<Set<TableColumn>,Set<TableColumn>>> split(Collection<Pair<Set<TableColumn>,Set<TableColumn>>> fds) {
		Set<Pair<Set<TableColumn>,Set<TableColumn>>> result = new HashSet<>();

		for(Pair<Set<TableColumn>,Set<TableColumn>> fd : fds) {
			result.addAll(split(fd));
		}

		return result;
	}

	public static Set<Pair<Set<TableColumn>,Set<TableColumn>>> split(Pair<Set<TableColumn>,Set<TableColumn>> fd) {
		Set<Pair<Set<TableColumn>,Set<TableColumn>>> result = new HashSet<>();

		if(fd.getSecond().size()>1) {
			for(TableColumn det : fd.getSecond()) {
				result.add(new Pair<>(new HashSet<>(fd.getFirst()), Q.toSet(det)));
			}
		} else {
			result.add(fd);
		}

		return result;
	}

	public static Map<Set<TableColumn>, Set<TableColumn>> specialise(Pair<Set<TableColumn>,Set<TableColumn>> fdToSpecialise, Collection<TableColumn> attributes) {

		Map<Set<TableColumn>, Set<TableColumn>> specialisations = new HashMap<>();

		// specialise by adding one attribute to the determinant
		for(TableColumn attribute : attributes) {

			// check the specialisation is non-trivial
			if(!fdToSpecialise.getFirst().contains(attribute)) {
				Set<TableColumn> newDet = new HashSet<>(fdToSpecialise.getFirst());
				newDet.add(attribute);

				Set<TableColumn> newDep = new HashSet<>(fdToSpecialise.getSecond());
				newDep.remove(attribute);

				if(newDep.size()>0) {
					specialisations.put(newDet, newDep);
				}
			}
		}

		return specialisations;
	}

	/**
	 * Checks if the provided FD has a generalisation in the provided functional dependencies.
	 * Call with allClosures(functionalDependencies) to guarantee that transitivity is taken into account!
	 */
	public static boolean hasGeneralisation(Pair<Set<TableColumn>, Set<TableColumn>> fdToCheck, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies) {
		return hasGeneralisation(fdToCheck, functionalDependencies, false);
	}

	public static boolean hasGeneralisation(Pair<Set<TableColumn>, Set<TableColumn>> fdToCheck, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, boolean verbose) {
		if(fdToCheck==null) {
			return true;
		} else {
			// X,Y,Z->V
			// check all proper subsets of the given determinant {X,Y,Z}
			for(Set<TableColumn> generalisedDeterminant : Q.getAllProperSubsets(fdToCheck.getFirst())) {
				// generalisedDeterminant: {X,Y}

				// get the dependant of the generalisation: Z u W
				// - W is determined by {X,Y} but not minimal for {X,Y,Z}
				Set<TableColumn> dependant = functionalDependencies.get(generalisedDeterminant);
				
				// check if an FD exists and is non-trivial
				// - dependant != {}
				// - {X,Y} != {Z}
				if(dependant!=null && !generalisedDeterminant.equals(fdToCheck.getSecond())) {

					// also consider transitivity (even if present in the input FDs, the generalised determinant might not exist)
					// - {X,Y}->{Z,W}->...
					dependant = Q.union(dependant, FunctionalDependencyUtils.closure(generalisedDeterminant, functionalDependencies));

					// if a subset of the input FD's determinant {X,Y,Z} determines its dependant V, then that is a generalisation
					// {X,Y}->{Z,W} && W >= V
					if(dependant.containsAll(fdToCheck.getSecond())) {
						if(verbose) System.out.println(String.format("{%s}->{%s} has generalisation {%s}->{%s}",
							StringUtils.join(Q.project(fdToCheck.getFirst(), (c)->c.getHeader()), ","),
							StringUtils.join(Q.project(fdToCheck.getSecond(), (c)->c.getHeader()), ","),
							StringUtils.join(Q.project(generalisedDeterminant, (c)->c.getHeader()), ","),
							StringUtils.join(Q.project(fdToCheck.getSecond(), (c)->c.getHeader()), ",")
						));
						return true;
					}

				}

			}

			return false;
		}
	}

	public static Map<Set<TableColumn>, Set<TableColumn>> generalise(Pair<Set<TableColumn>,Set<TableColumn>> fdToGeneralise, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies) {

		Map<Set<TableColumn>, Set<TableColumn>> generalisations = new HashMap<>();

		// check all proper subsets of the given determinant
		for(Set<TableColumn> generalisedDeterminant : Q.getAllProperSubsets(fdToGeneralise.getFirst())) {

			Set<TableColumn> dependant = functionalDependencies.get(generalisedDeterminant);

			// check if an FD exists
			if(dependant!=null) {

				// return only the part of the determinant which is in the given FD
				// given {X,Y} -> Z
				// - generalisations are {X} -> {Z}, {Y} -> Z and {} -> {Z} and {X}->{Y}, {Y}->{X}
				// - however, {X} -> {W} is not
				dependant = new HashSet<>(Q.intersection(dependant, Q.union(Q.without(fdToGeneralise.getFirst(), generalisedDeterminant), fdToGeneralise.getSecond())));

				if(dependant.containsAll(fdToGeneralise.getSecond())) {
					// add it to the result
					generalisations.put(generalisedDeterminant, dependant);
				}

			}

		}

		return generalisations;

	}

	public static Map<Set<TableColumn>, Set<TableColumn>> generalise(Pair<Set<TableColumn>,Set<TableColumn>> fdToGeneralise) {

		Map<Set<TableColumn>, Set<TableColumn>> generalisations = new HashMap<>();

		// check all proper subsets of the given determinant
		for(Set<TableColumn> generalisedDeterminant : Q.getAllProperSubsets(fdToGeneralise.getFirst())) {

			// return only the part of the determinant which is in the given FD
			// given {X,Y} -> Z
			// - generalisations are {X} -> {Z}, {Y} -> Z and {} -> {Z}
			// - however, {X} -> {W} is not
			Set<TableColumn> dependant = new HashSet<>(Q.union(Q.without(fdToGeneralise.getFirst(), generalisedDeterminant), fdToGeneralise.getSecond()));

			// add it to the result
			generalisations.put(generalisedDeterminant, dependant);
		}

		return generalisations;

	}

	/**
	 * Returns all FDs with their determinants replaced by their closure
	 */
	public static Map<Set<TableColumn>, Set<TableColumn>> allClosures(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies) {
		Map<Set<TableColumn>, Set<TableColumn>> result = new HashMap<>();

		for(Set<TableColumn> determinant : functionalDependencies.keySet()) {
			Set<TableColumn> dependant = closure(determinant, functionalDependencies);
			result.put(determinant, dependant);
		}

		return result;
	}

	public static Map<Set<TableColumn>, Set<TableColumn>> canonicalCover(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies) {
		return canonicalCover(functionalDependencies, false);
	}

	public static Map<Set<TableColumn>, Set<TableColumn>> canonicalCover(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, boolean verbose) {
		// create a deep copy of the functional dependencies, otherwise we would change the input data when modifying the sets!
		Map<Set<TableColumn>, Set<TableColumn>> copy = copy(functionalDependencies);

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
				if(closure(reduced, fds, verbose).containsAll(fd.getSecond())) {
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
				if(closure(fd.getFirst(), reducedFDs, verbose).contains(candidate)) {
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

	public static Map<Set<TableColumn>, Set<TableColumn>> focusedCanonicalCover(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, Set<TableColumn> focus, boolean verbose) {
		// create a deep copy of the functional dependencies, otherwise we would change the input data when modifying the sets!
		Map<Set<TableColumn>, Set<TableColumn>> copy = copy(functionalDependencies);

		Collection<Pair<Set<TableColumn>, Set<TableColumn>>> fds = Pair.fromMap(copy);

		// steer cover towards not reducing FDs which have the focus columns in their determinant by processing them last
		fds = Q.sort(fds, (f1,f2)->{
			if(f1.getFirst().containsAll(focus) && !f2.getFirst().containsAll(focus)) {
				return 1;
			} else if(!f1.getFirst().containsAll(focus) && f2.getFirst().containsAll(focus)) {
				return -1;
			} else {
				return 0;
			}
		});

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
				if(closure(reduced, fds, verbose).containsAll(fd.getSecond())) {
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
				if(closure(fd.getFirst(), reducedFDs, verbose).contains(candidate)) {
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

	public static Pair<Set<TableColumn>,Set<TableColumn>> reduceLHS(Pair<Set<TableColumn>,Set<TableColumn>> fd, Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies) {
		return reduceLHS(fd, functionalDependencies, false);
	}

	public static Pair<Set<TableColumn>,Set<TableColumn>> reduceLHS(Pair<Set<TableColumn>,Set<TableColumn>> fd, Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies, boolean verbose) {
		Collection<Pair<Set<TableColumn>, Set<TableColumn>>> fds = Pair.fromMap(functionalDependencies);
		
		// copy the input FD (so it is not changed in the calling method)
		fd = new Pair<>(new HashSet<>(fd.getFirst()), new HashSet<>(fd.getSecond()));

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
			if(closure(reduced, fds, verbose).containsAll(fd.getSecond())) {
				it.remove();

				if(verbose) System.out.println(String.format("[FunctionalDependencyUtils.canonicalCover] (Linksreduktion) \t{%s}+ -> {%s}, reducing to {%s}->{%s}",
						StringUtils.join(Q.project(reduced, (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(reduced, (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",")
					));
			}
		}

		return fd;
	}

	public static Map<Set<TableColumn>,Set<TableColumn>> mergeLHS(Set<Pair<Set<TableColumn>,Set<TableColumn>>> fds) {
		Map<Set<TableColumn>,Set<TableColumn>> result = new HashMap<>();
		// group all FDs by their LHS
		Map<Set<TableColumn>, Collection<Pair<Set<TableColumn>, Set<TableColumn>>>> groupedByLHS = Q.group(fds, (p)->p.getFirst());
		for(Set<TableColumn> lhs : groupedByLHS.keySet()) {
			// merge the RHSs
			Collection<Pair<Set<TableColumn>, Set<TableColumn>>> allRHS = groupedByLHS.get(lhs);
			Set<TableColumn> combinedRHS = Q.union(Q.project(allRHS, (p)->p.getSecond()));
			result.put(lhs, combinedRHS);
		}

		return result;
	}

	public static Map<Set<TableColumn>,Set<TableColumn>> minimise(Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies) {
		
		Set<Pair<Set<TableColumn>,Set<TableColumn>>> minimal = new HashSet<>();

		for(Pair<Set<TableColumn>,Set<TableColumn>> fd : split(Pair.fromMap(functionalDependencies))) {
			// reduce LHS
			fd = reduceLHS(fd, functionalDependencies);
			// remove trivial FDs
			fd = new Pair<>(fd.getFirst(), new HashSet<>(Q.without(fd.getSecond(), fd.getFirst())));
			if(fd.getSecond().size()>0) {
				minimal.add(fd);
			}
		}

		return mergeLHS(minimal);
	}

	public static Pair<Set<TableColumn>,Set<TableColumn>> minimise(Pair<Set<TableColumn>,Set<TableColumn>> fd, Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies) {
		fd = new Pair<>(new HashSet<>(fd.getFirst()), new HashSet<>(fd.getSecond()));

		// Linksreduktion
		Iterator<TableColumn> it = fd.getFirst().iterator();
		while(it.hasNext()) {
			// choose an element of the LHS
			TableColumn candidate = it.next();

			// remove it
			Set<TableColumn> reduced = new HashSet<>(fd.getFirst());
			reduced.remove(candidate);

			// if the closure of the remaining LHS still contains the RHS, the removed element was redundant
			if(closure(reduced, functionalDependencies).containsAll(fd.getSecond())) {
				it.remove();
			}
		}

		// Rechtsreduktion
		it = fd.getSecond().iterator();
		while(it.hasNext()) {
			// choose an element of the RHS
			TableColumn candidate = it.next();

			// remove it
			Set<TableColumn> reduced = new HashSet<>(fd.getSecond());
			reduced.remove(candidate);

			// create a new set of FDs
			Map<Set<TableColumn>, Set<TableColumn>> reducedFDs = new HashMap<>(functionalDependencies);
			// replace the currently tested FD in this new set by removing the chosen element of the RHS
			reducedFDs.put(fd.getFirst(), reduced);

			// if the closure of the LHS w.r.t. to the new set of FDs still contains the removed element, it was redundant
			if(closure(fd.getFirst(), reducedFDs).contains(candidate)) {
				it.remove();
			}
		}

		// Leere Klauseln
		if(fd.getSecond().size()==0) {
			// remove all FDs with an empty RHS
			return null;
		} else {
			return fd;
		}
	}

	public static Set<TableColumn> minimise(Set<TableColumn> attributes, Table t) {
		return minimise(attributes, t.getColumns().size(), t.getSchema().getFunctionalDependencies());
	}

	public static Set<TableColumn> minimise(Set<TableColumn> attributes, int numAttributesInRelation, Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies) {
		Set<TableColumn> result = new HashSet<>(attributes);
		Iterator<TableColumn> it = result.iterator();

		while(it.hasNext()) {
			TableColumn candidate = it.next();
			Set<TableColumn> reduced = new HashSet<>(result);
			reduced.remove(candidate);
			if(closure(reduced, functionalDependencies).size()==numAttributesInRelation) {
				it.remove();
			}
		}

		return result;
	}

	public static boolean areEquivalentSets(Map<Set<TableColumn>,Set<TableColumn>> f1, Map<Set<TableColumn>,Set<TableColumn>> f2) {
		// check if f1 a subset of f2
		// check if f2 a subset of f1

		return isSubsetOf(f1,f2) && isSubsetOf(f2,f1);
	}

	public static boolean isSubsetOf(Map<Set<TableColumn>,Set<TableColumn>> f1, Map<Set<TableColumn>,Set<TableColumn>> f2) {
		// for each FD in F1, check if its closure for F1 is contained its closure for F2

		for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(f1)) {
			Set<TableColumn> closure1 = closure(fd.getFirst(), f1);
			Set<TableColumn> closure2 = closure(fd.getFirst(), f2);

			if(!closure2.containsAll(closure1)) {
				return false;
			}
		}

		return true;
	}

	public static Set<Pair<Set<TableColumn>,Set<TableColumn>>> notIn(Map<Set<TableColumn>,Set<TableColumn>> f1, Map<Set<TableColumn>,Set<TableColumn>> f2) {
		// for each FD in F1, check if its closure for F1 is contained its closure for F2
		Set<Pair<Set<TableColumn>,Set<TableColumn>>> result = new HashSet<>();

		for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(f1)) {
			Set<TableColumn> closure1 = closure(fd.getFirst(), f1);
			Set<TableColumn> closure2 = closure(fd.getFirst(), f2);

			if(!closure2.containsAll(closure1)) {
				result.add(fd);
			}
		}

		return result;
	}

	public static Set<Set<TableColumn>> listCandidateKeys(Table t) {
		
		List<Set<TableColumn>> candidates = new LinkedList<>();

		Set<TableColumn> firstKey = minimise(new HashSet<>(t.getColumns()), t);

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
						Set<TableColumn> newKey = minimise(key, t);
						candidates.add(newKey);
						known++;
					}
				}
			}
			current++;
		}

		return new HashSet<>(candidates);
	}

	public static Set<Set<TableColumn>> listCandidateKeys(Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies, Set<TableColumn> columns) {
		
		List<Set<TableColumn>> candidates = new LinkedList<>();

		Set<TableColumn> firstKey = minimise(columns, columns.size(), functionalDependencies);

		candidates.add(firstKey);

		int known = 1;
		int current = 0;

		while(current < known) {
			for(Set<TableColumn> det : functionalDependencies.keySet()) {
				if(det.size()>0) {
					Set<TableColumn> dep = functionalDependencies.get(det);
					Set<TableColumn> key = Q.union(det, Q.without(candidates.get(current), dep));

					boolean superKey = Q.any(candidates, (k)->key.containsAll(k));

					if(!superKey) {
						Set<TableColumn> newKey = minimise(key, columns.size(), functionalDependencies);
						candidates.add(newKey);
						known++;
					}
				}
			}
			current++;
		}

		return new HashSet<>(candidates);
	}
	
	public static Map<Collection<TableColumn>, Double> enumerateApproximateKeys(Table t, Collection<Collection<TableColumn>> candidateKeysToCheck, double minUniqueness) {
		HashMap<Collection<TableColumn>, Double> approximateKeys = new HashMap<>();
		LinkedList<Collection<TableColumn>> candidates = new LinkedList<>();
		candidates.addAll(candidateKeysToCheck);
			
		while(candidates.size()>0) {
			Collection<TableColumn> current = candidates.removeFirst();

			for(TableColumn c : current) {
				Set<TableColumn> sub = new HashSet<>(current);
				sub.remove(c);
				
				double uniqueness = calculateUniqueness(t, sub);
				
				if(uniqueness >= minUniqueness) {
					approximateKeys.put(sub, uniqueness);
					if(sub.size()>1) {
						candidates.add(sub);
					}
				}
			}
		}	
			
		return approximateKeys;
	}
	public static double calculateUniqueness(Table t, Collection<TableColumn> columns) {
		HashSet<String> values = new HashSet<>();
		
		for(TableRow r : t.getRows()) {
			String allValues = StringUtils.join(r.project(columns), "");
			values.add(allValues);
		}
		
		return (double)values.size() / (double)t.getRows().size();
	}
	
	public static Collection<Collection<TableColumn>> listCandidateKeysExcludingColumns(Table t, Collection<TableColumn> excludedColumns) {
		HashSet<Collection<TableColumn>> superKeys = new HashSet<>();
		
		// check all possible superkeys
		// start with table heading and remove single attributes
		// for all sets that are superkeys, keep removing attributes
		
		LinkedList<Collection<TableColumn>> candidates = new LinkedList<>();
		Set<TableColumn> heading = new HashSet<>(t.getColumns());
		heading.removeAll(excludedColumns);
		candidates.add(heading);
		
		while(candidates.size()>0) {
			Collection<TableColumn> current = candidates.removeFirst();
			
			boolean anySubKey = false;
			for(TableColumn c : current) {
				Set<TableColumn> sub = new HashSet<>(current);
				sub.remove(c);
				
				// check if it's a super key
				Set<TableColumn> closure = closure(new HashSet<>(sub), t.getSchema().getFunctionalDependencies());
				closure.removeAll(excludedColumns);
				
				if(closure.equals(heading)) {
					// a subset of current is a superkey, so we don't want to output current
					anySubKey = true;
					candidates.add(sub);
				}
			}
			
			if(!anySubKey) {
				// current is a minimal superkey, so add it to the output
				superKeys.add(current);
			}
			
			
		}
		
		return superKeys;
	}
	
	public static void calculcateFunctionalDependencies(Collection<Table> tables, File csvLocation) throws Exception {
		FunctionalDependencyDiscovery.calculcateFunctionalDependencies(tables, csvLocation);
	}
	
	public static void calculcateFunctionalDependenciesWithoutCandidateKeys(Collection<Table> tables, File csvLocation) throws Exception {
		FunctionalDependencyDiscovery.calculcateFunctionalDependencies(tables, csvLocation);
	}

	public static Map<Set<TableColumn>, Set<TableColumn>> calculateFunctionalDependencies(final Table t, File tableAsCsv) throws Exception {
		return FunctionalDependencyDiscovery.calculateFunctionalDependencies(t, tableAsCsv);
	}
	public static void calculateApproximateFunctionalDependencies(Collection<Table> tables, File csvLocation, double errorThreshold) throws Exception {
		FunctionalDependencyDiscovery.calculateApproximateFunctionalDependencies(tables, csvLocation, errorThreshold);
	}
}
