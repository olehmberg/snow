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

import de.uni_mannheim.informatik.dws.tnt.match.data.StitchedModel;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import java.util.*;
import org.apache.commons.lang.StringUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class StitchedFunctionalDependencyUtils {

    public static Map<Set<TableColumn>, Set<TableColumn>> canonicalCover(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, StitchedModel model) {
		return canonicalCover(functionalDependencies, model, false);
	}

	public static Map<Set<TableColumn>, Set<TableColumn>> canonicalCover(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, StitchedModel model, boolean verbose) {
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
				if(closure(reduced, fds, model, verbose).containsAll(fd.getSecond())) {
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
				if(closure(fd.getFirst(), reducedFDs, model, verbose).contains(candidate)) {
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

    public static Map<Set<TableColumn>, Set<TableColumn>> focusedCanonicalCover(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, Set<TableColumn> focus, StitchedModel model, boolean verbose) {
		// create a deep copy of the functional dependencies, otherwise we would change the input data when modifying the sets!
		Map<Set<TableColumn>, Set<TableColumn>> copy = FunctionalDependencyUtils.copy(functionalDependencies);

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
				if(closure(reduced, fds, model, verbose).containsAll(fd.getSecond())) {
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
				if(closure(fd.getFirst(), reducedFDs, model, verbose).contains(candidate)) {
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

    public static Pair<Set<TableColumn>,Set<TableColumn>> reduceLHS(Pair<Set<TableColumn>,Set<TableColumn>> fd, Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies, StitchedModel model, boolean useWeakClosure, boolean verbose) {
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

			// if the closure of the remaining LHS still contains the RHS, the removed element was redundant
            Set<TableColumn> closure;
            if(useWeakClosure) {
                closure = weakClosure(reduced, fds, model, verbose);
            } else {
                closure = closure(reduced, fds, model, verbose);
            }
			if(closure.containsAll(fd.getSecond())) {
				it.remove();

				if(verbose) System.out.println(String.format("[FunctionalDependencyUtils.reduceLHS]\t{%s}+ -> {%s}, reducing to {%s}->{%s}",
						StringUtils.join(Q.project(reduced, (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(reduced, (c)->c.getHeader()), ","),
						StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",")
					));
			}
		}

		return fd;
	}

    public static Set<TableColumn> closure(Set<TableColumn> forColumns, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, StitchedModel model) {
        return closure(forColumns, Pair.fromMap(functionalDependencies), model, false);
    }

    public static Set<TableColumn> closure(Set<TableColumn> forColumns, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, StitchedModel model, boolean log) {
        return closure(forColumns, Pair.fromMap(functionalDependencies), model, log);
    }
    
	/**
	 * 
	 * @param forColumns the subset of columns
	 * @param functionalDependencies the set of functional dependencies
	 * @return Returns a collection of all columns that are in the calculated closure (and are hence determined by the given subset of columns)
	 */
	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> functionalDependencies, StitchedModel model, boolean log) {

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
			
                for(Pair<Set<TableColumn>,Set<TableColumn>> fd : FunctionalDependencyUtils.split(functionalDependencies)) {
                    // given two relations R1={a,b,c}, R2={b,c,d} stitched into R={a,b,c,d}
                    // with FDs
                    // R1: {a,b,c}->{}
                    // R2: {d,c}->{b}
                    // the closure is
                    // {a,b,c,d}+ = {a,b,c,d}
                    // {a,b,c}+ = {a,b,c}
                    // {d,c}+ = {b,c,d}
                    // {a,c,d}+ = {a,c,d} or {a,b,c,d} ?
                    // {a,c,d}+ = {a,c,d} because {d,c}->{b} is weak: {d,c} does not always occur where {b} occurs, so no transitivity via {b} can hold
                    
                    if(fd.getFirst().size()==0 || model.isAlwaysStrongFunctionalDependency(fd)) {
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
				}
				
			} while(result.size()!=lastCount);

            Set<TableColumn> weakColumns = new HashSet<>();
            for(Pair<Set<TableColumn>,Set<TableColumn>> fd : FunctionalDependencyUtils.split(functionalDependencies)) {
                if(fd.getFirst().size()!=0 && !model.isAlwaysStrongFunctionalDependency(fd)) {
                    Collection<TableColumn> determinant = fd.getFirst();

                    if(forColumns.containsAll(determinant)) {
                        Collection<TableColumn> dependant = fd.getSecond();
                
                        Collection<TableColumn> newColumns = Q.without(dependant, result);

                        weakColumns.addAll(dependant);

                        
                        if(newColumns.size()>0) {
                            trace.add(new Pair<>(determinant, newColumns));
                        }
                    }
                }
            }
            result.addAll(weakColumns);
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
			System.out.println(String.format("[StitchedFunctionalDepdencyUtils.closure] %s", sb.toString()));
		}

		return result;
    }

    public static Set<TableColumn> weakClosure(Set<TableColumn> forColumns, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, StitchedModel model) {
        return weakClosure(forColumns, Pair.fromMap(functionalDependencies), model, false);
    }

    public static Set<TableColumn> weakClosure(Set<TableColumn> forColumns, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, StitchedModel model, boolean log) {
        return weakClosure(forColumns, Pair.fromMap(functionalDependencies), model, log);
    }

    /**
     * Allows transitivity if the FD is an existing column combination.
     * weaker than closure(), which only allows transitivity if the determinant ALWAYS exists for the dependant
     */
    public static Set<TableColumn> weakClosure(Set<TableColumn> forColumns, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> functionalDependencies, StitchedModel model, boolean log) {

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
			
                for(Pair<Set<TableColumn>,Set<TableColumn>> fd : FunctionalDependencyUtils.split(functionalDependencies)) {
                    if(model.isExistingColumnCombination(Q.union(forColumns, fd.getFirst()))) {
                        Collection<TableColumn> determinant = fd.getFirst();

                        if(result.containsAll(determinant)) {
                            Collection<TableColumn> dependant = fd.getSecond();
                
                            Collection<TableColumn> newColumns = Q.without(dependant, result);

                            result.addAll(dependant);

                            
                            if(newColumns.size()>0) {
                                trace.add(new Pair<>(determinant, newColumns));
                            }
                        }
                    } else {
                        trace.add(new Pair<>(fd.getFirst(), new HashSet<>()));
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
			System.out.println(String.format("[StitchedFunctionalDepdencyUtils.weakClosure] %s", sb.toString()));
		}

		return result;
    }

	public static Map<Set<TableColumn>,Set<TableColumn>> minimise(Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies, StitchedModel model) {
		return minimise(functionalDependencies, model, false);
	}

	public static Map<Set<TableColumn>,Set<TableColumn>> minimise(Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies, StitchedModel model, boolean verbose) {
		
		Set<Pair<Set<TableColumn>,Set<TableColumn>>> minimal = new HashSet<>();

		for(Pair<Set<TableColumn>,Set<TableColumn>> fd : FunctionalDependencyUtils.split(Pair.fromMap(functionalDependencies))) {
			// reduce LHS
			fd = reduceLHS(fd, functionalDependencies, model, false, verbose);
			// remove trivial FDs
			fd = new Pair<>(fd.getFirst(), new HashSet<>(Q.without(fd.getSecond(), fd.getFirst())));
			if(fd.getSecond().size()>0) {
				minimal.add(fd);
			}
		}

		return FunctionalDependencyUtils.mergeLHS(minimal);
	}

	public static Set<TableColumn> minimise(Set<TableColumn> attributes, Table t, StitchedModel model) {
		return minimise(attributes, t.getColumns().size(), t.getSchema().getFunctionalDependencies(), model);
	}

	public static Set<TableColumn> minimise(Set<TableColumn> attributes, int numAttributesInRelation, Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies, StitchedModel model) {
		Set<TableColumn> result = new HashSet<>(attributes);
		Iterator<TableColumn> it = result.iterator();

		while(it.hasNext()) {
			TableColumn candidate = it.next();
			Set<TableColumn> reduced = new HashSet<>(result);
            reduced.remove(candidate);
			if(closure(reduced, functionalDependencies, model).size()==numAttributesInRelation) {
				it.remove();
			}
		}

		return result;
	}

    public static Set<Set<TableColumn>> listCandidateKeys(Table t, StitchedModel model) {
        return listCandidateKeys(t, model, false);
    }

	public static Set<Set<TableColumn>> listCandidateKeys(Table t, StitchedModel model, boolean verbose) {
        return listCandidateKeys(t.getSchema().getFunctionalDependencies(), new HashSet<>(t.getColumns()), model, verbose);
	}

    public static Set<Set<TableColumn>> listCandidateKeys(Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies, Set<TableColumn> columns, StitchedModel model) {
        return listCandidateKeys(functionalDependencies, columns, model, false);
    }

	/**
	 * 
	 * Lists all minimal candidate keys
	 * 
	 * based on:
	 * Lucchesi, Claudio L., and Sylvia L. Osborn. "Candidate keys for relations." Journal of Computer and System Sciences 17.2 (1978): 270-279.
	 * 
	 */
    public static Set<Set<TableColumn>> listCandidateKeys(Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies, Set<TableColumn> columns, StitchedModel model, boolean verbose) {
		
		List<Set<TableColumn>> candidates = new LinkedList<>();

		Set<TableColumn> firstKey = minimise(columns, columns.size(), functionalDependencies, model);

		candidates.add(firstKey);
        if(verbose) System.out.println(String.format("[StitchedFunctionalDependencyUtils.listCandidateKeys] discovered candidate key: {%s}",
            StringUtils.join(Q.project(firstKey, (c)->c.getHeader()), ",")
        ));

		int known = 1;
		int current = 0;

		while(current < known) {
			// for each known candidate key, check if combining it with an FD results in a new key
            for(Pair<Set<TableColumn>,Set<TableColumn>> fd : FunctionalDependencyUtils.split(Pair.fromMap(functionalDependencies))) {
                Set<TableColumn> det = fd.getFirst();
				Set<TableColumn> dep = fd.getSecond();
				
				if(det.size()>0) {
					// remove the FD's dependant from the current key and add the determinant
					Set<TableColumn> key = Q.union(det, Q.without(candidates.get(current), dep));

					// check if any known key is a subset of the result, i.e., the new key is not minimal
					boolean superKey = Q.any(candidates, (k)->key.containsAll(k));

					if(!superKey) {
						// if not, remove any unnecessary attributes from the new key
                        Set<TableColumn> newKey = minimise(key, columns.size(), functionalDependencies, model);
                        if(verbose) System.out.println(String.format("[StitchedFunctionalDependencyUtils.listCandidateKeys] combining {%s}->{%s} with known candidate key {%s}",
                            StringUtils.join(Q.project(det, (c)->c.getHeader()), ","),
                            StringUtils.join(Q.project(dep, (c)->c.getHeader()), ","),
                            StringUtils.join(Q.project(candidates.get(current), (c)->c.getHeader()), ",")
                        ));
                        if(verbose) System.out.println(String.format("[StitchedFunctionalDependencyUtils.listCandidateKeys] discovered candidate key: {%s}",
                            StringUtils.join(Q.project(newKey, (c)->c.getHeader()), ",")
						));
						// and add it to the list of known keys
						candidates.add(newKey);
						known++;
					}
				}
			}
			current++;
		}

		return new HashSet<>(candidates);
	}

	public static Set<Set<TableColumn>> listCandidateKeysBasedOnMinimalFDs(Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies, Set<TableColumn> columns, StitchedModel model, boolean verbose) {
		Set<Set<TableColumn>> result = new HashSet<>();

		for(Pair<Set<TableColumn>, Set<TableColumn>> fd : Pair.fromMap(functionalDependencies)) {
			Set<TableColumn> candidate = new HashSet<>(Q.union(fd.getFirst(), closure(fd.getFirst(), functionalDependencies, model)));
			if(candidate.containsAll(columns)) {
				result.add(new HashSet<>(fd.getFirst()));
			} else if(verbose) {
				System.out.println(String.format("No key: {%s} missing from {%s}+ = {%s}",
					StringUtils.join(Q.project(Q.without(columns, candidate), (c)->c.getHeader()), ","),
					StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
					StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",")
				));
			}
		}

		return result;
	}

	public static boolean areEquivalentSets(Map<Set<TableColumn>,Set<TableColumn>> f1, Map<Set<TableColumn>,Set<TableColumn>> f2, StitchedModel model) {
		// check if f1 a subset of f2
		// check if f2 a subset of f1

		return isSubsetOf(f1,f2,model) && isSubsetOf(f2,f1,model);
	}

	public static boolean isSubsetOf(Map<Set<TableColumn>,Set<TableColumn>> f1, Map<Set<TableColumn>,Set<TableColumn>> f2, StitchedModel model) {
		// for each FD in F1, check if its closure for F1 is contained its closure for F2

        Collection<Pair<Set<TableColumn>,Set<TableColumn>>> pairs1 = Pair.fromMap(f1);
        Collection<Pair<Set<TableColumn>,Set<TableColumn>>> pairs2 = Pair.fromMap(f2);

		for(Pair<Set<TableColumn>,Set<TableColumn>> fd : pairs1) {
			Set<TableColumn> closure1 = closure(fd.getFirst(), pairs1, model, false);
			Set<TableColumn> closure2 = closure(fd.getFirst(), pairs2, model, false);

			if(!closure2.containsAll(closure1)) {
				return false;
			}
		}

		return true;
	}
    public static boolean hasGeneralisation(Pair<Set<TableColumn>, Set<TableColumn>> fdToCheck, Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, StitchedModel model, boolean useWeakClosure) {
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
                    if(useWeakClosure) {
                        dependant = Q.union(dependant, weakClosure(generalisedDeterminant, functionalDependencies, model));
                    } else {
                        dependant = Q.union(dependant, closure(generalisedDeterminant, functionalDependencies, model));
                    }

					// if a subset of the input FD's determinant {X,Y,Z} determines its dependant V, then that is a generalisation
					// {X,Y}->{Z,W} && W >= V
					if(dependant.containsAll(fdToCheck.getSecond())) {
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
	
	/**
	 * Returns all FDs with their determinants replaced by their closure
	 */
	public static Map<Set<TableColumn>, Set<TableColumn>> allClosures(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, StitchedModel model) {
		return allClosures(functionalDependencies, model, false);
	}

	public static Map<Set<TableColumn>, Set<TableColumn>> allClosures(Map<Set<TableColumn>, Set<TableColumn>> functionalDependencies, StitchedModel model, boolean verbose) {
		Map<Set<TableColumn>, Set<TableColumn>> result = new HashMap<>();

		for(Set<TableColumn> determinant : functionalDependencies.keySet()) {
			Set<TableColumn> dependant = closure(determinant, functionalDependencies, model, verbose);
			result.put(determinant, dependant);
		}

		return result;
	}
}