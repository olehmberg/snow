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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
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
public class StrongAndWeakFunctionalDependencyUtils {

	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Map<Set<TableColumn>, Set<TableColumn>> strongFunctionalDependencies, Map<Set<TableColumn>, Set<TableColumn>> weakFunctionalDependencies) {
		return closure(forColumns, Pair.fromMap(strongFunctionalDependencies), Pair.fromMap(weakFunctionalDependencies), false);
	}

	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> strongFunctionalDependencies, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> weakFunctionalDependencies) {
		return closure(forColumns, strongFunctionalDependencies, weakFunctionalDependencies, false);
	}

	/**
	 * TODO not sure if correct!
	 * 
	 * @param forColumns the subset of columns
	 * @param functionalDependencies the set of functional dependencies
	 * @return Returns a collection of all columns that are in the calculated closure (and are hence determined by the given subset of columns)
	 */
	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> strongFunctionalDependencies, Collection<Pair<Set<TableColumn>, Set<TableColumn>>> weakFunctionalDependencies, boolean log) {
		
		Set<TableColumn> result = new HashSet<>();
		
		List<Pair<Collection<TableColumn>, Collection<TableColumn>>> trace = new LinkedList<>();

		if(strongFunctionalDependencies==null) {
			result = null;
		} else {
			result.addAll(forColumns);
			trace.add(new Pair<>(forColumns, null));
			
			int lastCount;
			do {
				lastCount = result.size();
			
				for(Pair<Set<TableColumn>,Set<TableColumn>> fd : strongFunctionalDependencies) {
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

			for(Pair<Set<TableColumn>,Set<TableColumn>> fd : weakFunctionalDependencies) {
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
					// sb.append(String.format("{%s}", StringUtils.join(Q.project(step.getFirst(), (c)->c.getHeader()), ",")));
					sb.append(String.format("{%s}", StringUtils.join(Q.project(step.getFirst(), (c)->c.toString()), ",")));
				} else {
					sb.append(String.format(" / {%s}->{%s}", 
						// StringUtils.join(Q.project(step.getFirst(), (c)->c.getHeader()), ","),
						// StringUtils.join(Q.project(step.getSecond(), (c)->c.getHeader()), ",")
						StringUtils.join(Q.project(step.getFirst(), (c)->c.toString()), ","),
						StringUtils.join(Q.project(step.getSecond(), (c)->c.toString()), ",")
					));
				}
			}
			System.out.println(String.format("[FunctionalDepdencyUtils.closure] %s", sb.toString()));
		}

		return result;
	}


	public static boolean hasGeneralisation(Pair<Set<TableColumn>, Set<TableColumn>> fdToCheck, Map<Set<TableColumn>, Set<TableColumn>> strongFunctionalDependencies, Map<Set<TableColumn>, Set<TableColumn>> weakFunctionalDependencies) {

		if(fdToCheck==null) {
			return true;
		} else {
			// check if it is a strong or weak FD
			boolean isStrong = false;
			if(strongFunctionalDependencies.containsKey(fdToCheck.getFirst())) {
				isStrong = closure(strongFunctionalDependencies.get(fdToCheck.getFirst()), strongFunctionalDependencies, weakFunctionalDependencies).containsAll(fdToCheck.getSecond());
			}

			// check all proper subsets of the given determinant
			for(Set<TableColumn> generalisedDeterminant : Q.getAllProperSubsets(fdToCheck.getFirst())) {

				Set<TableColumn> dependant = closure(generalisedDeterminant, strongFunctionalDependencies, weakFunctionalDependencies);

				// check if an FD exists
				if(dependant!=null) {

					if(dependant.containsAll(fdToCheck.getSecond())) {
						return true;
					}

				}

			}

			return false;

			// an input FD {X,W}->{Y} has a generalisation {X}->{Y} 
			
		}
	}

}
