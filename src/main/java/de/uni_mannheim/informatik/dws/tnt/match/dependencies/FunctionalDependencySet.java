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

import java.util.*;
import org.apache.commons.lang.StringUtils;

// import de.hpi.metanome.algorithms.hyfd.structures.FDTree.FD;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class FunctionalDependencySet {

    Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies;
    private String tableId = "";
    private Func<Boolean, Pair<Pair<Set<TableColumn>,Set<TableColumn>>,Pair<Set<TableColumn>,Set<TableColumn>>>> invalidationCondition = (p) -> true;
    private Func<Boolean, Pair<Set<TableColumn>,Set<TableColumn>>> propagationCondition = (p) -> true;
    private Func<Collection<TableColumn>, Pair<Set<TableColumn>,Set<TableColumn>>> propagateInvalidationToDependants = (p) -> new HashSet<>();
    private Func<Collection<TableColumn>, Pair<Pair<Set<TableColumn>,Set<TableColumn>>,Pair<Set<TableColumn>,Set<TableColumn>>>> specialiseWith = (p) -> new HashSet<>();
    private boolean verbose = false;

    private Func<Set<TableColumn>,Pair<Set<TableColumn>,Map<Set<TableColumn>,Set<TableColumn>>>> closure = (p) -> FunctionalDependencyUtils.closure(p.getFirst(), p.getSecond());
    private Func<Boolean,Pair<Pair<Set<TableColumn>,Set<TableColumn>>,Map<Set<TableColumn>,Set<TableColumn>>>> hasGeneralisation = (p) -> FunctionalDependencyUtils.hasGeneralisation(p.getFirst(), p.getSecond());
    private Func<Pair<Set<TableColumn>,Set<TableColumn>>,Pair<Pair<Set<TableColumn>,Set<TableColumn>>,Map<Set<TableColumn>,Set<TableColumn>>>> reduceLHS = (p) -> FunctionalDependencyUtils.reduceLHS(p.getFirst(), p.getSecond());
    private Func<Pair<Set<TableColumn>,Set<TableColumn>>,Pair<Pair<Set<TableColumn>,Set<TableColumn>>,Map<Set<TableColumn>,Set<TableColumn>>>> reduceLHSVerbose = (p) -> FunctionalDependencyUtils.reduceLHS(p.getFirst(), p.getSecond(), true);
    
    public boolean hasGeneralisation(Pair<Set<TableColumn>,Set<TableColumn>> fd, Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies) {
        return hasGeneralisation.invoke(new Pair<>(fd, functionalDependencies));
    }

    public Set<TableColumn> closure(Set<TableColumn> forColumns, Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies) {
        return closure.invoke(new Pair<>(forColumns, functionalDependencies));
    }

    public Pair<Set<TableColumn>,Set<TableColumn>> reduceLHS(Pair<Set<TableColumn>,Set<TableColumn>> fd, Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies) {
        return reduceLHS.invoke(new Pair<>(fd, functionalDependencies));
    }

    public Pair<Set<TableColumn>,Set<TableColumn>> reduceLHSVerbose(Pair<Set<TableColumn>,Set<TableColumn>> fd, Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies) {
        return reduceLHSVerbose.invoke(new Pair<>(fd, functionalDependencies));
    }

    /**
     * @param closure the closure to set
     */
    public void setClosure(
            Func<Set<TableColumn>, Pair<Set<TableColumn>, Map<Set<TableColumn>, Set<TableColumn>>>> closure) {
        this.closure = closure;
    }
    /**
     * @param hasGeneralisation the hasGeneralisation to set
     */
    public void setHasGeneralisation(
            Func<Boolean, Pair<Pair<Set<TableColumn>, Set<TableColumn>>, Map<Set<TableColumn>, Set<TableColumn>>>> hasGeneralisation) {
        this.hasGeneralisation = hasGeneralisation;
    }
    /**
     * @param reduceLHS the reduceLHS to set
     */
    public void setReduceLHS(
            Func<Pair<Set<TableColumn>, Set<TableColumn>>, Pair<Pair<Set<TableColumn>, Set<TableColumn>>, Map<Set<TableColumn>, Set<TableColumn>>>> reduceLHS) {
        this.reduceLHS = reduceLHS;
    }

    /**
     * @param reduceLHSVerbose the reduceLHSVerbose to set
     */
    public void setReduceLHSVerbose(
            Func<Pair<Set<TableColumn>, Set<TableColumn>>, Pair<Pair<Set<TableColumn>, Set<TableColumn>>, Map<Set<TableColumn>, Set<TableColumn>>>> reduceLHSVerbose) {
        this.reduceLHSVerbose = reduceLHSVerbose;
    }

    private int recursionDepth = 0;
    private String indentationString = "\t\t\t";
    protected String getLogIndentation() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < recursionDepth; i++) {
            sb.append(indentationString);
        }
        return sb.toString();
    }

    protected void log(String message) {
        if(verbose) {
            System.out.println(String.format("%s%s", getLogIndentation(), message));
        }
    }

    /**
     * @param tableId the tableId to set
     */
    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    /**
     * @return the functionalDependencies
     */
    public Map<Set<TableColumn>, Set<TableColumn>> getFunctionalDependencies() {
        return functionalDependencies;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public FunctionalDependencySet(Map<Set<TableColumn>,Set<TableColumn>> functionalDependencies) {
        // this.functionalDependencies = functionalDependencies;
        this.functionalDependencies = new HashMap<>(functionalDependencies);
        // this.functionalDependencies = FunctionalDependencyUtils.allClosures(functionalDependencies);
    }

    /**
     * Specifies a condition that must hold if an FD is invalidated after adding a new, minimal FD
     * 
     * @param invalidationCondition a function that receives a pair (added FD, invalidated FD) and returns whether the invalidated FD should be removed
     */
    public void setInvalidationCondition(
            Func<Boolean, Pair<Pair<Set<TableColumn>, Set<TableColumn>>, Pair<Set<TableColumn>, Set<TableColumn>>>> invalidationCondition) {
        this.invalidationCondition = invalidationCondition;
    }

    /**
     * Specifies a condition that must hold if an invalidated FD is propagated.
     * A condition that always returns false has the result that FDs are never propagated after they are invalidated.
     * 
     * @param propagationCondition the condition to evaluate
     */
    public void setPropagationCondition(
            Func<Boolean, Pair<Set<TableColumn>, Set<TableColumn>>> propagationCondition) {
        this.propagationCondition = propagationCondition;
    }

    /**
     * Specifies a set of additional dependants to which the invalidation of an FD is propagated.
     * Example: if X -> Y is invalidated and this function returns {A,B} for X -> Y, then X -> A and X -> B are also invalidated
     * 
     * @param propagateInvalidationToDependants the function returning the set of additional dependendants
     */
    public void setPropagateInvalidationToDependants(
            Func<Collection<TableColumn>, Pair<Set<TableColumn>, Set<TableColumn>>> propagateInvalidationToDependants) {
        this.propagateInvalidationToDependants = propagateInvalidationToDependants;
    }

    /**
     * Specifies the set of columns to use when specialising an invalidated FD.
     * Example: if X -> Y is invalidated and this function returns {A,B} for X -> Y, then {X,A} -> Y and {X,B} -> Y are generated as new FDs
     * 
     * @param specialiseWith the specialiseWith to set
     */
    public void setSpecialiseWith(Func<Collection<TableColumn>, Pair<Pair<Set<TableColumn>,Set<TableColumn>>,Pair<Set<TableColumn>,Set<TableColumn>>>> specialiseWith) {
        this.specialiseWith = specialiseWith;
    }

    public void add(FunctionalDependencySet fds) {
        for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(fds.getFunctionalDependencies())) {
            addFunctionalDependency(fd);
        }
    }

    public void addFunctionalDependency(Pair<Set<TableColumn>,Set<TableColumn>> fd) {
        Set<TableColumn> newDependant = functionalDependencies.get(fd.getFirst());
        if(newDependant==null) {
            newDependant = fd.getSecond();
        } else {
            newDependant = Q.union(newDependant, fd.getSecond());
        }
        functionalDependencies.put(fd.getFirst(), newDependant);

        log(String.format("\tadding %s: {%s}->{%s}", 
            tableId,
            StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
            StringUtils.join(Q.project(newDependant, (c)->c.getHeader()), ",")
            ));
    }

    public Collection<Pair<Set<TableColumn>, Set<TableColumn>>> getInvalidatedFDs(Pair<Set<TableColumn>,Set<TableColumn>> fd) {
        Collection<Pair<Set<TableColumn>, Set<TableColumn>>> invalidated = new LinkedList<>();

        Set<TableColumn> newFD = Q.union(fd.getFirst(), fd.getSecond());

        // check all existing FDs
        for(Pair<Set<TableColumn>, Set<TableColumn>> existing : FunctionalDependencyUtils.split(Pair.fromMap(functionalDependencies))) {

            if(
                (
                // if the existing determinant is a proper subset of the new determinant
                fd.getFirst().containsAll(existing.getFirst()) && fd.getFirst().size()>existing.getFirst().size()
                &&
                (
                    // and determines the same dependant
                    newFD.containsAll(existing.getSecond())
                )
                )
                && invalidationCondition.invoke(new Pair<>(fd, existing))
            ) {
                invalidated.add(existing);
            }
        }

        return invalidated;
    }

    /**
     * Removes an FD from the set.
     */
    public boolean removeFunctionalDependency(Pair<Set<TableColumn>,Set<TableColumn>> invalid) {
        Set<TableColumn> existingDependant = functionalDependencies.get(invalid.getFirst());
        
        // only continue if the FD existed
        if(existingDependant!=null && existingDependant.containsAll(invalid.getSecond())) {

            // check if there are any additional dependants in the generalisation
            existingDependant = new HashSet<>(Q.without(existingDependant, invalid.getSecond()));

            if(existingDependant.size()==0) {
                // if not, remove the FD completely
                log(String.format("\t\t\tremoving %s: {%s}->{%s}", 
                    tableId,
                    StringUtils.join(Q.project(invalid.getFirst(), (c)->c.getHeader()), ","),
                    StringUtils.join(Q.project(invalid.getSecond(), (c)->c.getHeader()), ",")
                    ));

                    functionalDependencies.remove(invalid.getFirst());
            } else {
                // otherwise, just remove the invalidated attributes from the dependant
                log(String.format("\t\t\treducing %s: {%s}->{%s} to %s: {%s}->{%s}", 
                    tableId,
                    StringUtils.join(Q.project(invalid.getFirst(), (c)->c.getHeader()), ","),
                    StringUtils.join(Q.project(functionalDependencies.get(invalid.getFirst()), (c)->c.getHeader()), ","),
                    tableId,
                    StringUtils.join(Q.project(invalid.getFirst(), (c)->c.getHeader()), ","),
                    StringUtils.join(Q.project(existingDependant, (c)->c.getHeader()), ",")
                    ));

                functionalDependencies.put(invalid.getFirst(), existingDependant);
            }

            return true;
        } 

        return false;
    }

    /**
     * Removes an invalidated FD from the set and makes sure that it also does not hold anymore via transitivity.
     * returns true only if an existing FD was removed.
     */
    public boolean removeInvalidatedFunctionalDependency(Pair<Set<TableColumn>,Set<TableColumn>> invalid) {

        // remove the invalidated dependency
        if(removeFunctionalDependency(invalid)) {
         
            return true;
        } else {
            return false;
        }
    }

    public Set<Pair<Set<TableColumn>,Set<TableColumn>>> propagateInvalidatedFD(Pair<Set<TableColumn>,Set<TableColumn>> invalid, Pair<Set<TableColumn>,Set<TableColumn>> basedOnAddedFD) {
        Set<Pair<Set<TableColumn>,Set<TableColumn>>> propagations = new HashSet<>();
        Set<TableColumn> existingDependant = functionalDependencies.get(invalid.getFirst());

        // propagete the invalidated FD if the condition (specified by the user) holds
        if(propagationCondition.invoke(invalid) && existingDependant!=null) {
            
            // propagate invalidation to other FDs with the same determinant
            Collection<TableColumn> additionalDependants = propagateInvalidationToDependants.invoke(invalid);

            

            // take care not to invalidate a trivial FD
            Collection<TableColumn> preparedAdditionalDependants = Q.without(additionalDependants, Q.union(invalid.getFirst(), invalid.getSecond()));
            // only propagate to dependants which actually exist (otherwise we would introduce an FD which does not exist, just to invalidate and specialise it)
            additionalDependants = Q.intersection(preparedAdditionalDependants, existingDependant);

            log(String.format("\t\tchecking propagations to %s: {%s} / intersection of {%s} and {%s}", 
                tableId,
                StringUtils.join(Q.project(additionalDependants, (c)->c.getHeader()), ","),
                StringUtils.join(Q.project(preparedAdditionalDependants, (c)->c.getHeader()), ","),
                StringUtils.join(Q.project(existingDependant, (c)->c.getHeader()), ",")
                ));

            if(additionalDependants.size()>0) {
                for(TableColumn additional : additionalDependants) {
                    if(additional!=null) {

                        Pair<Set<TableColumn>,Set<TableColumn>> propagatedInvalidation = new Pair<>(invalid.getFirst(), Q.toSet(additional));
                        
                        log(String.format("\t\tpropagating invalidation to %s: {%s}->{%s}", 
                            tableId,
                            StringUtils.join(Q.project(propagatedInvalidation.getFirst(), (c)->c.getHeader()), ","),
                            StringUtils.join(Q.project(propagatedInvalidation.getSecond(), (c)->c.getHeader()), ",")
                            ));

                        

                        removeInvalidatedFunctionalDependency(propagatedInvalidation);
                        // specialise the invalidated FD
                        propagations.addAll(specialise(propagatedInvalidation, basedOnAddedFD));
                  
                    }
                    
                }
            }
            
        }

        return propagations;
    }

    public Set<Pair<Set<TableColumn>, Set<TableColumn>>> specialise(Pair<Set<TableColumn>,Set<TableColumn>> fd, Pair<Set<TableColumn>,Set<TableColumn>> basedOnAddedFD) {
        Set<Pair<Set<TableColumn>, Set<TableColumn>>> specialisations = new HashSet<>();
        Collection<TableColumn> specialiseWithColumns = specialiseWith.invoke(new Pair<>(fd, basedOnAddedFD));

        log(String.format("\t\t\tspecialising with columns {%s}", 
            StringUtils.join(Q.project(specialiseWithColumns, (c)->c.getHeader()), ",")
            ));

        specialisations.addAll(Pair.fromMap(FunctionalDependencyUtils.specialise(fd, specialiseWithColumns)));

        Iterator<Pair<Set<TableColumn>, Set<TableColumn>>> it = specialisations.iterator();
        // for(Pair<Set<TableColumn>, Set<TableColumn>> specialisation : specialisations) {
        while(it.hasNext()) {
            Pair<Set<TableColumn>, Set<TableColumn>> specialisation = it.next();

            if(hasGeneralisation(specialisation, functionalDependencies)) {
                it.remove();
                log(String.format("\t\t\tskipping generalisable specialising %s: {%s}->{%s} to %s: {%s}->{%s}", 
                    tableId,
                    StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
                    StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
                    tableId,
                    StringUtils.join(Q.project(specialisation.getFirst(), (c)->c.getHeader()), ","),
                    StringUtils.join(Q.project(specialisation.getSecond(), (c)->c.getHeader()), ",")
                    ));
            } else {
                log(String.format("\t\t\tspecialising %s: {%s}->{%s} to %s: {%s}->{%s}", 
                    tableId,
                    StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
                    StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
                    tableId,
                    StringUtils.join(Q.project(specialisation.getFirst(), (c)->c.getHeader()), ","),
                    StringUtils.join(Q.project(specialisation.getSecond(), (c)->c.getHeader()), ",")
                    ));
            }
        }

        return specialisations;
    }

    public boolean containsSpecialisationOf(Pair<Set<TableColumn>,Set<TableColumn>> fd) {
    

        // check if the new FD is a subset of any existing determinant
        // - this step is needed because the next step does not seem to handle it properly for trivial FDs which are added for candidate keys
        Collection<Pair<Set<TableColumn>,Set<TableColumn>>> specialisations = Q.where(Pair.fromMap(functionalDependencies), (existing)->
            // the existing determinant contains the complete new FD
            existing.getFirst().containsAll(Q.union(fd.getFirst(), fd.getSecond()))
            ||
            // or the existing determinant is a superset of the new determinant and determines the same dependant
            existing.getFirst().containsAll(fd.getFirst()) && existing.getFirst().size()>fd.getFirst().size() && existing.getSecond().containsAll(fd.getSecond())

        );

        for(Pair<Set<TableColumn>,Set<TableColumn>> specialisation : specialisations) {
            log(String.format("\t\tskipping FD: violates minimality of {%s}->{%s}",
                StringUtils.join(Q.project(specialisation.getFirst(), (c)->c.getHeader()), ","),
                StringUtils.join(Q.project(specialisation.getSecond(), (c)->c.getHeader()), ",")
            ));
        }

        if(specialisations.size()>0) {
            return true;
        }

        return false;

    }
    
    /**
     * Adds a minimal functional dependency to this set. All existing FDs which are invalidated by the new FD are removed and possibly replaced by specialisations.
     */
    public void addVerifiedMinimalFunctionalDependency(Pair<Set<TableColumn>,Set<TableColumn>> fdToPropagateInOriginal) {
        addVerifiedMinimalFunctionalDependency(fdToPropagateInOriginal, true);
    }

    public void addVerifiedMinimalFunctionalDependency(Pair<Set<TableColumn>,Set<TableColumn>> fdToPropagateInOriginal, boolean propagate) {
        Queue<Pair<Set<TableColumn>,Set<TableColumn>>> verifiedFDs = new LinkedList<>(FunctionalDependencyUtils.split(fdToPropagateInOriginal));

        while(verifiedFDs.size()>0) {
            Pair<Set<TableColumn>,Set<TableColumn>> fd = verifiedFDs.poll();

            log(String.format("\taddVerifiedMinimalFunctionalDependency %s: {%s}->{%s}", 
                tableId,
                StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
                StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",")
                ));

            // if the new FD is a generalisation of an existing FD, do not propagate it
            // if we found {X}->{Y,Z} 
            // - it's a generalisation of {X,W}->{Y} and {X,W}->{Z}
            // - if only {X,W}->{Y} exists, we can still add {X}->{Z}
            if(
                !containsSpecialisationOf(fd)
            ) {

                // find all FDs which are invalidated by the new FD
                Collection<Pair<Set<TableColumn>, Set<TableColumn>>> invalidated = getInvalidatedFDs(fd);

                for(Pair<Set<TableColumn>,Set<TableColumn>> invalid : invalidated) {
                    if(verbose) {
                        // Set<TableColumn> dep = functionalDependencies.get(invalid.getFirst());
                        // if(dep!=null && dep.containsAll(invalid.getSecond())) {
                            log(String.format("\t\tinvalidated %s: {%s}->{%s}", 
                                tableId,
                                StringUtils.join(Q.project(invalid.getFirst(), (c)->c.getHeader()), ","),
                                StringUtils.join(Q.project(invalid.getSecond(), (c)->c.getHeader()), ",")
                                ));
                        // }
                    }

                    if(propagate) {
                        // propagate the invalidation
                        Set<Pair<Set<TableColumn>, Set<TableColumn>>> propagations = propagateInvalidatedFD(invalid,fd);

                        verifiedFDs.addAll(propagations);
                    }

                    // remove the invalidated FD
                    removeInvalidatedFunctionalDependency(invalid);
                }

                if(!fd.getFirst().containsAll(fd.getSecond())) { // skip trivial FDs)
                    // add the discovered FD
                    addFunctionalDependency(fd);
                    // addFunctionalDependency(closure);

                    // make sure this update did not cause inconsistencies
                    verifiedFDs.addAll(ensureConsistency(fd));
                }
                
            }
        }
    }

    public Set<Pair<Set<TableColumn>,Set<TableColumn>>> ensureConsistency(Pair<Set<TableColumn>,Set<TableColumn>> fd) {

        Set<Pair<Set<TableColumn>,Set<TableColumn>>> addedFDs = new HashSet<>();

        // check all existing FDs
        for(Pair<Set<TableColumn>, Set<TableColumn>> existing : FunctionalDependencyUtils.split(Pair.fromMap(functionalDependencies))) {

            // if the existing determinant is a proper subset of the new determinant
            // example: existing = {a}->{c}
            if(fd.getFirst().containsAll(existing.getFirst()) && fd.getFirst().size()>existing.getFirst().size()) {

                Set<TableColumn> simpleTransitive = functionalDependencies.get(existing.getSecond());

                if(
                    // check one-step transitivity
                    simpleTransitive!=null && simpleTransitive.containsAll(Q.without(fd.getFirst(), Q.union(existing.getFirst(), existing.getSecond())))
                ) {
                    // then, we must invalidate this FD, too
                    // example: existing = {a}->{c}

                    System.out.println(String.format("\t\tRemoving inconsistency: {%s}->{%s}->{%s} generalises {%s}->{%s}",
                        StringUtils.join(Q.project(existing.getFirst(), (c)->c.getHeader()), ","),
                        StringUtils.join(Q.project(existing.getSecond(), (c)->c.getHeader()), ","),
                        StringUtils.join(Q.project(simpleTransitive, (c)->c.getHeader()), ","),
                        StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
                        StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",")
                    ));

                    // remove the dependant {c} for {a}
                    removeInvalidatedFunctionalDependency(new Pair<>(existing.getFirst(), existing.getSecond()));

                }
            }
        }

        return addedFDs;
    }

	public String formatFunctionalDependencies() {
		StringBuilder sb = new StringBuilder();
		for(Collection<TableColumn> det : getFunctionalDependencies().keySet()) {
			Collection<TableColumn> dep = getFunctionalDependencies().get(det);
            sb.append(String.format("\t%s: {%s} -> {%s}\n", 
                tableId,
                StringUtils.join(Q.project(det, new TableColumn.ColumnHeaderProjection()), ","),
                StringUtils.join(Q.project(dep, new TableColumn.ColumnHeaderProjection()), ",")
                ));
		}
		return sb.toString();
    }
    
    	/**
	 * Returns the functional dependencies which will still exist if the table is projected using the specified columns.
	 * The result uses the columns of this Table instance and does not create new column instances.
	 */
	public Map<Set<TableColumn>,Set<TableColumn>> projectFunctionalDependencies(Collection<TableColumn> projectedColumns) throws Exception {
		Map<Set<TableColumn>,Set<TableColumn>> result = new HashMap<>();

		// copy functional dependencies
		for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(getFunctionalDependencies())) {
			Set<TableColumn> det = fd.getFirst();

			Set<TableColumn> dep = fd.getSecond();
			Set<TableColumn> depIntersection = Q.intersection(projectedColumns,dep);
			if (projectedColumns.containsAll(det) && depIntersection.size()>0) {
				result.put(det, depIntersection);
			}
		}

		return result;
	}

}