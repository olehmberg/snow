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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.StitchedModel;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.metanome.FDTreeWrapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class FunctionalDependencyPropagation {

    public void propagateDependencies(StitchedModel model, boolean verbose) {
        
        for(Table t : model.getTables().values()) {

            FDTreeWrapper fds = new FDTreeWrapper(t.getColumns());
            fds.setFunctionalDependencies(t.getSchema().getFunctionalDependencies());

            // check for possible propagation
            // - if {fk,date}->X holds in any table, why should it not hold in all tables with {fk,date}?
            // - must exclude FDs with union context columns in the determinant, because they might be page identifiers added due to incorrect table matching
            // for each FD X->Y
            // - if there exists and FD X'->Z with X a proper subset of X'
            // - then check if X' co-occurs with Y
            // - if yes, extend X->Y to X'->Y

            Collection<Pair<Set<TableColumn>, Set<TableColumn>>> allFDs = Pair.fromMap(fds.getFunctionalDependencies());
            Map<Set<TableColumn>, Set<TableColumn>> allClosures = StitchedFunctionalDependencyUtils.allClosures(t.getSchema().getFunctionalDependencies(), model);
            for(Pair<Set<TableColumn>,Set<TableColumn>> fd : FunctionalDependencyUtils.split(allFDs)) {

                if(
                    // skip FDs without determinant (which is always contained in all other tables...)
                    fd.getFirst().size()>0
                    && 
                    // also skip FDs which do not contain the FK, we will remove them in the next step anyways
                    Q.any(fd.getFirst(), (c)->"FK".equals(c.getHeader()))
                ) {

                    // check if there exists an FD with a superset of the current determinant
                    // which (the superset) always co-occurs with the current dependant
                    Set<Table> provForDependant = model.getStitchedColumnProvenance().get(Q.firstOrDefault(fd.getSecond()));

                    List<Set<TableColumn>> extendTo = new LinkedList<>();

                    for(Pair<Set<TableColumn>,Set<TableColumn>> extFD : allFDs) {
                        
                            if(
                                // extFD's determinant is a superset of fd's determinant
                                extFD.getFirst().containsAll(fd.getFirst()) && extFD.getFirst().size()>fd.getFirst().size()
                                &&
                                // if the extended FD determines only context attributes, do not propagate
                                !Q.all(extFD.getSecond(), (c)->ContextColumns.isRenamedUnionContextColumn(c.getHeader()))
                            ) {

                                // check that the additional attributes are not union-context attributes
                                Set<TableColumn> additional = new HashSet<>(Q.without(extFD.getFirst(), fd.getFirst()));
                                // remove all union-context attributes from the additional columns
                                additional = new HashSet<>(Q.where(additional, (c)->!ContextColumns.isRenamedUnionContextColumn(c.getHeader())));

                                // now check if the additional attributes always co-occur with the current dependant
                                Collection<Table> provForExt = model.getProvenanceForColumnCombination(additional);

                                // check if the provenance of tables which contain the extended version of the FD is the same as the prov of the extended determinant
                                boolean hasSharedProv = false;
                                for(TableColumn extDep : extFD.getSecond()) {
                                    Collection<Table> provForExtWithDep = model.getProvenanceForColumnCombination(new HashSet<>(Q.union(fd.getFirst(), additional, fd.getSecond(), Q.toSet(extDep))));
                                    if(provForExtWithDep.size()>0) {
                                        if(verbose) System.out.println(String.format("[FunctionalDependencyPropagation] skipping propagation {%s}->{%s} to {%s}, has provenance with {%s}",
                                            StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
                                            StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
                                            StringUtils.join(Q.project(extFD.getFirst(), (c)->c.getHeader()), ","),
                                            extDep.getHeader()
                                        ));
                                        hasSharedProv = true;
                                    }
                                }
                                if(hasSharedProv) {
                                    continue;
                                }

                                if(
                                    // the additional attribute must appear in all tables where the dependant appears
                                    provForExt.containsAll(provForDependant)
                                    // weaker condition: the additional attributes must at least once appear with the dependant
                                    // Q.intersection(provForExt, provForDependant).size()>0   
                                    &&
                                    // and skip FDs which have only a single table as provenance: we would propagate an extended FD within the same table
                                    provForExt.size()>1
                                ) {
                                    if(additional.size()>0) {
                                        if(verbose) System.out.println(String.format("[FunctionalDependencyPropagation] {%s}->{%s} is contained in {%s}/{%s}->{%s}",
                                            StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
                                            StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
                                            StringUtils.join(Q.project(Q.union(fd.getFirst(),additional), (c)->c.getHeader()), ","),
                                            StringUtils.join(Q.project(extFD.getFirst(), (c)->c.getHeader()), ","),
                                            StringUtils.join(Q.project(extFD.getSecond(), (c)->c.getHeader()), ",")
                                        ));

                                        // if yes, propagate
                                        Set<TableColumn> extendedDeterminant = new HashSet<>(Q.union(fd.getFirst(), additional));
                                        extendTo.add(extendedDeterminant);
                                    }
                                }
                            }
                    }

                    // we have found possible extensiosn
                    if(extendTo.size()>0) {

                        fds.removeNonFunctionalDependency(fd);

                        // remove all extensions which are subsets of other extensions
                        Iterator<Set<TableColumn>> it = extendTo.iterator();
                        while(it.hasNext()) {
                            Set<TableColumn> extendedDeterminant = it.next();
                            if(Q.any(extendTo, (other)->other.containsAll(extendedDeterminant) && other.size()>extendedDeterminant.size())) {
                                it.remove();
                            }
                        }

                        for(Set<TableColumn> extendedDeterminant : extendTo) {
                            if(verbose) System.out.println(String.format("[FunctionalDependencyPropagation]\textending {%s}->{%s} to {%s}->{%s}",
                                StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
                                StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
                                StringUtils.join(Q.project(extendedDeterminant, (c)->c.getHeader()), ","),
                                StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ",")
                            ));

                            Pair<Set<TableColumn>,Set<TableColumn>> extendedFD = new Pair<>(extendedDeterminant, fd.getSecond());
                            fds.addMinimalFunctionalDependency(extendedFD);

                            // if we extend X->Z to X,Y->Z then we must remove all other W->Z
                            // - if X,Y->W was in the original FDs, we have introduced a transitivity X,Y->W->Z
                            Set<TableColumn> extClosure = StitchedFunctionalDependencyUtils.closure(extendedDeterminant, t.getSchema().getFunctionalDependencies(), model);
                            for(Pair<Set<TableColumn>,Set<TableColumn>> otherFD : FunctionalDependencyUtils.split(allFDs)) {
                                // if a different determinant determines the same dependant
                                if(!otherFD.getFirst().equals(fd.getFirst()) && fd.getSecond().containsAll(otherFD.getSecond())) {
                                    Set<TableColumn> otherClosure = allClosures.get(otherFD.getFirst());
                                    if(
                                        // check if the extended determinant determines the other determinant
                                        extClosure.containsAll(otherFD.getFirst())
                                        &&
                                        // and there is no bijection
                                        !otherClosure.containsAll(extendedDeterminant)

                                    ) {
                                        // if yes, remove the other FD
                                        // - Note: the other FD will still be checked for propagation, and if possible also extended!
                                        fds.removeNonFunctionalDependency(otherFD);

                                        if(verbose) System.out.println(String.format("[FunctionalDependencyPropagation]\tremoving {%s}->{%s}, contained in {%s}+ = {%s}",
                                            StringUtils.join(Q.project(otherFD.getFirst(), (c)->c.getHeader()), ","),
                                            StringUtils.join(Q.project(otherFD.getSecond(), (c)->c.getHeader()), ","),
                                            StringUtils.join(Q.project(extendedDeterminant, (c)->c.getHeader()), ","),
                                            StringUtils.join(Q.project(extClosure, (c)->c.getHeader()), ",")
                                        ));
                                    }
                                }
                            }
                        }

                    }

                }

            }

            t.getSchema().setFunctionalDependencies(fds.getFunctionalDependencies());
        }
    }

    // public void removePartialTransitivity(StitchedModel model) {
    //     for(Table t : model.getTables().values()) {

    //         FDTreeWrapper fds = new FDTreeWrapper(t.getColumns());

    //         // if we do not add the closure to the FDTree, we can loose candidate keys when removing FDs!
    //         for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(StitchedFunctionalDependencyUtils.allClosures(t.getSchema().getFunctionalDependencies(), model))) {
    //         // for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(t.getSchema().getFunctionalDependencies())) {
    //             fds.addMinimalFunctionalDependency(fd);
    //         }

    //         // t.getSchema().setFunctionalDependencies(fds.getFunctionalDependencies());

    //         FunctionalDependencyPropagation prop = new FunctionalDependencyPropagation();
    //         prop.removePartialTransitivity(fds, t, model);

    //         fds.minimise();

    //         t.getSchema().setFunctionalDependencies(fds.getFunctionalDependencies());
    //     }
    // }

    // public void removePartialTransitivity(FDTreeWrapper fds, Table t, StitchedModel model) {
    //     // remove partial transitivities
    //     Map<Set<TableColumn>, Set<TableColumn>> allClosures = StitchedFunctionalDependencyUtils.allClosures(t.getSchema().getFunctionalDependencies(), model);
    //     // for(Pair<Set<TableColumn>,Set<TableColumn>> fd : FunctionalDependencyUtils.split(Pair.fromMap(t.getSchema().getFunctionalDependencies()))) {
    //     for(Pair<Set<TableColumn>,Set<TableColumn>> fd : Pair.fromMap(t.getSchema().getFunctionalDependencies())) {
    //         removePartialTransitivity(fd, fds, t, model, allClosures);
    //     }
    // }

    // public void removePartialTransitivity(Pair<Set<TableColumn>,Set<TableColumn>> fd, FDTreeWrapper fds, Table t, StitchedModel model, Map<Set<TableColumn>, Set<TableColumn>> allClosures) {
    //     Collection<Pair<Set<TableColumn>, Set<TableColumn>>> allFDs = Pair.fromMap(t.getSchema().getFunctionalDependencies());
    //     //TODO move this part to FKFDFilter where it can apply to all (not only propagated) FDs
    //     //CHECK might not be necessay in general? was it an effect of the propagation?
    //     // - if there is an overlapping transitivity, we assume that it is coincidental
    //     // - i.e., FK,X->Y,Z and FK,Y->Z but not FK,Y->X
    //     // - then assume FK,Y->Z is coincidental
    //     // if we extend X->Z to X,Y->Z then we must remove all other W->Z
    //     // - if X,Y->W was in the original FDs, we have introduced a transitivity X,Y->W->Z

    //     // we want to remove FDs which are weaker, but determine the same dependant (remove BCNF violations?)
    //     // we do not want to remove FDs which are weaker and determine a different dependant (don't remove 2NF violations)
         
    //     // we use extended FDs here, so no need to calculate the closure
    //     // Map<Set<TableColumn>, Set<TableColumn>> allClosures = new HashMap<>(t.getSchema().getFunctionalDependencies());

    //     for(Pair<Set<TableColumn>,Set<TableColumn>> otherFD : allFDs) {
    //         // if a different determinant determines the same dependant

    //         boolean logKeep = false;
    //         boolean logRemove = true;

    //         if(Q.project(otherFD.getFirst(), (c)->c.getHeader()).containsAll(Q.toSet("page title","FK", " annual mean wage"))) {
    //             logRemove = true;
    //         }

    //         if(
    //             Q.project(otherFD.getFirst(), (c)->c.getHeader()).containsAll(Q.toSet("page title","FK", " annual mean wage"))
    //             &&
    //             Q.project(fd.getFirst(), (c)->c.getHeader()).containsAll(Q.toSet("page title","FK","month","year"))
    //         ) {
    //                 logKeep=true;
    //         }

    //         // if(logAll) {
    //         //     System.out.println(String.format("Checking if {%s}->{%s} contains {%s}->{%s}",
    //         //         StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
    //         //         StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
    //         //         StringUtils.join(Q.project(otherFD.getFirst(), (c)->c.getHeader()), ","),
    //         //         StringUtils.join(Q.project(otherFD.getSecond(), (c)->c.getHeader()), ",")
    //         //     ));
    //         // }

    //         // we use extended FDs, so if otherFD is a proper subset of FD, do not remove it!
    //         if(!otherFD.getFirst().equals(fd.getFirst()) && !fd.getFirst().containsAll(otherFD.getFirst())) {
    //             // Set<TableColumn> extClosure = StitchedFunctionalDependencyUtils.closure(fd.getFirst(), t.getSchema().getFunctionalDependencies(), model);        
    //             // Set<TableColumn> otherClosure = StitchedFunctionalDependencyUtils.closure(otherFD.getFirst(), t.getSchema().getFunctionalDependencies(), model);
    //             Set<TableColumn> extClosure = allClosures.get(fd.getFirst());
    //             Set<TableColumn> otherClosure = allClosures.get(otherFD.getFirst());
    //             // Set<TableColumn> extClosure = new HashSet<>(Q.union(fd.getFirst(), fd.getSecond()));
    //             // Set<TableColumn> otherClosure = new HashSet<>(Q.union(otherFD.getFirst(), otherFD.getSecond()));
    //             if(
    //                 // check if the extended determinant determines the other determinant
    //                 // extClosure.containsAll(otherFD.getFirst())
    //                 extClosure.containsAll(otherClosure)
    //                 &&
    //                 // and there is no bijection
    //                 // !StitchedFunctionalDependencyUtils.closure(otherFD.getFirst(), t.getSchema().getFunctionalDependencies(), model).containsAll(fd.getFirst())
    //                 // !otherClosure.containsAll(fd.getFirst())
    //                 !otherClosure.containsAll(extClosure)
    //                 // &&
    //                 // if the extended FD already determines the dependant, do not propagate?
    //                 // - this would extend an FD that should have been discovered if it existed
    //                 // - should not be possible here (but can be during the propagation step!): if a subset of fd's det determines its dep, then it is not minimal!
    //                 // !extClosure.containsAll(fd.getSecond())
    //             ) {
    //                 // if yes, remove the other FD
    //                 // if fd.getSecond().containsAll(otherFD.getSecond())

    //                 // only remove dependants which are determined by the current FD
    //                 // - doesn't make sense, the current FD has the complete other FD in its closure
    //                 // - does make sense ... an FD might gain and loose different attributes in its dependant ...
    //                 // -- X->A
    //                 // -- by closure, added B to X->A,B
    //                 // -- then remove A, changed to X->B
    //                 // -- if we remove X->A,B completely, X->B is lost
    //                 Set<TableColumn> detToRemove = new HashSet<>(Q.intersection(Q.union(fd.getFirst(),fd.getSecond()), otherFD.getSecond()));

    //                 // fds.removeNonFunctionalDependency(otherFD);
    //                 fds.removeNonFunctionalDependency(new Pair<>(otherFD.getFirst(), detToRemove));
    //                 // fds.removeNonFunctionalDependency(new Pair<>(otherFD.getFirst(), otherClosure)); // removing the closure seems to also remove all FDs with subsets of the determinant

    //                 if(logRemove) {
    //                     System.out.println(String.format("[FunctionalDependencyPropagation]\tremoving {%s}->{%s}, contained in {%s}+ = {%s}",
    //                         StringUtils.join(Q.project(otherFD.getFirst(), (c)->c.getHeader()), ","),
    //                         StringUtils.join(Q.project(otherFD.getSecond(), (c)->c.getHeader()), ","),
    //                         StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
    //                         StringUtils.join(Q.project(extClosure, (c)->c.getHeader()), ",")
    //                     ));
    //                 }
    //             } else {
    //                 if(logKeep) {
    //                     System.out.println(String.format("[FunctionalDependencyPropagation] keeping {%s}->{%s} / {%s}+\\{%s}+={%s} / {%s}+\\{%s}+={%s}",
    //                         StringUtils.join(Q.project(otherFD.getFirst(), (c)->c.getHeader()), ","),
    //                         StringUtils.join(Q.project(otherFD.getSecond(), (c)->c.getHeader()), ","),
    //                         StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
    //                         StringUtils.join(Q.project(otherFD.getFirst(), (c)->c.getHeader()), ","),
    //                         StringUtils.join(Q.project(Q.without(extClosure, otherClosure), (c)->c.getHeader()), ","),
    //                         StringUtils.join(Q.project(otherFD.getFirst(), (c)->c.getHeader()), ","),
    //                         StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
    //                         StringUtils.join(Q.project(Q.without(otherClosure, extClosure), (c)->c.getHeader()), ",")
    //                     ));
    //                 }
    //             }
    //         }
    //     }
    // }

}