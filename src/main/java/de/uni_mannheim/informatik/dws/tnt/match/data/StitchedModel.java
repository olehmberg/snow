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
package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import de.uni_mannheim.informatik.dws.snow.SchemaNormalisation.NormalisationHeuristic;
import de.uni_mannheim.informatik.dws.snow.SchemaNormalisation.StitchingMethod;
import de.uni_mannheim.informatik.dws.tnt.match.DeterminantSelector;
import de.uni_mannheim.informatik.dws.tnt.match.TableReconstructor;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FDScorer;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencySet;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.NFSFunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.NFSWebTableNormaliser;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.StitchedFunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.StitchedWebTableNormaliser;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.WebTableNormaliser;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.CorrespondenceProjector;
import de.uni_mannheim.informatik.dws.tnt.match.entitystitching.EntityBasedCorrespondenceFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.BothKeysFullyMappedFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.BothTablesFullyMappedFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.MaxColumnsMappedFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.NonContextColumnMappedFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.OneKeyFullyMappedFilter;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.OneTableFullyMappedFilter;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.metanome.FDTreeWrapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class StitchedModel {

	Collection<EntityTable> entityGroups;
	DataSet<MatchableTableRow, MatchableTableColumn> records;
	DataSet<MatchableTableColumn, MatchableTableColumn> attributes;
	DataSet<MatchableTableDeterminant, MatchableTableColumn> candidateKeys;
	Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences;
    Map<Integer, Table> tables;
    Map<Table, Collection<Table>> stitchedProvenance;
    Map<TableColumn, Set<Table>> stitchedColumnProvenance;
    Set<Set<TableColumn>> existingColumnCombinationsInProvenance;
    
    /**
     * @return the entityGroups
     */
    public Collection<EntityTable> getEntityGroups() {
        return entityGroups;
    }
    /**
     * @param entityGroups the entityGroups to set
     */
    public void setEntityGroups(Collection<EntityTable> entityGroups) {
        this.entityGroups = entityGroups;
    }
    /**
     * @return the records
     */
    public DataSet<MatchableTableRow, MatchableTableColumn> getRecords() {
        return records;
    }
    /**
     * @param records the records to set
     */
    public void setRecords(DataSet<MatchableTableRow, MatchableTableColumn> records) {
        this.records = records;
    }
    /**
     * @return the attributes
     */
    public DataSet<MatchableTableColumn, MatchableTableColumn> getAttributes() {
        return attributes;
    }
    /**
     * @param attributes the attributes to set
     */
    public void setAttributes(DataSet<MatchableTableColumn, MatchableTableColumn> attributes) {
        this.attributes = attributes;
    }
    /**
     * @return the candidateKeys
     */
    public DataSet<MatchableTableDeterminant, MatchableTableColumn> getCandidateKeys() {
        return candidateKeys;
    }
    /**
     * @param candidateKeys the candidateKeys to set
     */
    public void setCandidateKeys(DataSet<MatchableTableDeterminant, MatchableTableColumn> candidateKeys) {
        this.candidateKeys = candidateKeys;
    }
    /**
     * @return the schemaCorrespondences
     */
    public Processable<Correspondence<MatchableTableColumn, Matchable>> getSchemaCorrespondences() {
        return schemaCorrespondences;
    }
    /**
     * @param schemaCorrespondences the schemaCorrespondences to set
     */
    public void setSchemaCorrespondences(
            Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
        this.schemaCorrespondences = schemaCorrespondences;
    }
    /**
     * @return the tables
     */
    public Map<Integer, Table> getTables() {
        return tables;
    }
    /**
     * @param tables the tables to set
     */
    public void setTables(Map<Integer, Table> tables) {
        this.tables = tables;
    }

    /**
     * @return the stitchedProvenance
     */
    public Map<Table, Collection<Table>> getStitchedProvenance() {
        return stitchedProvenance;
    }

    /**
     * @param stitchedProvenance the stitchedProvenance to set
     */
    public void setStitchedProvenance(Map<Table, Collection<Table>> stitchedProvenance) {
        this.stitchedProvenance = stitchedProvenance;

        existingColumnCombinationsInProvenance = new HashSet<>();
        stitchedColumnProvenance = new HashMap<>();
        for(Table t : stitchedProvenance.keySet()) {
            Collection<Table> stitchedTables = stitchedProvenance.get(t);
            
            // collect the provenance for each column: the set of tables that contained the column
            for(TableColumn c : t.getColumns()) {
                for(Table stitchedTable : stitchedTables) {
                    Collection<TableColumn> stitchedColumns = Q.where(stitchedTable.getColumns(), (c0)->c.getProvenance().contains(c0.getIdentifier()) || c0.getIdentifier().equals(c.getIdentifier()));

                    if(stitchedColumns.size()>0) {
                        MapUtils.get(stitchedColumnProvenance, c, new HashSet<>()).add(stitchedTable);
                    }
                }
            }

            // pre-fill the provenance cache
            for(Table stitchedTable : stitchedTables) {
                Set<String> prov = new HashSet<>();
                for(TableColumn c : stitchedTable.getColumns()) {
                    prov.add(c.getIdentifier());
                }
                
                Set<TableColumn> existingColumnCombination = new HashSet<>();
                Collection<TableColumn> stitchedColumns = Q.where(t.getColumns(), (c)->Q.intersection(c.getProvenance(),prov).size()>0 || prov.contains(c.getIdentifier()));
                existingColumnCombination.addAll(stitchedColumns);
                existingColumnCombinationsInProvenance.add(existingColumnCombination);
            }
            
        }
    }

    /**
     * @return the stitchedColumnProvenance
     */
    public Map<TableColumn, Set<Table>> getStitchedColumnProvenance() {
        return stitchedColumnProvenance;
    }

    /**
     * @return the existingColumnCombinationsInProvenance
     */
    public Set<Set<TableColumn>> getExistingColumnCombinationsInProvenance() {
        return existingColumnCombinationsInProvenance;
    }

    public void loadCandidateKeys() {
        WebTableDataSetLoader loader = new WebTableDataSetLoader();
        candidateKeys = loader.loadForeignKeyDataSet(tables.values(), records);
    }

    public void filterSchemaCorrespondencesByEntityGroups() {
		// filter the schema correspondences according to the entity groups
    	EntityBasedCorrespondenceFilter filter = new EntityBasedCorrespondenceFilter();
    	System.out.println(String.format("%d correspondences before filtering by entity", schemaCorrespondences.size()));
    	schemaCorrespondences = filter.filterCorrespondences(schemaCorrespondences, entityGroups);
    	System.out.println(String.format("%d correspondences after filtering by entity", schemaCorrespondences.size()));
	}

    public void updateModel(Collection<Table> tables, Collection<Table> previousTables, boolean verbose) {
		System.out.println(String.format("[updateModel] Projecting %d schema correspondences", schemaCorrespondences.size()));
		WebTableDataSetLoader loader = new WebTableDataSetLoader();
		records = loader.loadRowDataSet(tables);
		schemaCorrespondences = CorrespondenceProjector.projectCorrespondences(schemaCorrespondences, records.getSchema(), tables, attributes,verbose);
		schemaCorrespondences = schemaCorrespondences.append(CorrespondenceProjector.createCorrespondencesFromProjection(previousTables, tables, records.getSchema()));
		attributes = records.getSchema();
		candidateKeys = loader.loadCandidateKeyDataSet(tables, records);
		System.out.println(String.format("[updateModel] %d schema correspondences after projection", schemaCorrespondences.size()));
	}

    public StitchedModel copy() {
        StitchedModel model = new StitchedModel();

        model.setTables(new HashMap<>(tables));
        model.setAttributes(new ParallelHashedDataSet<>(attributes.copy().get()));
        model.setRecords(new ParallelHashedDataSet<>(records.copy().get()));
        for(MatchableTableColumn c : model.getAttributes().get()) {
            model.getRecords().addAttribute(c);
        }
        model.setSchemaCorrespondences(schemaCorrespondences.copy());
        model.setCandidateKeys(new ParallelHashedDataSet<>(candidateKeys.copy().get()));
        model.setEntityGroups(new ArrayList<>(entityGroups));

        return model;
    }

    public void printTablesAndFDs() {
        for(Table t : tables.values()) {
            System.out.println(String.format("[StitchedModel] Table #%d %s: {%s}",
                t.getTableId(),
                t.getPath(),
                StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
            ));
            System.out.println(t.formatFunctionalDependencies());
        }
    }

	public StitchedModel createUniversalSchema(boolean addData) {
        StitchedModel universal = new StitchedModel();
		TableReconstructor tr = new TableReconstructor(tables);
		int nextTableId = Q.<Integer>max(Q.project(tables.values(), (t)->t.getTableId()))+1;
		tr.setKeepUnchangedTables(true);
		tr.setOverlappingAttributesOnly(false);
		tr.setClusterByOverlappingAttributes(false);
		tr.setMergeToSet(false);
		tr.setCreateNonOverlappingClusters(true);
        // tr.setVerifyDependencies(true);	
        tr.setVerifyDependencies(false);	
        tr.setPropagateDependencies(false);
//			tr.setLog(log);
//			tr.setLogDirectory(logDirectory);
		WebTableDataSetLoader loader = new WebTableDataSetLoader();
        DataSet<MatchableTableDeterminant, MatchableTableColumn> functionalDependencies = loader.loadFunctionalClosureDataSet(tables.values(), records);

        //DEBUG print FDs
        for(MatchableTableDeterminant fd : functionalDependencies.sort((fd)->fd.getTableId()).get()) {
            System.out.println(String.format("[createUniversalSchema] input FD #%d: {%s}->{%s}",
                fd.getTableId(),
                StringUtils.join(Q.project(fd.getColumns(), (c)->c.getHeader()), ","),
                StringUtils.join(Q.project(fd.getDependant(), (c)->c.getHeader()), ",")
            ));
        }

		Collection<Table> result = tr.reconstruct(nextTableId, addData ? getRecords() : new ProcessableCollection<>(), attributes, candidateKeys, functionalDependencies, schemaCorrespondences);
        universal.setTables(Q.map(result, (t)->t.getTableId()));
        universal.setStitchedProvenance(tr.getStitchedProvenance());

        if(!addData) {
            // all unstitched tables still have their rows, remove them to get only the schema
            for(Table t : result) {
                if(t.getSize()>0) {
                    t.clear();
                }
            }
        }

        // table reconstructor only uses candidate keys which exist in all tables, so here they will likely all be rejected
        for(Table t : result) {
            t.getSchema().setCandidateKeys(FunctionalDependencyUtils.listCandidateKeys(t));
        }

		if(result.size()>0) {
			// update schema correspondences, record, and attribute datasets
			// important: update the original schema correspondences, not the filtered ones!
			// the filtered correspondences were used for stitching, so they only connect one column to itself after stitching
			System.out.println(String.format("[createUniversalSchema] Projecting %d schema correspondences", schemaCorrespondences.size()));
			
			universal.setRecords(loader.loadRowDataSet(result));
			Processable<Correspondence<MatchableTableColumn, Matchable>> cors = CorrespondenceProjector.projectCorrespondences(schemaCorrespondences, universal.getRecords().getSchema(), result, attributes, false).distinct();
			// remove correspondences between a single column (two corresponding columns were stitched together)
            cors = cors.where((c)->c.getFirstRecord().getIdentifier()!=c.getSecondRecord().getIdentifier());
            universal.setSchemaCorrespondences(cors);
			universal.setAttributes(universal.getRecords().getSchema());
			System.out.println(String.format("[createUniversalSchema] %d schema correspondences after projection", schemaCorrespondences.size()));
			
			System.out.println(String.format("[createUniversalSchema] %d attributes / %d records in total after table stitching", universal.getAttributes().size(), universal.getRecords().size()));
		} else {
			System.out.println("[createUniversalSchema] Stitching did not result in any new tables!");
        }
        
        return universal;
    }
    
    public StitchedModel normalise(
        NormalisationHeuristic normalisationHeuristic
        , boolean limitToFK
        , boolean addFullClosure
        , boolean verbose
        , Map<Table, Set<TableColumn>> nullFreeSubschemes
    ) throws Exception {
        NFSWebTableNormaliser normaliser = new NFSWebTableNormaliser();
        Collection<Table> normalised = new LinkedList<>();
        int nextTableId = Q.<Integer>max(Q.project(tables.values(), (t)->t.getTableId()))+1;
		
		for(Table t : tables.values()) {
			
			TableColumn foreignKey = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
						
			System.out.println(String.format("[normalise] table #%d %s: {%s}", t.getTableId(), t.getPath(), StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
			if(verbose) {
				System.out.println(t.formatFunctionalDependencies());
			}
			
			// if(normalisationHeuristic==NormalisationHeuristic._2NF) {
			// 	System.out.println(String.format("Normalising %s to 2NF", t.getPath()));
			// 	Collection<Table> normalisedTables = normaliser.normaliseTo2NF(t, nextTableId, limitToFK ? foreignKey : null, addFullClosure, nullFreeSubschemes.get(t));
			// 	for(Table n : normalisedTables) {
			// 		n.setPath(String.format("%d.json", n.getTableId()));
			// 	}
			// 	normalised.addAll(normalisedTables);
			// 	nextTableId+=normalised.size();
            // } else 
            if(normalisationHeuristic==NormalisationHeuristic._3NF) {
				System.out.println(String.format("Normalising %s to 3NF", t.getPath()));
				Collection<Table> normalisedTables = normaliser.normaliseTo3NF(t, nextTableId, limitToFK ? foreignKey : null, addFullClosure, nullFreeSubschemes.get(t));
				for(Table n : normalisedTables) {
					n.setPath(String.format("%d.json", n.getTableId()));
				}
				normalised.addAll(normalisedTables);
                // nextTableId+=normalised.size();
                nextTableId+=normalisedTables.size();
			} else {
				normalised.add(t);
			}

        }
        
        StitchedModel normalisedModel = new StitchedModel();
        normalisedModel.setTables(Q.map(normalised, (t)->t.getTableId()));
        normalisedModel.setRecords(records);
        normalisedModel.setSchemaCorrespondences(schemaCorrespondences);
        normalisedModel.setAttributes(attributes);
        normalisedModel.updateModel(normalisedModel.getTables().values(), getTables().values(), false);
        return normalisedModel;
    }

    Map<Table, Pair<Map<Set<TableColumn>,Set<TableColumn>>,Map<Set<TableColumn>,Set<TableColumn>>>> strongAndWeakFDs = null;
    public Map<Table, Pair<Map<Set<TableColumn>,Set<TableColumn>>,Map<Set<TableColumn>,Set<TableColumn>>>> getStrongAndWeakFunctionalDependencies() {
        if(strongAndWeakFDs!=null) {
            return strongAndWeakFDs;
        } else {

            Map<Table, Pair<Map<Set<TableColumn>,Set<TableColumn>>,Map<Set<TableColumn>,Set<TableColumn>>>> strongAndWeakFDs = new HashMap<>();

            for(Table t : getTables().values()) {                
                // separate FDs into strong and weak FDs
                FunctionalDependencySet strongFDs = new FunctionalDependencySet(new HashMap<>());
                FunctionalDependencySet weakFDs = new FunctionalDependencySet(new HashMap<>());
                for(Pair<Set<TableColumn>,Set<TableColumn>> fd : FunctionalDependencyUtils.split(Pair.fromMap(t.getSchema().getFunctionalDependencies()))) {

                    if(isStrongFunctionalDependency(fd)) {
                        strongFDs.addFunctionalDependency(fd);
                    } else {
                        weakFDs.addFunctionalDependency(fd);
                    }
                }

                strongAndWeakFDs.put(t, new Pair<>(strongFDs.getFunctionalDependencies(), weakFDs.getFunctionalDependencies()));
            }

            return strongAndWeakFDs;
        }
    }

    public boolean isExistingColumnCombination(Set<TableColumn> columnCombination) {
        for(Set<TableColumn> cc : existingColumnCombinationsInProvenance) {
            if(cc.containsAll(columnCombination)) {
                return true;
            } else {
                // System.out.println(String.format("{%s} not contained in {%s}",
                //     StringUtils.join(Q.project(columnCombination, (c)->c.toString()), ","),
                //     StringUtils.join(Q.project(cc, (c)->c.toString()), ",")
                // ));
            }
        }
        return false;
    }

    public boolean isAlwaysStrongFunctionalDependency(Pair<Set<TableColumn>,Set<TableColumn>> fd) {

        if(fd.getSecond().size()==0 || fd.getFirst().size()==0) {
            return true;
        }

        // whenever the dependant occurs in a table, the determinant must also be present
        Set<Table> depProv = null;

        for(TableColumn c : fd.getSecond()) {
            if(depProv==null) {
                depProv = stitchedColumnProvenance.get(c);
            } else {
                depProv = Q.intersection(depProv, stitchedColumnProvenance.get(c));
            }
        }

        Set<Table> detProv = null;

        for(TableColumn c : fd.getFirst()) {
            if(detProv==null) {
                detProv = stitchedColumnProvenance.get(c);
            } else {
                detProv = Q.intersection(detProv, stitchedColumnProvenance.get(c));
            }
        }

        if(detProv==null) {
            System.err.println(String.format("No provenance information available for determinant: {%s}",
                StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ",")
            ));
        }

        return detProv.containsAll(depProv);
    }

    public boolean isStrongFunctionalDependency(Pair<Set<TableColumn>,Set<TableColumn>> fd) {
        Set<TableColumn> columnCombination = new HashSet<>(Q.union(fd.getFirst(), fd.getSecond()));
        return Q.any(existingColumnCombinationsInProvenance, (cc)->cc.containsAll(columnCombination));
    }

    public Collection<Table> getProvenanceForColumnCombination(Set<TableColumn> columns) {
        Table t = Q.firstOrDefault(Q.where(getTables().values(), (t0)->t0.getColumns().containsAll(columns)));

        if(t==null) {
            // System.out.println(String.format("[StitchedModel.getProvenanceForColumnCombination] Invalid column combination: {%s}",
            //     StringUtils.join(Q.project(columns, (c)->c.getHeader()), ",")
            // ));
            return null;
        }

        Collection<Table> stitchedTables = getStitchedProvenance().get(t);

        Set<Table> tables = new HashSet<>(stitchedTables);
        for(TableColumn c : columns) {
            tables = Q.intersection(tables, stitchedColumnProvenance.get(c));
        }

        return tables;
    }

    public Set<TableColumn> isNullFreeDeterminantFor(Set<TableColumn> determinant) {
        Set<TableColumn> result = null;

        Map<TableColumn, Set<TableColumn>> nullFreePartitions = getNullFreePartitions();

        for(TableColumn c : determinant) {
            if(result==null) {
                result = nullFreePartitions.get(c);
            } else {
                result = Q.intersection(result, nullFreePartitions.get(c));
            }
        }

        return result;
    }

    public Map<TableColumn, Set<TableColumn>> getNullFreePartitions() {
        Map<TableColumn, Set<TableColumn>> result = new HashMap<>();

        for(Table t : getTables().values()) {
            
            for(TableColumn c : t.getColumns()) {
                Set<Table> prov = stitchedColumnProvenance.get(c);

                Set<TableColumn> partition = new HashSet<>();

                for(TableColumn c2 : t.getColumns()) {
                    if(c2!=c) {
                        if(stitchedColumnProvenance.get(c2)==null) {
                            System.err.println(String.format("No stitched provenance information available for column '%s'!", c2.toString()));
                        }

                        // c2 occurs in all tables where c occurs
                        if(stitchedColumnProvenance.get(c2).containsAll(prov)) {
                            partition.add(c2);
                        }
                    }
                }
                result.put(c, partition);
            }
        }

        return result;
    }

    public StitchedModel normalise(
        NormalisationHeuristic normalisationHeuristic
        , boolean limitToFK
        , boolean addFullClosure
        , boolean verbose
        , boolean useNFDs
    ) throws Exception {
        
        Collection<Table> normalised = new LinkedList<>();
        int nextTableId = Q.<Integer>max(Q.project(tables.values(), (t)->t.getTableId()))+1;
        
        Map<Table, Collection<Table>> provenance = new HashMap<>();

        if(stitchedProvenance!=null) {
            Map<Table, Set<TableColumn>> nullFreeSubschemes = new HashMap<>();
            for(Table t : getTables().values()) {

                TableColumn foreignKey = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));

                // define null-free subschemes (NFS) over the universal relation
                // - a null-free subscheme in our case is the set of attributes that occurred in all stitched tables
                Set<TableColumn> nfs = new HashSet<>();
                Collection<Table> stitchedTables = getStitchedProvenance().get(t);
                if(stitchedTables!=null) {
                    // add all columns which have all tables in their provenance
                    for(TableColumn c : stitchedColumnProvenance.keySet()) {
                        if(stitchedColumnProvenance.get(c).containsAll(stitchedTables)) {
                            nfs.add(c);
                        }
                    }
                    nullFreeSubschemes.put(t, nfs);
                    // separate FDs into strong and weak FDs
                    FunctionalDependencySet strongFDs = new FunctionalDependencySet(new HashMap<>());
                    FunctionalDependencySet weakFDs = new FunctionalDependencySet(new HashMap<>());
                    for(Pair<Set<TableColumn>,Set<TableColumn>> fd : FunctionalDependencyUtils.split(Pair.fromMap(t.getSchema().getFunctionalDependencies()))) {
                        Set<Table> tablesCoveringDeterminant = new HashSet<>(stitchedTables);
                        for(TableColumn c : fd.getFirst()) {
                            tablesCoveringDeterminant = Q.intersection(tablesCoveringDeterminant, stitchedColumnProvenance.get(c));
                        }
                        Set<Table> tablesCoveringDependant = new HashSet<>();
                        if(fd.getSecond().size()>0) {
                            tablesCoveringDependant = stitchedColumnProvenance.get(Q.firstOrDefault(fd.getSecond()));
                        }

                        if(tablesCoveringDeterminant.containsAll(tablesCoveringDependant)) {
                            strongFDs.addFunctionalDependency(fd);
                        } else {
                            if(fd.getSecond().size()>0) {
                                weakFDs.addFunctionalDependency(fd);
                            }
                        }
                    }

                    System.out.println(String.format("Universal table #%d {%s}",
                        t.getTableId(),
                        StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
                    ));
                    System.out.println("Strong FDs");
                    System.out.println(strongFDs.formatFunctionalDependencies());
                    System.out.println("Weak FDs");
                    System.out.println(weakFDs.formatFunctionalDependencies());
                }

                if(useNFDs) {
                    NFSWebTableNormaliser normaliser = new NFSWebTableNormaliser();
                    // calculate the canonical cover using the closure algorithm for NFDs with an NFs
                    t.getSchema().setFunctionalDependencies(NFSFunctionalDependencyUtils.canonicalCover(t.getSchema().getFunctionalDependencies(), nfs, true));
                    t.getSchema().setCandidateKeys(NFSFunctionalDependencyUtils.listCandidateKeys(t, nfs));

                    System.out.println(String.format("Normalising %s to 3NF", t.getPath()));
                    Collection<Table> normalisedTables = normaliser.normaliseTo3NF(t, nextTableId, limitToFK ? foreignKey : null, addFullClosure, nullFreeSubschemes.get(t));
                    for(Table n : normalisedTables) {
                        n.setPath(String.format("%d.json", n.getTableId()));
                    }
                    normalised.addAll(normalisedTables);
                    // nextTableId+=normalised.size();
                    nextTableId+=normalisedTables.size();
                } else {
                    StitchedWebTableNormaliser normaliser = new StitchedWebTableNormaliser();
                    
                    if(normalisationHeuristic==NormalisationHeuristic._2NF) {
                        System.out.println(String.format("Normalising %s to 2NF", t.getPath()));
                        Collection<Table> normalisedTables = normaliser.normaliseTo2NF(t, nextTableId, limitToFK ? foreignKey : null, addFullClosure);
                        for(Table n : normalisedTables) {
                            n.setPath(String.format("%d.json", n.getTableId()));
                        }
                        normalised.addAll(normalisedTables);
                        nextTableId+=normalised.size();
                    } else if(normalisationHeuristic==NormalisationHeuristic._3NF) {
                        System.out.println(String.format("Calculating canonical cover for universal relation #%d", t.getTableId()));
                        TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
                        t.getSchema().setFunctionalDependencies(StitchedFunctionalDependencyUtils.focusedCanonicalCover(t.getSchema().getFunctionalDependencies(), Q.toSet(fk), this, false));
                        System.out.println(String.format("Calculating candidate keys for universal relation #%d", t.getTableId()));
                        t.getSchema().setCandidateKeys(FunctionalDependencyUtils.listCandidateKeys(t));

                        System.out.println(String.format("Normalising %s to 3NF", t.getPath()));
                        Collection<Table> normalisedTables = normaliser.normaliseTo3NF(t, nextTableId, limitToFK ? foreignKey : null, addFullClosure, this);
                        for(Table n : normalisedTables) {
                            n.setPath(String.format("%d.json", n.getTableId()));
                        }
                        normalised.addAll(normalisedTables);
                        nextTableId+=normalised.size();
                    } else if(normalisationHeuristic==NormalisationHeuristic.None) {
                        normalised.add(t);
                    } else { // BCNF
                        FDScorer scorer = new FDScorer();
                        scorer.addScoringFunction(FDScorer.dataTypeScore, "data type score");
                        scorer.addScoringFunction(FDScorer.lengthScore, "length score");
                        scorer.addScoringFunction(FDScorer.extensionalDeterminantProvenanceScore, "extensional determinant provenance score");
                        scorer.addScoringFunction(FDScorer.disambiguationOfForeignKey, "disambiguation of FK score");
                        scorer.addScoringFunction(FDScorer.filenameScore, "filename score");
                        scorer.addScoringFunction(FDScorer.disambiguationSplitScore, "disambiguation split score");

                        System.out.println(String.format("Normalising %s to BCNF", t.getPath()));
                        Collection<Table> normalisedTables = normaliser.normaliseToBCNF(t, nextTableId, limitToFK ? foreignKey : null, this, scorer);
                        Collection<Table> prov = new LinkedList<>();
                        prov.add(t);
                        for(Table n : normalisedTables) {
                            n.setPath(String.format("%d.json", n.getTableId()));
                            provenance.put(n, prov);
                        }
                        normalised.addAll(normalisedTables);
                        nextTableId+=normalised.size();
                    }
        
                }
            }
        } else {

            WebTableNormaliser normaliser = new WebTableNormaliser();
    		for(Table t : tables.values()) {
			
                TableColumn foreignKey = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"FK".equals(c.getHeader())));
                            
                System.out.println(String.format("[normalise] table #%d %s: {%s}", t.getTableId(), t.getPath(), StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
                if(verbose) {
                    System.out.println(t.formatFunctionalDependencies());
                }
                
                if(normalisationHeuristic==NormalisationHeuristic._2NF) {
                    System.out.println(String.format("Normalising %s to 2NF", t.getPath()));
                    Collection<Table> normalisedTables = normaliser.normaliseTo2NF(t, nextTableId, limitToFK ? foreignKey : null, addFullClosure);
                    for(Table n : normalisedTables) {
                        n.setPath(String.format("%d.json", n.getTableId()));
                    }
                    normalised.addAll(normalisedTables);
                    nextTableId+=normalised.size();
                } else if(normalisationHeuristic==NormalisationHeuristic._3NF) {
                    System.out.println(String.format("Normalising %s to 3NF", t.getPath()));
                    Collection<Table> normalisedTables = normaliser.normaliseTo3NF(t, nextTableId, limitToFK ? foreignKey : null, addFullClosure);
                    for(Table n : normalisedTables) {
                        n.setPath(String.format("%d.json", n.getTableId()));
                    }
                    normalised.addAll(normalisedTables);
                    nextTableId+=normalised.size();
                } else {
                    normalised.add(t);
                }

            }
        }
        
        StitchedModel normalisedModel = new StitchedModel();
        normalisedModel.setTables(Q.map(normalised, (t)->t.getTableId()));
        normalisedModel.setRecords(records);
        normalisedModel.setSchemaCorrespondences(schemaCorrespondences);
        normalisedModel.setAttributes(attributes);
        normalisedModel.updateModel(normalisedModel.getTables().values(), getTables().values(), false);
        normalisedModel.setStitchedProvenance(provenance);
        return normalisedModel;
    }

    public StitchedModel stitchTables(
		boolean log, 
		StitchingMethod stitchingMethod, 
		boolean keepUnchangedTables, 
		boolean overlappingAttributesOnly, 
		boolean clusterByAttributes, 
		boolean mergeToSet,
        boolean propagateDependencies,
        File logDirectory) {

        Map<Integer, Table> tablesToStitch = tables;

		int nextTableId = Q.<Integer>max(Q.project(tablesToStitch.values(), (t)->t.getTableId()))+1;
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> filteredCorrespondences = getSchemaCorrespondences();

		// filter correspondences to control which tables are stitched
		// choose filter based on parameters
		switch (stitchingMethod) {
		case OneTableFullyMapped:
			OneTableFullyMappedFilter filter = new OneTableFullyMappedFilter();
			// filter.setLog(log);
			filter.setLogDirectory(logDirectory);
			System.out.println(String.format("[OneTableFullyMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = filter.run(filteredCorrespondences, tablesToStitch.values());	
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			break;
		case BothTablesFullyMapped:
			BothTablesFullyMappedFilter filterBoth = new BothTablesFullyMappedFilter();
			filterBoth.setEnforceDistinctProvenance(false);
			// filterBoth.setLog(log);
			filterBoth.setLogDirectory(logDirectory);
			System.out.println(String.format("[BothTablesFullyMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = filterBoth.run(filteredCorrespondences, tablesToStitch.values());	
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			break;
		case OneKeyFullyMapped:
			OneKeyFullyMappedFilter filterOneKey = new OneKeyFullyMappedFilter();
			filterOneKey.setEnforceDistinctProvenance(false);
			filterOneKey.setPropagateKeys(true);
			// filterOneKey.setPropagateDependencies(propagateDependencies);
			filterOneKey.setLogDirectory(logDirectory);
			System.out.println(String.format("[OneKeyFullyMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = filterOneKey.run(filteredCorrespondences, getAttributes(), tablesToStitch.values());	
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			// candidateKeys = filterOneKey.getClusteredCandidateKeys();
			setCandidateKeys(filterOneKey.getClusteredCandidateKeys());
			System.out.println(String.format("Created %d clustered candidate keys", getCandidateKeys().size()));
			for(MatchableTableDeterminant key : getCandidateKeys().get()) {
				System.out.println(String.format("[OneKeyFyllyMapped] clustered candidate key: #%d: {%s}",
					key.getTableId(),
					MatchableTableColumn.formatCollection(key.getColumns())
				));
			}
			break;
		case MaxColumnsMapped:
			MaxColumnsMappedFilter maxColumnsMapped = new MaxColumnsMappedFilter();
			maxColumnsMapped.setEnforceDistinctProvenance(false);
			maxColumnsMapped.setLogDirectory(logDirectory);
			System.out.println(String.format("[MaxColumnsMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = maxColumnsMapped.run(filteredCorrespondences, tablesToStitch.values());	
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			break;
		case NonContextColumnMapped:
			NonContextColumnMappedFilter filterNonContextMapped = new NonContextColumnMappedFilter();
			filterNonContextMapped.setEnforceDistinctProvenance(false);
			filterNonContextMapped.setLogDirectory(logDirectory);
			System.out.println(String.format("[NonContextColumnMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = filterNonContextMapped.run(filteredCorrespondences, tablesToStitch.values());	
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			break;
		case BothKeysFullyMapped:
			// cluster all matching candidate keys
			// then stitch all tables which have candidate keys in the same clusters
			BothKeysFullyMappedFilter filterKeys = new BothKeysFullyMappedFilter();
			filterKeys.setEnforceDistinctProvenance(false);
			System.out.println(String.format("[BothKeysFullyMapped] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = filterKeys.run(filteredCorrespondences, getAttributes(), tablesToStitch.values());	
			setCandidateKeys(filterKeys.getClusteredCandidateKeys());
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			System.out.println(String.format("Created %d clustered candidate keys", getCandidateKeys().size()));
			break;
		case DeterminantClusters:
			DeterminantSelector ds = new DeterminantSelector(true, logDirectory);
			System.out.println(String.format("[DeterminantClusters] Filtering %d schema correspondences", filteredCorrespondences.size()));
			filteredCorrespondences = ds.selectDeterminant(getRecords(), getAttributes(), getCandidateKeys(), getSchemaCorrespondences(), tablesToStitch);
			System.out.println(String.format("%d schema correspondences after filtering", filteredCorrespondences.size()));
			System.out.println(String.format("Created %d propagated candidate keys", getCandidateKeys().size()));
			break;
		default:
			break;
		}
		
		if(stitchingMethod==StitchingMethod.None) {
			return this;
		} else {
			if(log) {
				for(Correspondence<MatchableTableColumn, Matchable> cor : filteredCorrespondences.get()) {
					System.out.println(String.format("\t{%s}->{%s}", cor.getFirstRecord(), cor.getSecondRecord()));
				}
			}

			TableReconstructor tr = new TableReconstructor(tablesToStitch);
			tr.setKeepUnchangedTables(keepUnchangedTables);
			tr.setOverlappingAttributesOnly(overlappingAttributesOnly);
			tr.setClusterByOverlappingAttributes(clusterByAttributes);
			tr.setMergeToSet(mergeToSet);
			tr.setCreateNonOverlappingClusters(true);
			tr.setVerifyDependencies(propagateDependencies);	
//			tr.setLog(log);
//			tr.setLogDirectory(logDirectory);
			WebTableDataSetLoader loader = new WebTableDataSetLoader();
			DataSet<MatchableTableDeterminant, MatchableTableColumn> functionalDependencies = loader.loadFunctionalDependencyDataSet(tablesToStitch.values(), getRecords());
			Collection<Table> result = tr.reconstruct(nextTableId, getRecords(), getAttributes(), getCandidateKeys(), functionalDependencies, filteredCorrespondences);
            
            StitchedModel model = copy();
            model.setTables(Q.map(result, (t)->t.getTableId()));
            model.setStitchedProvenance(tr.getStitchedProvenance());

			if(result.size()>0) {
				// update schema correspondences, record, and attribute datasets
				// important: update the original schema correspondences, not the filtered ones!
				// the filtered correspondences were used for stitching, so they only connect one column to itself after stitching
				System.out.println(String.format("[stitchTables] Projecting %d schema correspondences", getSchemaCorrespondences().size()));
                
				model.setRecords(loader.loadRowDataSet(result));
				Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = CorrespondenceProjector.projectCorrespondences(getSchemaCorrespondences(), model.getRecords().getSchema(), result, getAttributes(), false).distinct();
				// remove correspondences between a single column (two corresponding columns were stitched together)
				schemaCorrespondences = schemaCorrespondences.where((c)->c.getFirstRecord().getIdentifier()!=c.getSecondRecord().getIdentifier());
				model.setSchemaCorrespondences(schemaCorrespondences);
				model.setAttributes(model.getRecords().getSchema());
				System.out.println(String.format("[stitchTables] %d schema correspondences after projection", schemaCorrespondences.size()));
				
				try {
					Correspondence.toGraph(schemaCorrespondences.get()).writePajekFormat(new File(logDirectory, "stitched_correspondences.net"));
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				System.out.println(String.format("%d attributes / %d records in total after table stitching", model.getAttributes().size(), model.getRecords().size()));
			} else {
				System.out.println("Stitching did not result in any new tables!");
			}

            model.updateModel(result, tablesToStitch.values(), false);

			return model;
		}
    }
    
    public void stitchDependencies(Collection<Table> from, boolean verbose) {

        StitchedModel toModel = this;

		double minRelativeSizeForFDUpdates = 0.05;

		Collection<Table> to = toModel.getTables().values();
		// Map<TableColumn, Set<TableColumn>> nullFreePartitions = toModel.getNullFreePartitions();

		for(Table propagateToTable : to) {
			FDTreeWrapper fds = new FDTreeWrapper(propagateToTable.getColumns());
			fds.addMostGeneralDependencies();
			
			Set<String> provenance = propagateToTable.getProvenance();

			// get all attributes which are in all tables
			Set<TableColumn> intersection = null;
			Map<TableColumn, Set<Table>> attributesInTables = new HashMap<>();
			for(Table propagateFromTable : from) {
				if(provenance.contains(propagateFromTable.getPath()) || propagateFromTable.getPath().equals(propagateToTable.getPath())) {
					Set<TableColumn> attributesInBothTables = new HashSet<>();
					for(TableColumn c : propagateFromTable.getColumns()) {
						TableColumn originalColumn = Q.firstOrDefault(Q.where(propagateToTable.getColumns(), (c0)->c0.getProvenance().contains(c.getIdentifier())));
						if(originalColumn!=null) {
							attributesInBothTables.add(originalColumn);
							Set<Table> tablesForColumn = MapUtils.get(attributesInTables, originalColumn, new HashSet<>());
							tablesForColumn.add(propagateFromTable);
						}
					}
					if(intersection==null) {
						intersection=attributesInBothTables;
					} else {
						intersection = Q.intersection(intersection, attributesInBothTables);
					}
				}
			}
			Set<TableColumn> attributesInAllTables = intersection;

			if(verbose) System.out.println(String.format("[forwardPropagateDependencies] updating table #%d, attributes in all tables are {%s}",
				propagateToTable.getTableId(),
				StringUtils.join(Q.project(attributesInAllTables, (c)->c.getHeader()), ",")
			));
			for(TableColumn c : propagateToTable.getColumns()) {
				Set<Table> tbls = attributesInTables.get(c);
				if(tbls!=null) {
					if(verbose) System.out.println(String.format("[forwardPropagateDependencies]\t%s in %d tables: {%s}",
						c.toString(),
						tbls.size(),
						StringUtils.join(Q.project(tbls, (t)->t.getPath()), ",")
					));
				} else {
					if(verbose) System.out.println(String.format("[forwardPropagateDependencies]\t%s in no other tables",
						c.toString()
					));
				}
			}

			if(attributesInAllTables.size()==0) {
				// the table was not stitched, no need to propagate FDs from a table to itself
				continue;
			}

			List<Pair<Table, Pair<Set<TableColumn>, Set<TableColumn>>>> allFDsToPropagate = new LinkedList<>();
			for(Table propagateFromTable : from) {
			
				// check if FROM table was stitched into TO table	
				if(provenance.contains(propagateFromTable.getPath()) || propagateToTable.getPath().equals(propagateFromTable.getPath())) {
					Collection<Pair<Set<TableColumn>, Set<TableColumn>>> fdsToPropagate = Pair.fromMap(propagateFromTable.getSchema().getFunctionalDependencies());
		
					for(Pair<Set<TableColumn>, Set<TableColumn>> fd : fdsToPropagate) {
						allFDsToPropagate.add(new Pair<>(propagateFromTable, fd));
					}
				}
			}

			// group the FDs by their determinant size
			Map<Integer, Collection<Pair<Table, Pair<Set<TableColumn>, Set<TableColumn>>>>> groupedBySize = Q.group(allFDsToPropagate, (p)->p.getSecond().getFirst().size());

			// keep a list of all invalidated FDs for the propagation step
			Collection< Pair<Set<TableColumn>, Set<TableColumn>>> invalidatedFDs = new LinkedList<>();

			// process the groups in order
			for(int currentGroup = 0; currentGroup <= Q.<Integer>max(groupedBySize.keySet()); currentGroup++) {

				// add the existing minimal FDs
				if(currentGroup>0) {
					if(verbose) System.out.println(String.format("[stitchDependencies] adding minimal FDs with determinant size %d to #%d",
						currentGroup,
						propagateToTable.getTableId()
					));
					Collection<Pair<Table, Pair<Set<TableColumn>, Set<TableColumn>>>> fdsInGroup = groupedBySize.get(currentGroup);
					if(fdsInGroup==null) {
						fdsInGroup = new LinkedList<>();
					}
					for(Pair<Table, Pair<Set<TableColumn>, Set<TableColumn>>> p : fdsInGroup) {
						Pair<Set<TableColumn>, Set<TableColumn>> fd = p.getSecond();
						Table propagateFromTable = p.getFirst();
						// map the FD's columns to the columns in the TO table
						Set<TableColumn> determinantInTO = new HashSet<>();
						Set<TableColumn> dependantInTO = new HashSet<>();
						for(TableColumn c : fd.getFirst()) {
							TableColumn toColumn = Q.firstOrDefault(Q.where(propagateToTable.getColumns(), (c0)->c0.getProvenance().contains(c.getIdentifier())));
							if(toColumn!=null) {
								determinantInTO.add(toColumn);
							}
						}	
						for(TableColumn c : fd.getSecond()) {
							TableColumn toColumn = Q.firstOrDefault(Q.where(propagateToTable.getColumns(), (c0)->c0.getProvenance().contains(c.getIdentifier())));
							if(toColumn!=null) {
								dependantInTO.add(toColumn);
							}
						}

						for(TableColumn dep : dependantInTO) {
							int fromTableSize = p.getFirst().getSize();
							Set<TableColumn> addedDep = Q.toSet(dep);
							// if det contains a rare/unmapped attribute, we will always add the FD
							// - check tables with same dep, ignore det!
							Collection<Table> otherTablesWithColumnCombination = toModel.getProvenanceForColumnCombination(new HashSet<>(addedDep));
							Collection<Table> otherTablesWithDeterminant = toModel.getProvenanceForColumnCombination(new HashSet<>(determinantInTO));
							int maxOtherTableSize = fromTableSize;
							for(Table otherTable : otherTablesWithColumnCombination) {
								maxOtherTableSize = Math.max(otherTable.getSize(), maxOtherTableSize);
							}
							int maxOtherTableSizeDeterminant = fromTableSize;
							for(Table otherTable : otherTablesWithDeterminant) {
								maxOtherTableSizeDeterminant = Math.max(otherTable.getSize(), maxOtherTableSizeDeterminant);
							}

							double relativeSize = fromTableSize / (double) maxOtherTableSize;
							double relativeDeterminantSize = fromTableSize / (double) maxOtherTableSizeDeterminant;

							// if we are about to add X,Y->Z we must check that X,Y does not invalidate any existing FD!
							// - simply check if any FD contains the LHS we are going to add
							boolean lhsValid = true;
							boolean invalidatesOtherFD = false;
							for(Pair<Set<TableColumn>,Set<TableColumn>> invalidated : Pair.fromMap(fds.getFunctionalDependencies())) {
								// if an existing determinant is a subset of the new determinant
								// new: X,Y->Z
								// invalidates:	X->Y, X->Z, Y->Z, Y->Z
								if(determinantInTO.containsAll(invalidated.getFirst()) && determinantInTO.size()>invalidated.getFirst().size()) {
									Collection<TableColumn> rest = Q.without(determinantInTO, invalidated.getFirst());
									// and any of the remaining attributes of the new determinant is in its dependant, then the existing FD will be invalidated
									if(Q.intersection(invalidated.getSecond(), rest).size()>0) {
										invalidatesOtherFD = true;
										if(verbose) System.out.println(String.format("[StitchedDependencies]\tchecking determinant provenance: {%s}->{%s} invalidates {%s}->{%s}",
											StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
											StringUtils.join(Q.project(fd.getSecond(), (c)->c.getHeader()), ","),
											StringUtils.join(Q.project(invalidated.getFirst(), (c)->c.getHeader()), ","),
											StringUtils.join(Q.project(invalidated.getSecond(), (c)->c.getHeader()), ",")
										));
										break;
									}
								}
							}
							// only check if the determinant can invalidate any other FD (size > 1)
							// if(determinantInTO.size()>1 && !fds.getFunctionalDependencies().containsKey(determinantInTO)) {
							if(determinantInTO.size()>1 && invalidatesOtherFD) {
								// if not, check if the source table is large enough to introduce this LHS
								lhsValid = relativeDeterminantSize >=minRelativeSizeForFDUpdates;
							}

							if(relativeSize>=minRelativeSizeForFDUpdates && lhsValid) {
								if(verbose) System.out.println(String.format("[stitchDependencies]\tadding minimal FD #%d: {%s}->{%s} corresponding to #%d: {%s}->{%s} / relative size %f (%d/%d) / determinant relative size %f (%d/%d)",
									propagateFromTable.getTableId(),
									StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
									dep.getHeader(),
									propagateToTable.getTableId(),
									StringUtils.join(Q.project(determinantInTO, (c)->c.getHeader()), ","),
									dep.getHeader(),
									relativeSize,
									fromTableSize,
									maxOtherTableSize,
									relativeDeterminantSize,
									fromTableSize,
									maxOtherTableSizeDeterminant
								));
								fds.addMinimalFunctionalDependency(new Pair<>(determinantInTO, Q.toSet(dep)));
							} else {
								if(verbose) System.out.println(String.format("[stitchDependencies]\tnot stitching FD #%d: {%s}->{%s}: relative size %f (%d/%d) below threshold %f / determinant relative size %f (%d/%d) / invalidates: %b",
									propagateFromTable.getTableId(),
									StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
									dep.getHeader(),
									relativeSize,
									fromTableSize,
									maxOtherTableSize,
									minRelativeSizeForFDUpdates,
									relativeDeterminantSize,
									fromTableSize,
									maxOtherTableSizeDeterminant,
									invalidatesOtherFD
								));
							}
						}

					}
				} else {
					if(verbose) System.out.println(String.format("[stitchDependencies] initialising minimal FDs with determinant size %d for #%d: {}->{%s}",
						currentGroup,
						propagateToTable.getTableId(),
						StringUtils.join(Q.project(fds.getFunctionalDependencies().get(new HashSet<>()), (c)->c.getHeader()), ",")
					));
				}

				// remove all non FDs
				if(verbose) System.out.println(String.format("[stitchDependencies] removing non FDs with determinant size %d from #%d",
					currentGroup,
					propagateToTable.getTableId()
				));

				// we process level-wise, so we only have to remove non FDs which are one element smaller than the next group size

				// go through all existing FDs of current group size
				// - if they don't hold in all of the FROM tables, remove them
				for(Pair<Set<TableColumn>, Set<TableColumn>> fd : Pair.fromMap(fds.getFunctionalDependencies())) {
					if(fd.getFirst().size()==currentGroup) {

						Set<TableColumn> updatedClosure = StitchedFunctionalDependencyUtils.closure(fd.getFirst(), fds.getFunctionalDependencies(), toModel);

						for(Table propagateFromTable : from) {
				
							// check if FROM table was stitched into TO table	
							if(provenance.contains(propagateFromTable.getPath()) || propagateToTable.getPath().equals(propagateFromTable.getPath())) {

								Set<TableColumn> determinantInFROM = new HashSet<>();
								for(TableColumn c : fd.getFirst()) {
									TableColumn fromColumn = Q.firstOrDefault(Q.where(propagateFromTable.getColumns(), (c0)->c.getProvenance().contains(c0.getIdentifier()) || c.getIdentifier().equals(c0.getIdentifier())));
									if(fromColumn!=null) {
										determinantInFROM.add(fromColumn);
									}
								}	

								// only proceed if the determinant exists in the FROM table
								if(determinantInFROM.size()==fd.getFirst().size()) {

									// we calculate the closure in the FROM table, so we don't have to consider stitched FDs!
									Set<TableColumn> closure = FunctionalDependencyUtils.closure(determinantInFROM, propagateFromTable.getSchema().getFunctionalDependencies());
								
									Set<TableColumn> nonDeterminedFROM = new HashSet<>(Q.without(propagateFromTable.getColumns(), closure));
									if(nonDeterminedFROM.size()>0) {
										Set<TableColumn> nonDeterminedTO = new HashSet<>();
										for(TableColumn c : nonDeterminedFROM) {
											TableColumn toColumn = Q.firstOrDefault(Q.where(propagateToTable.getColumns(), (c0)->c0.getProvenance().contains(c.getIdentifier())));
											if(toColumn!=null) {
												nonDeterminedTO.add(toColumn);
											}
										}

										for(TableColumn nonDet : nonDeterminedTO) {
											Set<TableColumn> removedDep = Q.toSet(nonDet);
											int fromTableSize = propagateFromTable.getSize();
											Collection<Table> otherTablesWithColumnCombination = toModel.getProvenanceForColumnCombination(removedDep);
											int maxOtherTableSize = fromTableSize;
											for(Table otherTable : otherTablesWithColumnCombination) {
												maxOtherTableSize = Math.max(otherTable.getSize(), maxOtherTableSize);
											}
					
											double relativeSize = fromTableSize / (double) maxOtherTableSize;
											if(relativeSize>=minRelativeSizeForFDUpdates) {
												if(verbose) System.out.println(String.format("[stitchDependencies]\tremoving non FD #%d: {%s}->{%s} invalidated by minimal #%d: {%s}->{%s} / updated {%s}->{%s} / relative size %f (%d/%d)",
													propagateToTable.getTableId(),
													StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
													StringUtils.join(Q.project(removedDep, (c)->c.getHeader()), ","),
													propagateFromTable.getTableId(),
													StringUtils.join(Q.project(determinantInFROM, (c)->c.getHeader()), ","),
													StringUtils.join(Q.project(closure, (c)->c.getHeader()), ","),
													StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
													StringUtils.join(Q.project(updatedClosure, (c)->c.getHeader()), ","),
													relativeSize,
													fromTableSize,
													maxOtherTableSize
												));

												Pair<Set<TableColumn>,Set<TableColumn>> invalidated = new Pair<>(fd.getFirst(), removedDep);
												fds.removeNonFunctionalDependency(invalidated);

												invalidatedFDs.add(invalidated);
											} else {
												if(verbose) System.out.println(String.format("[stitchDependencies]\tnot stitching non-FD #%d: {%s}-!>{%s}: relative size %f (%d/%d) below threshold %f",
													propagateFromTable.getTableId(),
													StringUtils.join(Q.project(fd.getFirst(), (c)->c.getHeader()), ","),
													nonDet.getHeader(),
													relativeSize,
													fromTableSize,
													maxOtherTableSize,
													minRelativeSizeForFDUpdates
												));
											}
										}
									}
								}
							}
						}

					}
				} // end invalidation

				// printFunctionalDependencies(fds.getFunctionalDependencies());
			} // end current group

			fds.removeGeneralisations();

			propagateToTable.getSchema().setFunctionalDependencies(fds.getFunctionalDependencies());
		}
	}
}