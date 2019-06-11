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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching;

import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.model.*;
import java.util.*;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ColumnTranslator {

	private Map<Set<Integer>, Map<String, MatchableTableColumn>> columnMappingByTables = new HashMap<>();
	private Map<Integer, Set<Integer>> tableMapping;

    public void prepare(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
        // group schema correspondences to speed up lookup for table pairs
        Processable<Group<Set<Integer>, Correspondence<MatchableTableColumn, Matchable>>> schemaCorrespondencesGroupedByTableCombination = schemaCorrespondences
            .group((cor,col)->col.next(new Pair<>(Q.toSet(cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId()), cor)));
        // Map<Set<Integer>, Group<Set<Integer>, Correspondence<MatchableTableColumn, Matchable>>> tableCombinationToCorrespondences = Q.map(schemaCorrespondencesGroupedByTableCombination.get(), (g)->g.getKey());
		columnMappingByTables = new HashMap<>();
		tableMapping = new HashMap<>();
        for(Group<Set<Integer>, Correspondence<MatchableTableColumn, Matchable>> g : schemaCorrespondencesGroupedByTableCombination.get()) {
            Set<Integer> tableIds = g.getKey();
            Map<String, MatchableTableColumn> mapping = new HashMap<>();
            for(Correspondence<MatchableTableColumn, Matchable> cor : g.getRecords().get()) {
                mapping.put(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord());
                mapping.put(cor.getSecondRecord().getIdentifier(), cor.getFirstRecord());
            }
			columnMappingByTables.put(tableIds, mapping);
			
			for(Integer tableId : tableIds) {
				Set<Integer> mappedTables = MapUtils.get(tableMapping, tableId, new HashSet<>());
				mappedTables.addAll(Q.without(tableIds, Q.toSet(tableId)));
			}
        }
    }

	public Collection<MatchableTableColumn> translateMatchableColumns(Collection<MatchableTableColumn> columns, Table from, Table to, Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		Set<MatchableTableColumn> result = new HashSet<>();
		for(MatchableTableColumn c : columns) {
			Correspondence<MatchableTableColumn, Matchable> correspondence = correspondences
				.where(
					(cor)->
						cor.getFirstRecord().getIdentifier().equals(c.getIdentifier()) && cor.getSecondRecord().getTableId()==to.getTableId() 
						|| 
						cor.getSecondRecord().getIdentifier().equals(c.getIdentifier()) && cor.getFirstRecord().getTableId()==to.getTableId())
				.firstOrNull();
			if(correspondence!=null) {
				if(correspondence.getFirstRecord().getIdentifier().equals(c.getIdentifier())) {
					result.add(correspondence.getSecondRecord());
				} else if (correspondence.getSecondRecord().getIdentifier().equals(c.getIdentifier())) {
					result.add(correspondence.getFirstRecord());
				}
			}
		}
		return result;
	}

    /**
     * must call prepare() before using this method!
     */
	public Collection<MatchableTableColumn> translateMatchableColumns(Collection<MatchableTableColumn> columns, Table from, Table to) {
		Set<MatchableTableColumn> result = new HashSet<>();
		Map<String, MatchableTableColumn> mapping = columnMappingByTables.get(Q.toSet(from.getTableId(), to.getTableId()));
		for(MatchableTableColumn c : columns) {

			MatchableTableColumn otherColumn = mapping.get(c.getIdentifier());
			if(otherColumn!=null) {
				result.add(otherColumn);
			}
		}
		return result;
	}

    /**
     * must call prepare() before using this method!
     */
	public Collection<TableColumn> translateColumns(Collection<TableColumn> columns, Table to) {
		Set<TableColumn> result = new HashSet<>();
		for(TableColumn c : columns) {
            Map<String, MatchableTableColumn> mapping = columnMappingByTables.get(Q.toSet(c.getTable().getTableId(), to.getTableId()));
            if(mapping!=null) {
                MatchableTableColumn otherColumn = mapping.get(c.getIdentifier());
                if(otherColumn!=null) {
                    result.addAll(Q.where(to.getColumns(), (col)->col.getIdentifier().equals(otherColumn.getIdentifier())));
                }
            }
		}
		return result;
	}

    /**
     * must call prepare() before using this method!
     */
	public Collection<TableColumn> translateColumns(Collection<TableColumn> columns, Table from, Table to) {
		Set<TableColumn> result = new HashSet<>();
        Map<String, MatchableTableColumn> mapping = columnMappingByTables.get(Q.toSet(from.getTableId(), to.getTableId()));
        if(mapping!=null) {
            for(TableColumn c : columns) {
                MatchableTableColumn otherColumn = mapping.get(c.getIdentifier());
                if(otherColumn!=null) {
                    result.addAll(Q.where(to.getColumns(), (col)->col.getIdentifier().equals(otherColumn.getIdentifier())));
                }
            }
        }
		return result;
	}

	public Collection<TableColumn> translateColumns(Collection<TableColumn> columns, Table from, Table to, Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		Set<TableColumn> result = new HashSet<>();
		for(TableColumn c : columns) {
			Correspondence<MatchableTableColumn, Matchable> correspondence = correspondences
				.where(
					(cor)->
						cor.getFirstRecord().getIdentifier().equals(c.getIdentifier()) && cor.getSecondRecord().getTableId()==to.getTableId() 
						|| 
						cor.getSecondRecord().getIdentifier().equals(c.getIdentifier()) && cor.getFirstRecord().getTableId()==to.getTableId())
				.firstOrNull();
			if(correspondence!=null) {
				if(correspondence.getFirstRecord().getIdentifier().equals(c.getIdentifier())) {
					result.addAll(Q.where(to.getColumns(), (col)->col.getIdentifier().equals(correspondence.getSecondRecord().getIdentifier())));
				} else if (correspondence.getSecondRecord().getIdentifier().equals(c.getIdentifier())) {
					result.addAll(Q.where(to.getColumns(), (col)->col.getIdentifier().equals(correspondence.getFirstRecord().getIdentifier())));
				}
			}
		}
		return result;
	}

	/**
     * must call prepare() before using this method!
     */
	public Collection<TableColumn> getAllMappedColumns(TableColumn c, Map<Integer, Table> otherTables) {
		Set<Integer> mappedTables = tableMapping.get(c.getTable().getTableId());
		Collection<TableColumn> result = new LinkedList<>();
		if(mappedTables!=null) {
			for(Integer otherTable : mappedTables) {
				result.addAll(translateColumns(Q.toSet(c), otherTables.get(otherTable)));
			}
		}
		return result;
	}

}