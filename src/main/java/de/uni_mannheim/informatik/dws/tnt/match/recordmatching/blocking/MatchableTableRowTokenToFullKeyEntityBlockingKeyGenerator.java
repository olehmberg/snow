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
package de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableEntity;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * 
 * Generates tokens from the blocked columns, which are then used as blocking keys for MatchableEntity
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator extends BlockingKeyGenerator<MatchableTableRow, MatchableTableColumn, MatchableEntity> {

	private static final long serialVersionUID = 1L;

	private Collection<MatchableTableDeterminant> candidateKeys;
	private Map<Integer, Collection<MatchableTableDeterminant>> excludedCandidateKeys;
	private boolean matchPartialKeys = false;
	
	Map<Integer, Set<MatchableEntity>> createdEntitiesPerClass;
	Map<String, Set<MatchableEntity>> createdEntitiesPerColumn;

	private boolean doNotDeduplicate = false;
	/**
	 * @param doNotDeduplicate the doNotDeduplicate to set
	 */
	public void setDoNotDeduplicate(boolean doNotDeduplicate) {
		this.doNotDeduplicate = doNotDeduplicate;
	}

	/**
	 * @param matchPartialKeys the matchPartialKeys to set
	 */
	public void setMatchPartialKeys(boolean matchPartialKeys) {
		this.matchPartialKeys = matchPartialKeys;
	}

	/**
	 * @return the createdEntitiesPerClass
	 */
	public Map<Integer, Set<MatchableEntity>> getCreatedEntitiesPerClass() {
		return createdEntitiesPerClass;
	}
	
	/**
	 * @return the createdEntitiesPerColumn
	 */
	public Map<String, Set<MatchableEntity>> getCreatedEntitiesPerColumn() {
		return createdEntitiesPerColumn;
	}
	
	public MatchableTableRowTokenToFullKeyEntityBlockingKeyGenerator(Collection<MatchableTableDeterminant> candidateKeys, Map<Integer, Collection<MatchableTableDeterminant>> excludedCandidateKeys) {
		this.candidateKeys = candidateKeys;
		this.excludedCandidateKeys = excludedCandidateKeys;
		this.createdEntitiesPerClass = new HashMap<>();
		this.createdEntitiesPerColumn = new HashMap<>();
	}
	
	protected Map<Integer, MatchableEntity> createEntitiesFromRecord(MatchableTableRow record, Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		// first group the schema correspondences by class
		Processable<Group<Integer, Correspondence<MatchableTableColumn, Matchable>>> groupedByClass = correspondences.group((c,col)->col.next(new Pair<>(c.getSecondRecord().getTableId(), c)));

		Collection<MatchableTableDeterminant> excludedKeysForRecord = MapUtils.get(excludedCandidateKeys, record.getTableId(), new HashSet<>());

		// check for each class if a non-excluded key is completely mapped, if yes, create a new entity
		Processable<Pair<Integer, MatchableEntity>> entities = groupedByClass
			.map((g, col)->{
				
				Map<MatchableTableColumn, MatchableTableColumn> kbToWeb = new HashMap<>();
				for(Correspondence<MatchableTableColumn, Matchable> cor : g.getRecords().get()) {
					kbToWeb.put(cor.getSecondRecord(), cor.getFirstRecord());
				}

				Set<MatchableTableColumn> primeAttributes = new HashSet<>();
				for(MatchableTableDeterminant key : candidateKeys) {
					if(
						key.getTableId()==g.getKey() 
						&& !excludedKeysForRecord.contains(key) 
						&& (kbToWeb.keySet().containsAll(key.getColumns()) || matchPartialKeys)
					) {
						primeAttributes.addAll(key.getColumns());
					}
				}

				List<MatchableTableColumn> columns = new LinkedList<>();
				for(MatchableTableColumn kbC : primeAttributes) {
					MatchableTableColumn webC = kbToWeb.get(kbC);
					if(webC!=null) {
						columns.add(webC);
					}
				}
				MatchableEntity e = new MatchableEntity(record, columns);
				if(doNotDeduplicate) {
					e = new MatchableEntity(record, Arrays.asList(record.getSchema()));
				}
				col.next(new Pair<>(g.getKey(), e));
			});

		return Pair.toMap(entities.get());
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator#generateBlockingKeys(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable, de.uni_mannheim.informatik.dws.winter.processing.DataIterator)
	 */
	@Override
	public void generateBlockingKeys(MatchableTableRow record,
			Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableEntity>> resultCollector) {
		
		if(correspondences!=null) {
			// create one entity for every completely matched candidate key which was not excluded by the inclusion dependency filter
			Map<Integer, MatchableEntity> entities = createEntitiesFromRecord(record, correspondences);

			// block using label correspondences instead of a list of columns
			for(Correspondence<MatchableTableColumn, Matchable> cor : correspondences.get()) {

				if(cor.getSecondRecord().getHeader().equals(KnowledgeBase.RDFS_LABEL)) {

					MatchableEntity entity = entities.get(cor.getSecondRecord().getTableId());

					//TODO: this is not thread-safe!
					// keep track of all created entities
					MapUtils.get(createdEntitiesPerClass, cor.getSecondRecord().getTableId(), new HashSet<>()).add(entity);
					MapUtils.get(createdEntitiesPerColumn, 
						String.format("%d-%s", cor.getSecondRecord().getTableId(), cor.getFirstRecord().getIdentifier()), 
						new HashSet<>()).add(entity);
					
					MatchableTableColumn col = cor.getFirstRecord();
					Object value = record.get(col.getColumnIndex());
					
					if(value!=null && entity!=null) {
						Object[] listValues = null;
						if(value.getClass().isArray()) {
							listValues = (Object[])value;
						} else {
							listValues = new Object[] { value };
						}
						
						for(Object o : listValues) {
							if(o!=null) {
								String s = o.toString();
								
								s = s.toLowerCase();
								s = s.replaceAll("[.!?]+", " ");
								s = s.replaceAll("[^a-z0-9\\s]", "");
								
								for(String token : WebTablesStringNormalizer.tokenise(s, true)) {
									// prefix the blocking key value with the class id for which the entity was created
									resultCollector.next(new Pair<>(String.format("class_%d-%s", cor.getSecondRecord().getTableId(), token), entity));
								}
							}
						}
					}
				}
			}
		}
	}

}
