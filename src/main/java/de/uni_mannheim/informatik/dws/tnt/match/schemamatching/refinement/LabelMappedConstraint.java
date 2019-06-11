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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement;

import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.*;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class LabelMappedConstraint {

    private boolean log;

    public LabelMappedConstraint(boolean log) {
        this.log = log;
    }

    public Processable<Correspondence<MatchableTableColumn, Matchable>> applyLabelMappedConstraint(Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBcorrespondences) {
		// constraint that all columns of at least one key must be mapped if a class mapping is assigned
		// group correspondences by table/class combination, check for each class if a full key was mapped. if not, remove correspondences 
		
		tableToKBcorrespondences = tableToKBcorrespondences
			// group correspondences by table/class combination
			.group((Correspondence<MatchableTableColumn, Matchable> record,
					DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>>> resultCollector) 
				-> {
					resultCollector.next(new Pair<>(new Pair<>(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				})
			// map correspondences to output if rdfs:label is mapped
			.map((Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, Matchable>> record, DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) 
				-> {
					boolean isLabelMapped = false;
					for(Correspondence<MatchableTableColumn, Matchable> cor : record.getRecords().get()) {
						if(KnowledgeBase.RDFS_LABEL.equals(cor.getSecondRecord().getHeader())) {
							isLabelMapped = true;
							break;
						}
					}
					
					if(isLabelMapped) {
						for(Correspondence<MatchableTableColumn, Matchable> cor : record.getRecords().get()) {
							resultCollector.next(cor);
						}
					} else {
						if(log) {
							System.out.println(String.format("Filtering mapping from table #%d to class #%d",
								record.getKey().getFirst(),
								record.getKey().getSecond()
							));
						}
					}
				});
		System.out.println(String.format("Label mapped constraint resulted in %d correspondences", tableToKBcorrespondences.size()));
		
		if(log) {
			for(Correspondence<MatchableTableColumn, Matchable> cor : tableToKBcorrespondences.get()) {
				System.out.println(String.format("\t%f\t%s <-> %s", cor.getSimilarityScore(), cor.getFirstRecord(), cor.getSecondRecord()));
			}
		}
		
		return tableToKBcorrespondences;
	}

}