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
package de.uni_mannheim.informatik.dws.tnt.match.entitystitching.matchers;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.instancebased.MatchableTableColumnFKBasedValueGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.ValueBasedCorrespondenceRescorer;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.VectorCreationMethod;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.SetAggregator;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceCosineSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableToTableRefinementMatcher {

    private double similarityThreshold = 0.0;

    public TableToTableRefinementMatcher(double similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
    }

    public Processable<Correspondence<MatchableTableColumn, Matchable>> run(Processable<Correspondence<MatchableTableColumn, Matchable>> existingCorrespondences, DataSet<MatchableTableRow, MatchableTableColumn> records) {

        if(existingCorrespondences.size()==0) {
            return existingCorrespondences;
        }

        // create a set of all columns in correspondences
        // - we only have to create values for these columns and can skip all others
        
        Processable<Pair<String, Set<MatchableTableColumn>>> aggregated = existingCorrespondences.aggregate((c,col)->{col.next(new Pair<>("",c.getFirstRecord())); col.next(new Pair<>("",c.getSecondRecord()));},new SetAggregator<>());

        Set<MatchableTableColumn> colsInCors = Q.firstOrDefault(aggregated.get()).getSecond();

        ValueBasedCorrespondenceRescorer<MatchableTableRow, MatchableTableColumn, MatchableTableColumn, MatchableValue> rescorer = 
            new ValueBasedCorrespondenceRescorer<>(
                new MatchableTableColumnFKBasedValueGenerator(colsInCors),
                new MatchableTableColumnFKBasedValueGenerator(colsInCors),
                new VectorSpaceCosineSimilarity(),
                VectorCreationMethod.TFIDF,
                similarityThreshold
            );

        return rescorer.run(records, records, existingCorrespondences);
    }

}