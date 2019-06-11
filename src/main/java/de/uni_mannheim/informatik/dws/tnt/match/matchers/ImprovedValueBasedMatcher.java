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
package de.uni_mannheim.informatik.dws.tnt.match.matchers;

import de.uni_mannheim.informatik.dws.tnt.match.data.*;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.BlockingKeyIndexer.VectorCreationMethod;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceCosineSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.vectorspace.VectorSpaceMaximumOfContainmentSimilarity;

import java.util.*;
import org.apache.commons.lang.StringUtils;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ImprovedValueBasedMatcher extends TableToTableMatcher {

    private double similarityThreshold = 0.0;

    /**
     * @param similarityThreshold the similarityThreshold to set
     */
    public void setSimilarityThreshold(double similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
    }

    public ImprovedValueBasedMatcher(double similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
    }

	@Override
	protected void runMatching() {
        
        MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();

        Processable<Correspondence<MatchableTableColumn, MatchableValue>> cors = engine.runInstanceBasedSchemaMatching(
            web.getRecords(),
            web.getRecords(),
            new MatchableTableColumnValueGenerator(),
            new MatchableTableColumnValueGenerator(),
            VectorCreationMethod.TFIDF,
            new VectorSpaceCosineSimilarity(),
            similarityThreshold
        );

        // TableToTable matcher expects its base matcher to produce no bidirectional correspondences, so we filter out the duplicates
        cors = cors.map((c)->{
            if(c.getFirstRecord().getDataSourceIdentifier()<c.getSecondRecord().getDataSourceIdentifier()) {
                return c;
            } else {
                return null;
            }
        });

        schemaCorrespondences = Correspondence.toMatchable(cors);
    }
}