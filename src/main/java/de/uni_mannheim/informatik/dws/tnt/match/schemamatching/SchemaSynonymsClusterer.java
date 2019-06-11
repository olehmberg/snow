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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;

/**
 * Generates clusters of synonymous attributes via the provided schema correspondences.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaSynonymsClusterer {


	public Processable<Set<String>> runClustering(
			Processable<MatchableTableColumn> dataset,
			boolean isSymmetric,
			Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		

		ConnectedComponentClusterer<String> labelComponentClusterer = new ConnectedComponentClusterer<>();
		
		Set<String> allLabels = new HashSet<>();
		
		for(MatchableTableColumn col : dataset.get()) {
			if(!SpecialColumns.isSpecialColumn(col)) {
				allLabels.add(col.getHeader());
			}
		}
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : correspondences.get()) {
			
			String h1 = cor.getFirstRecord().getHeader();
			String h2 = cor.getSecondRecord().getHeader();
			
			if(!h1.equals(h2)) {
				labelComponentClusterer.addEdge(new Triple<String, String, Double>(h1, h2, 1.0));
			}
		}
		
		for(String label : allLabels) {
			labelComponentClusterer.addEdge(new Triple<String, String, Double>(label, label, 1.0));
		}
		
		Processable<Set<String>> result = new ProcessableCollection<>();
		for(Collection<String> cluster : labelComponentClusterer.createResult().keySet()) {
			result.add(new HashSet<>(cluster));
		}
		
		return result;
	}
	
}
