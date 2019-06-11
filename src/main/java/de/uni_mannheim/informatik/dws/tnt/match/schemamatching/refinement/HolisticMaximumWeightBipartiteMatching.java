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

import java.util.HashMap;
import java.util.Map;

import org.jgrapht.WeightedGraph;
import org.jgrapht.alg.flow.EdmondsKarpMFImpl;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class HolisticMaximumWeightBipartiteMatching {

	Processable<MatchableTableColumn> tableSchema;
	Processable<Correspondence<MatchableTableColumn, Matchable>> tableToTableCorrespondences; 
	Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBCorrespondences;
	
	public HolisticMaximumWeightBipartiteMatching(Processable<MatchableTableColumn> tableSchema,
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToTableCorrespondences, 
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBCorrespondences) {
		this.tableSchema = tableSchema;
		this.tableToTableCorrespondences = tableToTableCorrespondences;
		this.tableToKBCorrespondences = tableToKBCorrespondences;
	}
	
	public void run() {
		
		Triple<WeightedGraph<MatchableTableColumn, DefaultWeightedEdge>, MatchableTableColumn, MatchableTableColumn> graphWithSourceAndSink = createGraph(tableSchema, tableToTableCorrespondences, tableToKBCorrespondences);
		
		WeightedGraph<MatchableTableColumn, DefaultWeightedEdge> graph = graphWithSourceAndSink.getFirst();
		MatchableTableColumn source = graphWithSourceAndSink.getSecond();
		MatchableTableColumn sink = graphWithSourceAndSink.getThird();
		
		EdmondsKarpMFImpl<MatchableTableColumn, DefaultWeightedEdge> maxFlow = new EdmondsKarpMFImpl<>(graph);
		
		
		
	}
	
	protected Triple<WeightedGraph<MatchableTableColumn, DefaultWeightedEdge>, MatchableTableColumn, MatchableTableColumn> createGraph(Processable<MatchableTableColumn> tableSchema,
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToTableCorrespondences, 
			Processable<Correspondence<MatchableTableColumn, Matchable>> tableToKBCorrespondences) {
		// create the graph and the two partitions
		WeightedGraph<MatchableTableColumn, DefaultWeightedEdge> graph = new SimpleWeightedGraph<>(DefaultWeightedEdge.class);
		Map<DefaultWeightedEdge, Correspondence<MatchableTableColumn, Matchable>> edgeToCorrespondence = new HashMap<>();
		
		// create the source and sink nodes
		MatchableTableColumn source = new MatchableTableColumn("source");
		MatchableTableColumn sink = new MatchableTableColumn("sink");
		
		StringBuilder sb = new StringBuilder();
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : tableToTableCorrespondences.get()) {
			graph.addVertex(cor.getFirstRecord());
			graph.addVertex(cor.getSecondRecord());
			
			// edge between two table columns
			DefaultWeightedEdge edge = graph.addEdge(cor.getFirstRecord(), cor.getSecondRecord());
			graph.setEdgeWeight(edge,(int)( cor.getSimilarityScore()));
			edgeToCorrespondence.put(edge, cor);
			
			sb.append(String.format("\t%.6f\t%s <-> %s\n", cor.getSimilarityScore(), cor.getFirstRecord(), cor.getSecondRecord()));
		}
		
		for(Correspondence<MatchableTableColumn, Matchable> cor : tableToKBCorrespondences.get()) {
			graph.addVertex(cor.getFirstRecord());
			graph.addVertex(cor.getSecondRecord());
			
			// edge between the source and a table column
			DefaultWeightedEdge edge = graph.addEdge(source, cor.getFirstRecord());
			graph.setEdgeWeight(edge,(int)( cor.getSimilarityScore()));
			edgeToCorrespondence.put(edge, cor);
			
			// edge between a table column and kb property
			edge = graph.addEdge(cor.getFirstRecord(), cor.getSecondRecord());
			graph.setEdgeWeight(edge,(int)( cor.getSimilarityScore()));
			edgeToCorrespondence.put(edge, cor);
			
			sb.append(String.format("\t%.6f\t%s <-> %s\n", cor.getSimilarityScore(), cor.getFirstRecord(), cor.getSecondRecord()));
		}
		
		return new Triple<>(graph, source, sink);
	}
	
}
