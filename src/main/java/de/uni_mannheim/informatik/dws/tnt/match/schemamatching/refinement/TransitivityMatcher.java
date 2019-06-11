package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

public class TransitivityMatcher<RecordType extends Matchable> {

	public Processable<Correspondence<RecordType, Matchable>> run(Processable<Correspondence<RecordType, Matchable>> correspondences) {
		ConnectedComponentClusterer<RecordType> clusterer = new ConnectedComponentClusterer<>();
		for(Correspondence<RecordType, Matchable> cor : correspondences.get()) {
			clusterer.addEdge(new Triple<>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		
		Map<Collection<RecordType>, RecordType> clusters = clusterer.createResult();
		
		Collection<Correspondence<RecordType, Matchable>> results = new LinkedList<>();
		
		for(Collection<RecordType> cluster : clusters.keySet()) {
			
			List<RecordType> list = new ArrayList<>(cluster);
			
			for(int i = 0; i < list.size(); i++) {
				for(int j = 0; j < list.size(); j++) {
					if(i!=j) {
						results.add(new Correspondence<RecordType, Matchable>(list.get(i), list.get(j), 1.0));
					}
				}
			}
			
		}
		
		return correspondences.createProcessableFromCollection(results);
	}
	
}
