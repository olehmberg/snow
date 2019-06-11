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

import java.util.Collection;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DistinctTableProvenanceFilter {
	
//	private File logDirectory;
	private boolean log = false;
	private boolean returnRemovedCorrespondences = false;
	
	public void setReturnRemovedCorrespondences(boolean returnRemovedCorrespondences) {
		this.returnRemovedCorrespondences = returnRemovedCorrespondences;
	}
	
//	public void setLogDirectory(File logDirectory) {
//		this.logDirectory = logDirectory;
//	}
	public void setLog(boolean log) {
		this.log = log;
	}
	
	public Processable<Correspondence<MatchableTableColumn, Matchable>> run(
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			Collection<Table> tables) {
		// keep correspondences if columns were not created by projection from the same table
		// - as there is only one step at which tables are projected, the decomposition, two tables were created from the same table as soon as they share any provenance
		
		// create column provenance dataset: column id -> set<provenance>
		ParallelProcessableCollection<Pair<String, Collection<String>>> provenance = new ParallelProcessableCollection<>();
		for(Table t : tables) {
			for(TableColumn c : t.getColumns()) {
				provenance.add(new Pair<>(c.getIdentifier(), c.getProvenance()));
			}
		}
		
		// join correspondences with provenance (2 joins)
		return schemaCorrespondences
			.join(provenance, (c)->c.getFirstRecord().getIdentifier(), (p)->p.getFirst())
			.join(provenance, (p)->p.getFirst().getSecondRecord().getIdentifier(), (p)->p.getFirst())
			// keep correspondence only if provenance sets are distinct			
			.map(
				(Pair<Pair<Correspondence<MatchableTableColumn, Matchable>, Pair<String, Collection<String>>>, Pair<String, Collection<String>>> record, DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) 
				->{
					
					Collection<String> leftProvenance = record.getFirst().getSecond().getSecond();
					Collection<String> rightProvenance = record.getSecond().getSecond();
					Correspondence<MatchableTableColumn, Matchable> cor = record.getFirst().getFirst();
					Set<String> intersection = Q.intersection(leftProvenance, rightProvenance);
					
					if(intersection.size()==0) {
						if(log) {
							System.out.println(String.format("\tkeeping correspondence %s->%s\n\t\t%s\n\t\t%s", 
								cor.getFirstRecord(),
								cor.getSecondRecord(),
								StringUtils.join(leftProvenance, ","),
								StringUtils.join(rightProvenance, ",")
								));
						}
					} else {
						if(log) {
							System.out.println(String.format("\tremoving correspondence %s->%s\tshared provenance: %s", 
								cor.getFirstRecord(),
								cor.getSecondRecord(),
								StringUtils.join(intersection, ",")
								));
						}
					}
					
					if(intersection.size()==0 && !returnRemovedCorrespondences) {
						resultCollector.next(cor);
					} else if(intersection.size()>0 && returnRemovedCorrespondences) {
						resultCollector.next(cor);
					}
					
				});
	}

}