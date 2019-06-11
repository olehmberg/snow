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
package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence.RecordId;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class PrintGoldStandardStatistics extends Executable {
	
	@Parameter(names = "-log")
	private String logLocation;
	
	@Parameter(names = "-correspondences")
	private String correspondenceLocation;

	@Parameter(names = "-web")
	private String webLocation;
	
	public static void main(String[] args) throws Exception {
		PrintGoldStandardStatistics app = new PrintGoldStandardStatistics();
		
		if(app.parseCommandLine(PrintGoldStandardStatistics.class, args)) {
			app.run();
		}
	}
	
	public void run() throws IOException {

		BufferedWriter logWriter = null;
		
		if(logLocation!=null) {
			logWriter = new BufferedWriter(new FileWriter(new File(logLocation)));
		}
		
		for(String gsLocation : this.getParams()) {
			
			MatchingGoldStandard gs = new MatchingGoldStandard();
			File gsFile = new File(gsLocation);
			
			System.out.println("*******************************************************************************************");
			System.out.println("*******************************************************************************************");
			System.out.println(String.format("Loading %s", gsFile.getName()));
			gs.loadFromTSVFile(gsFile);

			int manuallyAnnotated = 0;

			Pair<Distribution<String>, Distribution<String>> distributions = null;
			if(correspondenceLocation!=null) {
				// load the correspondences
				File corFile = new File(new File(correspondenceLocation), new File(gsLocation).getName().replaceAll("\\.tsv", ".csv"));
				Processable<Correspondence<RecordId, RecordId>> correspondences = Correspondence.loadFromCsv(corFile);

				int labelledPairs = gs.getPositiveExamples().size()+gs.getNegativeExamples().size();
				System.out.println(String.format("\t%d/%d correspondences annotated", labelledPairs, correspondences.size()));

				if(correspondences.size()==labelledPairs) {
					System.out.println("\t*** The goldstandard is complete ***");
				}

				Pair<Distribution<String>, Distribution<String>> distributionsNotExact = getSimilarityDistributions(gs, true, correspondences);

				if(distributionsNotExact!=null) {
					System.out.println("\t*** Distributions for non-exact matches ***");
					System.out.println(String.format("\tTotal number of non-exactly matching positive examples: %d", distributionsNotExact.getFirst().getPopulationSize()));

					// manually annotated correspondences are either in the positive examples with a score below 1.0 
					// or they are in the negative examples for records which have no positive example
					manuallyAnnotated = distributionsNotExact.getFirst().getPopulationSize() + getRecordsWithoutPositiveExamples(gs).size();
					System.out.println(String.format("\tTotal number of manually annotated examples: %d", manuallyAnnotated));

					if(webLocation!=null) {
						Set<String> noCandidates = getRecordsWithoutCandidates(gs, webLocation, gsLocation);
						System.out.println(String.format("\tTotal number of records without candidates: %d", noCandidates.size()));
					}

					System.out.println(String.format("\tSimilarity distribution for positive examples: %s", distributionsNotExact.getFirst().formatCompact()));
				}

				distributions = getSimilarityDistributions(gs, false, correspondences);
				
				if(distributions!=null) {
					System.out.println("\t*** Distributions for all matches ***");
					System.out.println(String.format("\tSimilarity distribution for positive examples: %s", distributions.getFirst().formatCompact()));
					System.out.println(String.format("\tSimilarity distribution for negative examples: %s", distributions.getSecond().formatCompact()));
				}
			}
			
			if(logLocation!=null) {
				if(distributions!=null) {
					logWriter.write(String.format("%s\n", 
							StringUtils.join(new String[]{
									gsFile.getName(),
									Integer.toString(gs.getPositiveExamples().size()),
									Integer.toString(gs.getNegativeExamples().size()),
									Integer.toString(manuallyAnnotated)
									, Double.toString(distributions.getFirst().getRelativeFrequency("0.0"))
									, Double.toString(distributions.getFirst().getRelativeFrequency("0.1"))
									, Double.toString(distributions.getFirst().getRelativeFrequency("0.2"))
									, Double.toString(distributions.getFirst().getRelativeFrequency("0.3"))
									, Double.toString(distributions.getFirst().getRelativeFrequency("0.4"))
									, Double.toString(distributions.getFirst().getRelativeFrequency("0.5"))
									, Double.toString(distributions.getFirst().getRelativeFrequency("0.6"))
									, Double.toString(distributions.getFirst().getRelativeFrequency("0.7"))
									, Double.toString(distributions.getFirst().getRelativeFrequency("0.8"))
									, Double.toString(distributions.getFirst().getRelativeFrequency("0.9"))
									, Double.toString(distributions.getFirst().getRelativeFrequency("1.0"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("0.0"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("0.1"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("0.2"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("0.3"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("0.4"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("0.5"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("0.6"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("0.7"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("0.8"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("0.9"))
									, Double.toString(distributions.getSecond().getRelativeFrequency("1.0"))
							}, "\t")
							));
				} else {
					logWriter.write(String.format("%s\n", 
						StringUtils.join(new String[]{
								gsFile.getName(),
								Integer.toString(gs.getPositiveExamples().size()),
								Integer.toString(gs.getNegativeExamples().size()),
								Integer.toString(manuallyAnnotated)
//								distributions!=null ? distributions.getFirst().formatCompact() : "",
//								distributions!=null ? distributions.getSecond().formatCompact() : ""
						}, "\t")
						));
				}
			}
			
		}
		
		if(logLocation!=null) {
			logWriter.close();
		}
		
	}

	protected Set<String> getRecordsWithoutCandidates(MatchingGoldStandard gs, String webLocation, String gsFilename) throws FileNotFoundException {
		Set<String> result = new HashSet<>();

		File webFile = new File(webLocation, new File(gsFilename).getName().replace(".tsv", ""));
		JsonTableParser parser = new JsonTableParser();
		parser.setInferSchema(false);
		Table t = parser.parseTable(webFile);

		for(TableRow r : t.getRows()) {
			result.add(r.getIdentifier());
		}

		for(Pair<String,String> p : gs.getPositiveExamples()) {
			result.remove(p.getFirst());
		}
		for(Pair<String, String> p : gs.getNegativeExamples()) {
			result.remove(p.getFirst());
		}

		return result;
	}

	protected Set<String> getRecordsWithoutPositiveExamples(MatchingGoldStandard gs)  {
		Set<String> positiveExamples = new HashSet<>();

		for(Pair<String,String> p : gs.getPositiveExamples()) {
			positiveExamples.add(p.getFirst());
		}	

		Set<String> noPositiveExamples = new HashSet<>();
		for(Pair<String, String> p : gs.getNegativeExamples()) {
			noPositiveExamples.add(p.getFirst());
		}

		noPositiveExamples = new HashSet<>(Q.without(noPositiveExamples, positiveExamples));

		return noPositiveExamples;
	}

	protected Pair<Distribution<String>,Distribution<String>> getSimilarityDistributions(MatchingGoldStandard gs, boolean noExactMatches, Processable<Correspondence<RecordId, RecordId>> correspondences) throws IOException {
		
			if(noExactMatches) {
				correspondences = correspondences.where((c)->c.getSimilarityScore()<1.0);
			}

			// generate the distributions
			Processable<String> positiveSimilarities = correspondences
				.join(new ProcessableCollection<>(gs.getPositiveExamples()), 
						(c)->new Pair<>(c.getFirstRecord().getIdentifier(), c.getSecondRecord().getIdentifier()),
						(p)->p)
				.map((Pair<Correspondence<RecordId, RecordId>, Pair<String, String>> record,
							DataIterator<String> resultCollector) 
					-> {
						String value = Double.toString(Math.floor(record.getFirst().getSimilarityScore()*10)/10.0);
						resultCollector.next(value);
					});
			Processable<String> negativeSimilarities = correspondences
					.join(new ProcessableCollection<>(gs.getNegativeExamples()), 
							(c)->new Pair<>(c.getFirstRecord().getIdentifier(), c.getSecondRecord().getIdentifier()),
							(p)->p)
					.map((Pair<Correspondence<RecordId, RecordId>, Pair<String, String>> record,
								DataIterator<String> resultCollector) 
						-> {
							String value = Double.toString(Math.floor(record.getFirst().getSimilarityScore()*10)/10.0);
							resultCollector.next(value);
						});	
			
			Distribution<String> positiveDistribution = Distribution.fromCollection(positiveSimilarities.get());
			Distribution<String> negativeDistribution = Distribution.fromCollection(negativeSimilarities.get());
			
			return new Pair<>(positiveDistribution, negativeDistribution);
	}
}
