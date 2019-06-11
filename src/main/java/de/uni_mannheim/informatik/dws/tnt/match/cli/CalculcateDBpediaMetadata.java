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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.LodCsvTableParser;

/**
 * 
 * Calculates potential candidate keys and property cardinalities for DBpedia classes
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CalculcateDBpediaMetadata extends Executable {

	@Parameter(names = "-kb")
	private String kbLocation;
	
	public static void main(String[] args) throws IOException {
		
		CalculcateDBpediaMetadata app = new CalculcateDBpediaMetadata();
		
		if(app.parseCommandLine(CalculcateDBpediaMetadata.class, args)) {
			
			app.run();
			
		}
		
	}
	
	private Pattern disambiguationPattern = Pattern.compile("^[^(]+\\(([^)]+)\\)");
	private Pattern yearPattern = Pattern.compile(".*\\d{4}.*");
	private Pattern bracketsPattern = Pattern.compile("\\([^)]*\\)");
	
	public void run() throws FileNotFoundException {
		//TODO re-run with new pattern
//		KnowledgeBase kb = KnowledgeBase.loadKnowledgeBase(new File(kbLocation), null, null, false);
		LodCsvTableParser parser = new LodCsvTableParser();
//		parser.setConvertValues(false);
		
		File dir = new File(kbLocation);
		
		// get the names of all classes
		Set<String> classes = new HashSet<>();
		List<File> files = new LinkedList<>();
		if(dir.isDirectory()) {
			for(File f : dir.listFiles()) {
				String cls = f.getName().replace(".csv", "").toLowerCase();
				classes.add(cls);
				System.out.println(cls);
				files.add(f);
			}	
		} else {
			String cls = dir.getName().replace(".csv", "").toLowerCase();
			classes.add(cls);
			files.add(dir);
		}
		
		// add sub-classes of athlete
		classes.add("ArcherPlayer");
		classes.add("AthleticsPlayer ");
		classes.add("AustralianRulesFootballPlayer");
		classes.add("BadmintonPlayer");
		classes.add("BaseballPlayer");
		classes.add("BasketballPlayer");
		classes.add("Bodybuilder");
		classes.add("Boxer");
		classes.add("AmateurBoxer");
		classes.add("BullFighter");
		classes.add("Canoeist");
		classes.add("ChessPlayer");
		classes.add("Cricketer");
		classes.add("Cyclist");
		classes.add("DartsPlayer");
		classes.add("Fencer");
		classes.add("GaelicGamesPlayer");
		classes.add("GolfPlayer");
		classes.add("GridironFootballPlayer");
		classes.add("AmericanFootballPlayer");
		classes.add("CanadianFootballPlayer");
		classes.add("Gymnast");
		classes.add("HandballPlayer");
		classes.add("HighDiver");
		classes.add("HorseRider");
		classes.add("Jockey");
		classes.add("LacrossePlayer");
		classes.add("MartialArtist");
		classes.add("MotorsportRacer");
		classes.add("MotorcycleRider");
		classes.add("MotocycleRacer");
		classes.add("SpeedwayRider");
		classes.add("RacingDriver");
		classes.add("DTMRacer");
		classes.add("FormulaOneRacer");
		classes.add("NascarDriver");
		classes.add("RallyDriver");
		classes.add("NationalCollegiateAthleticAssociationAthlete");
		classes.add("NetballPlayer");
		classes.add("PokerPlayer");
		classes.add("Rower");
		classes.add("RugbyPlayer");
		classes.add("SnookerPlayer");
		classes.add("SnookerChamp");
		classes.add("SoccerPlayer");
		classes.add("SquashPlayer");
		classes.add("Surfer");
		classes.add("Swimmer");
		classes.add("TableTennisPlayer");
		classes.add("TeamMember");
		classes.add("TennisPlayer");
		classes.add("VolleyballPlayer");
		classes.add("BeachVolleyballPlayer");
		classes.add("WaterPoloPlayer");
		classes.add("WinterSportPlayer");
		classes.add("Biathlete");
		classes.add("BobsleighAthlete");
		classes.add("CrossCountrySkier");
		classes.add("Curler");
		classes.add("FigureSkater");
		classes.add("IceHockeyPlayer");
		classes.add("NordicCombined");
		classes.add("Skater");
		classes.add("Ski_jumper");
		classes.add("Skier");
		classes.add("SpeedSkater");
		classes.add("Wrestler");
		classes.add("SumoWrestler");

		
		Set<String> testUris = new HashSet<>();
		testUris.add("http://dbpedia.org/resource/Come_Fly_with_Me_(1957_song)");
		testUris.add("http://dbpedia.org/resource/For_All_We_Know_(1970_song)");
		
		Map<String, Pair<Integer, Integer>> entityCounts = new HashMap<>();
		Map<String, Map<String, Integer>> allCounts = new HashMap<>();
		
		// process all files
		for(File f : files) {
			
			System.out.println(String.format("Parsing %s", f.getName()));
			
			// load the file
			Table t = parser.parseTable(f);
			
			// find the rdfs:label property
			TableColumn label = null;
			for(TableColumn c : t.getColumns()) {
				if("rdf-schema#label".equals(c.getHeader())) {
					label = c;
					break;
				}
			}
			
			if(label==null) {
				continue;
			} else {
				System.out.println(String.format("\trdfs:label is column %d: '%s'", label.getColumnIndex(), label.getHeader()));
			}
			
			Map<String, Integer> propertyDisambiguationSourceCounts = new HashMap<>();
			int numDisambiguated = 0;
			
			// go through all entities
			for(TableRow r : t.getRows()) {
				
				String uri = (String)r.get(0);
				
				if(testUris.contains(uri)) {
					System.out.println("debug");
				}
				
				// get the label value
				String labelValue = (String)r.get(label.getColumnIndex());
				
				if(labelValue!=null) {
				
					// get the label's disambiguation part
					Matcher matcher = disambiguationPattern.matcher(labelValue);
					
					if(matcher.matches()) {
						
						numDisambiguated++;
						
						String disambiguation = matcher.group(1);

//						System.out.println(String.format("\tLabel '%s' has disambiguation '%s'", labelValue, disambiguation));
						
						// go through all other properties and check if their value is contained in the disambiguation part
						for(TableColumn c : t.getColumns()) {
							
							if(c!=label) {
								
								// get the property value
								Object propValue = r.get(c.getColumnIndex());
								
								if(propValue!=null) {
									
									// remove any possible disambiguation from the value
									String stringValue = propValue.toString();
									
//									Matcher valueMatcher = noDisambiguationPattern.matcher(stringValue);
									
//									if(valueMatcher.matches()) {
										
//										System.out.println(String.format("Non-disambiguated value for '%s' is '%s'", stringValue, valueMatcher.group(1)));
										
										//stringValue = valueMatcher.group(1).trim();
										stringValue = bracketsPattern.matcher(stringValue).replaceAll("").trim();
										
										// check if the value occurs in the label's disambiguation part
										if(disambiguation.contains(stringValue)) {
											
//											System.out.println(String.format("\tdisambiguation '%s' contains '%s'", disambiguation, stringValue));
											
											// if yes, increase the count for this property
											MapUtils.increment(propertyDisambiguationSourceCounts, c.getHeader());
										} else if(c.getDataType()==DataType.date) {
											
											// if there's no match and the property is of type date, try only the year part
											LocalDateTime dt = (LocalDateTime)propValue;
											
											stringValue = Integer.toString(dt.getYear());
											
											if(disambiguation.contains(stringValue)) {
												
//												System.out.println(String.format("\tdisambiguation '%s' contains '%s'", disambiguation, stringValue));
												
												// if yes, increase the count for this property
												MapUtils.increment(propertyDisambiguationSourceCounts, c.getHeader());
											} else {
												
												// sometimes the disambiguation uses the decade instead of the actual year, so try that, too
												
												stringValue = Integer.toString(dt.getYear()/10);
												
												if(disambiguation.contains(stringValue)) {
													// if yes, increase the count for this property
													MapUtils.increment(propertyDisambiguationSourceCounts, c.getHeader());													
												}
												
											}
											
										}
										
//									}
									
									
								}
								
							}
							
						}
						
						// check if a class name is used in disambiguation
						for(String cls : classes) {
							if(disambiguation.contains(cls.toLowerCase())) {
								MapUtils.increment(propertyDisambiguationSourceCounts, cls);
							}
						}
						
						// check if a sequence of four digits is used in disambiguation (date properties can be very unreliable)
						if(yearPattern.matcher(disambiguation).matches()) {
							MapUtils.increment(propertyDisambiguationSourceCounts, "4-digits");
						}
					}
				
				}
				
			}
			
			if(propertyDisambiguationSourceCounts.size()>0) {
				
				System.out.println(String.format("\t%d entities total, %d disambiguated (%.2f%%)", t.getRows().size(), numDisambiguated, 100*numDisambiguated/(double)t.getRows().size()));
				
				System.out.println("\tDisambiguation source statistics");
				
				// show statistics about disambiguation sources
				for(Map.Entry<String, Integer> e : Q.sort(propertyDisambiguationSourceCounts.entrySet(), (e1,e2)->-Integer.compare(e1.getValue(), e2.getValue()))) {
					
					System.out.println(String.format("\t%d\t%.2f%%\t%s", e.getValue(), 100*e.getValue()/(double)numDisambiguated, e.getKey().toString()));
					
				}
				
				allCounts.put(t.getPath(), propertyDisambiguationSourceCounts);
				entityCounts.put(t.getPath(), new Pair<Integer, Integer>(t.getRows().size(), numDisambiguated));
			} else {
				System.out.println("\tno disambiguated entities.");
			}
		}
		
		System.out.println("Relevant (>=5%) properties:");
		// print output again as tsv data
		// cls	ttl entities	disambig entities	keyprop1	keyprop1 entity fraction	keyprop2 ...
		for(String cls : allCounts.keySet()) {
			
			Pair<Integer, Integer> entities = entityCounts.get(cls);
			Map<String, Integer> propertyDisambiguationSourceCounts = allCounts.get(cls);
			
			StringBuilder sb = new StringBuilder();
			
			sb.append(cls);
			sb.append("\t");
			sb.append(Integer.toString(entities.getFirst()));
			sb.append("\t");
			sb.append(Integer.toString(entities.getSecond()));
			
			if(propertyDisambiguationSourceCounts.size()>0) {
				
				for(Map.Entry<String, Integer> e : Q.sort(propertyDisambiguationSourceCounts.entrySet(), (e1,e2)->-Integer.compare(e1.getValue(), e2.getValue()))) {
					
					double percentage = e.getValue() / (double) entities.getFirst();
					
					if(percentage<0.05) {
						// consider properties as relevant as long as they disambiguate at least 5% of all entities
						break;
					} else {
						sb.append("\t");
						sb.append(e.getKey());
						sb.append("\t");
						sb.append(Double.toString(percentage));
					}
					
				}
				
				System.out.println(sb.toString());
			}
		}
		
	}
	
}
