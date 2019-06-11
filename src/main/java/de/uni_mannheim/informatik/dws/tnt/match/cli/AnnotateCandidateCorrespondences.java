package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.DBpediaClient;
import de.uni_mannheim.informatik.dws.tnt.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils2;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

public class AnnotateCandidateCorrespondences extends Executable {

	@Parameter(names="-web", required=true)
	private String webLocation;

	@Parameter(names = "-relations")
	private String relationsLocation;

	@Parameter(names="-kb", required=true)
	private String kbLocation;
	
	@Parameter(names="-cors", required=true)
	private String corLocation;;
	
	@Parameter(names = "-result")
	private String resultLocation;
	
	@Parameter(names = "-w")
	private int tableWidth = 30;
	
	@Parameter(names = "-maxSim")
	private double maxSim = 1.0;
	public static void main(String[] args) throws IOException {
		AnnotateCandidateCorrespondences app = new AnnotateCandidateCorrespondences();
		
		if(app.parseCommandLine(AnnotateCandidateCorrespondences.class, args)) {
			app.run();			
		}
	}

	private MatchingGoldStandard annotations;
	private WebTables web = null;
	// private Map<String, Collection<Table>> tablesByFK;
	// private Map<String, Collection<TableRow>> rowsByFK;
	private Map<String, Map<Table, List<TableRow>>> rowsByTableByFK;
	
	protected void loadRelations() {
		try {
			System.out.println("Loading relations ...");
			web = WebTables.loadWebTables(new File(relationsLocation), true, false, false, true);

			// index tables by FK values
			rowsByTableByFK = new HashMap<>();

			for(Table t : web.getTables().values()) {
				TableColumn fk = Q.firstOrDefault(Q.where(t.getColumns(), (c)->"fk".equals(c.getHeader())));

				if(fk!=null) {
					for(TableRow r : t.getRows()) {
						String fkValue = (String)r.get(fk.getColumnIndex());
						if(fkValue!=null) {
							// MapUtils.get(tablesByFK, fkValue, new HashSet<>()).add(t);
							// MapUtils.get(rowsByFK, fkValue, new HashSet<>()).add(r);
							get(rowsByTableByFK, fkValue, t, new LinkedList<>()).add(r);
						}
					}
				} else {
					System.out.println(String.format("Could not identify foreign key column in relation {%s}",
						StringUtils.join(Q.project(t.getColumns(), (c)->c.getHeader()), ",")
					));
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static <K,K2,V> V get(Map<K, Map<K2,V>> map, K index1, K2 index2, V defaultValue) {
		
		Map<K2,V> innerMap = map.get(index1);
		
		if(innerMap==null) {
			innerMap = new HashMap<>();
			map.put(index1, innerMap);
		}
			
		return MapUtils.get(innerMap, index2, defaultValue);

	}

	public void run() throws IOException {
		WebTables web = WebTables.loadWebTables(new File(webLocation), false, false, false, false);
		KnowledgeBase kb = KnowledgeBase.loadKnowledgeBase(new File(kbLocation), null, null, false, true);
		
		Processable<Correspondence<MatchableTableRow, Matchable>> correspondences = Correspondence.loadFromCsv(new File(corLocation), web.getRecords(), kb.getRecords());
		
		annotations = new MatchingGoldStandard();
		
		if(new File(resultLocation).exists()) {
			annotations.loadFromTSVFile(new File(resultLocation));
		}
		
		final Set<String> idsInGs = new HashSet<>();
		Set<String> refIds = new HashSet<>();
		for(Pair<String, String> pos : annotations.getPositiveExamples()) {
			idsInGs.add(pos.getFirst());
			refIds.add(pos.getSecond());
		}
		for(Pair<String, String> neg : annotations.getNegativeExamples()) {
			idsInGs.add(neg.getFirst());
			refIds.add(neg.getSecond());
		}
		
		// group correspondences by left id
		Processable<Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>>> groups = correspondences
			.group(
				(Correspondence<MatchableTableRow, Matchable> record, DataIterator<Pair<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>>> resultCollector) 
				->{
					resultCollector.next(new Pair<>(record.getFirstRecord(), record));
				})
			.sort(
				(Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>> g) -> {
					Collection<Double> scores = Q.project(g.getRecords().get(), (c)->c.getSimilarityScore());
					return Q.max(scores);
				}
				, false)
			.where((Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>> g) -> {
				Collection<Double> scores = Q.project(g.getRecords().get(), (c)->c.getSimilarityScore());
				return Q.max(scores)<=maxSim;
			});
		
		Scanner s = new Scanner(System.in);
		
		// quick-check mode for correspondences with score 1.0
		Processable<Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>>> fullScoreMatches = groups
			.where((g)-> {
				Collection<Double> scores = Q.project(g.getRecords().get(), (c)->c.getSimilarityScore());
				return (Q.max(scores).intValue())==1 && !idsInGs.contains(g.getKey().getIdentifier());
//				((int)Q.max(Q.project(g.getRecords().get(), (c)->c.getSimilarityScore())))==1 && !idsInGs.contains(g.getKey().getIdentifier())
			});
		
		Collection<Pair<Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>>, Correspondence<MatchableTableRow, Matchable>>> matches = annotateQuick(fullScoreMatches,s);
		for(Pair<Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>>, Correspondence<MatchableTableRow, Matchable>> p : matches) {
			Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>> g = p.getFirst();
			Correspondence<MatchableTableRow, Matchable> match = p.getSecond();
			annotations.addPositiveExample(new Pair<>(g.getKey().getIdentifier(), match.getSecondRecord().getIdentifier()));
			for(Correspondence<MatchableTableRow, Matchable> cor : g.getRecords().get()) {
				if(cor!=match) {
					annotations.addNegativeExample(new Pair<>(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier()));
				}
			}
		}
		
		idsInGs.clear();
		refIds.clear();
		for(Pair<String, String> pos : annotations.getPositiveExamples()) {
			idsInGs.add(pos.getFirst());
			refIds.add(pos.getSecond());
		}
		for(Pair<String, String> neg : annotations.getNegativeExamples()) {
			idsInGs.add(neg.getFirst());
			refIds.add(neg.getSecond());
		}
		
		int ttlAnnotated = 0;
		for(Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>> group : groups.get()) {
			
			if(!idsInGs.contains(group.getKey().getIdentifier())) {
				System.out.println(String.format("Annotated %d records: %d positive / %d negative examples (%d candidates)", ttlAnnotated, annotations.getPositiveExamples().size(), annotations.getNegativeExamples().size(), groups.size()));
				
				Pair<Correspondence<MatchableTableRow, Matchable>, Boolean> result = annotate(group, s);
				
				if(result==null) {
					s.close();
					return;
				}
				
				if(result.getSecond()) {
					annotations.addPositiveExample(new Pair<>(result.getFirst().getFirstRecord().getIdentifier(), result.getFirst().getSecondRecord().getIdentifier()));
					for(Correspondence<MatchableTableRow, Matchable> cor : group.getRecords().get()) {
						if(cor!=result.getFirst()) {
							annotations.addNegativeExample(new Pair<>(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier()));
						}
					}
				} else {
					for(Correspondence<MatchableTableRow, Matchable> cor : group.getRecords().get()) {
						annotations.addNegativeExample(new Pair<>(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier()));
					}
//					annotations.addNegativeExample(new Pair<>(result.getFirst().getFirstRecord().getIdentifier(), result.getFirst().getSecondRecord().getIdentifier()));
				}
			} else {
				for(Correspondence<MatchableTableRow, Matchable> cor : group.getRecords().get()) {
					if(!refIds.contains(cor.getSecondRecord().getIdentifier())) {
						annotations.addNegativeExample(new Pair<>(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier()));
					}
				}
			}
			ttlAnnotated++;
		}
		
		saveResults();
		s.close();

	}
	
	private int pageSize = 30;
	
	private Collection<Pair<Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>>, Correspondence<MatchableTableRow, Matchable>>> annotateQuick(Processable<Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>>> groups, Scanner s) throws IOException {
		List<Pair<Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>>, Correspondence<MatchableTableRow, Matchable>>> list = new ArrayList<>();
		for(Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>> g : groups.get()) {
			Correspondence<MatchableTableRow, Matchable> maxCor = g.getRecords().sort((c)->c.getSimilarityScore(),false).firstOrNull();
			list.add(new Pair<>(g,maxCor));
		}
		
		int page = 0;
		int pages = (int)Math.ceil((list.size() / (double)pageSize));
		
		Set<Integer> incorrect = new HashSet<>();
		
		if(list.size()>0) {
			do {
				System.out.println(String.format("Quick-check (page %d/%d):", page+1, pages));
				for(int i = page * pageSize; i < list.size() && i < ((page+1)*pageSize); i++) {
					if(!incorrect.contains(i)) {
						System.out.println(String.format("[%d] %s\t||\t%s", i, list.get(i).getFirst().getKey().format(tableWidth), list.get(i).getSecond().getSecondRecord().format(tableWidth)));
					}
				}
				System.out.println("Enter '+' to move to the next page / end quick check mode");
				System.out.println("Enter '++' to accept all matches with similarity score 1.0");
				System.out.println("Enter '-' to end quick check mode WITHOUT annotations");
				System.out.print("Enter numbers (separated by space) of INCORRECT matches (will be checked again later), end input with '+': ");
				
				String input = null;
				
				do {
					input = s.next();
				
					if("++".equals(input)) {
						page = pages;
						break;
					} else if("+".equals(input)) {
						page++;
					}  else if("-".equals(input)) {
						for(int i = 0; i < list.size(); i++) {
							incorrect.add(i);
						}
						page = pages;
						break;
					} else {
					
						try {
							int i = Integer.parseInt(input);
			
							incorrect.add(i);
						} catch(Exception e) {
							System.out.println(String.format("Incorrect input: '%s'", input));
						}
					}
				} while(!"+".equals(input));
			} while(page<pages);
		}
		
		LinkedList<Pair<Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>>, Correspondence<MatchableTableRow, Matchable>>> correct = new LinkedList<>();
		for(int i = 0; i < list.size(); i++) {
			if(!incorrect.contains(i)) {
				correct.add(list.get(i));
			}
		}
		
		return correct;
	}
	
	private Pair<Correspondence<MatchableTableRow, Matchable>, Boolean> annotate(Group<MatchableTableRow, Correspondence<MatchableTableRow, Matchable>> group, Scanner s) throws IOException {
		List<Correspondence<MatchableTableRow, Matchable>> candidates = new ArrayList<>(group.getRecords()
				.sort((c)->{
					MatchableTableRow r = c.getSecondRecord();
					MatchableTableColumn label = Q.firstOrDefault(Q.where(Q.toList(r.getSchema()), (col)->"rdf-schema#label".equals(col.getHeader())));
					return r.get(label.getColumnIndex()).toString();
				})
				.sort((c)->c.getSimilarityScore(),false)
				.get());
		int page = 0;
		int pages = (int)Math.ceil((candidates.size() / (double)pageSize));
		
		Pair<Correspondence<MatchableTableRow, Matchable>, Boolean> result = null;
		
		while(result==null) {
			System.out.println();
			// show left record & all right records as candidates
			// System.out.println(group.getKey().format(tableWidth));
			for(MatchableTableColumn c : group.getKey().getSchema()) {
				System.out.println(String.format("%s: %s",
					c.getHeader(),
					group.getKey().get(c.getColumnIndex())==null ? "null" : group.getKey().get(c.getColumnIndex()).toString()
				));
			}
			
			System.out.println(String.format("Candidates (page %d/%d):", page+1, pages));
			for(int i = page * pageSize; i < candidates.size() && i < ((page+1)*pageSize); i++) {
				System.out.println(String.format("[%d] %.2f\t%s", i, candidates.get(i).getSimilarityScore(), candidates.get(i).getSecondRecord().format(tableWidth)));
			}
			
			System.out.println();
			System.out.println("[context] browse related records");
			System.out.println("[number] select candidate");
			System.out.println("[n] no match");
			System.out.println("[more number] show abstract of a candidate");
			System.out.println("[w X] set column width to X");
			System.out.println("[+] next page");
			System.out.println("[-] previous page");
			System.out.println("[s] save results");
			System.out.println("[q] quit (without saving)");
			
			// let user lookup DBp abstract
			// if user chooses a candidate -> create positive example
			// if user chooses 'not existing' -> create negative example for candidate with highest score
			System.out.print("> ");
			String input = s.next();
			
			switch (input) {
			case "n":
				result = new Pair<Correspondence<MatchableTableRow,Matchable>, Boolean>(candidates.get(0), false);
				break;
			case "-":
				page--;
				if(page<0) {
					page = pages-1;
				}
				break;
			case "+":
				page++;
				if(page==pages) {
					page=0;
				}
				break;
			case "s":
				saveResults();
				break;
			case "q":
				return null;
			case "context":
				showContext(group.getKey(), s);
				break;
			default:
				if(input.startsWith("more")) {
					input = s.next();
					
					try {
						int idx = Integer.parseInt(input);
						
						if(idx<candidates.size()) {
							showMore(candidates.get(idx).getSecondRecord());
						} else {
							System.out.println("Invalid selection");
						}
					} catch(Exception e) {
						e.printStackTrace();
						System.out.println("Invalid input");
					}
					
				} else if(input.startsWith("w")) {
					input = s.next();
						
					try {
						int width = Integer.parseInt(input);
						
						tableWidth = width;
					} catch(Exception e) {
						e.printStackTrace();
						System.out.println("Invalid input");
					}
				} 
				else {
					try {
						int idx = Integer.parseInt(input);
						
						if(idx<candidates.size()) {
							result = new Pair<Correspondence<MatchableTableRow,Matchable>, Boolean>(candidates.get(idx), true);
						} else {
							System.out.println("Invalid selection");
						}
					} catch(Exception e) {
						e.printStackTrace();
						System.out.println("Invalid input");
					}
				}
				break;
			}
			
		}
		
		return result;
	}
	
	private void showContext(MatchableTableRow r, Scanner s) {
		if(web==null) {
			loadRelations();
		}

		if(web!=null) {
			// get the primary key column
			MatchableTableColumn pk = Q.firstOrDefault(Q.where(Q.toList(r.getSchema()), (c)->"pk".equals(c.getHeader())));

			if(pk==null) {
				System.out.println(String.format("Could not identify primary key in relation {%s}",
					StringUtils.join(Q.project(Q.toList(r.getSchema()), (c)->c.getHeader()), ",")
				));
			} else {
				// find relations for r
				String pkValue = (String)r.get(pk.getColumnIndex());

				Map<Table, List<TableRow>> matches = rowsByTableByFK.get(pkValue);

				if(matches==null) {
					System.out.println(String.format("Could not find any related records for primary key value '%s'", pkValue));
				} else {
					// show content
					browseRelatedTables(matches, s);
				}
			}

		}
	}

	private void browseRelatedTables(Map<Table, List<TableRow>> data, Scanner s) {
		boolean exit = false;

		do {

			List<Table> tableList = new ArrayList<>(data.keySet());
			for(int i = 0; i < tableList.size(); i++) {
				System.out.println(String.format("[%d] %s: {%s}",
					i,
					tableList.get(i).getPath(),
					StringUtils.join(Q.project(tableList.get(i).getColumns(), (c)->c.getHeader()), ",")
				));
			}

			System.out.println("[q] back to annotation mode");
			System.out.print("Choose table [n]: ");
			String input = s.next();

			if("q".equals(input)) {
				exit = true;
			} else {
				try {
					int idx = Integer.parseInt(input);
					
					if(idx<tableList.size()) {
						Table t = tableList.get(idx);
						exit = browseRelatedRecords(t, data.get(t), s);
					} else {
						System.out.println("Invalid selection");
					}
				} catch(Exception e) {
					e.printStackTrace();
					System.out.println("Invalid input");
				}
			}

		} while(!exit);
	}

	private boolean browseRelatedRecords(Table t, List<TableRow> records, Scanner s) {
		int page = 0;
		int pageSize = 10;
		int pages = (int)Math.ceil((records.size() / (double)pageSize));
		boolean exit = false;

		do {

			System.out.println(String.format("page %d/%d", page, pages));
			System.out.println(t.getSchema().format(tableWidth));
			for(int i = (page * pageSize); i < records.size() && i < ((page+1)*pageSize); i++) {
				System.out.println(records.get(i).format(tableWidth));
			}

			System.out.println("[+] next page");
			System.out.println("[-] previous page");
			System.out.println("[w X] set column width to X");
			System.out.println("[b] back to table selection");
			System.out.println("[q] back to annotation");
			System.out.print("> ");
			String input = s.next();

			if("b".equals(input)) {
				return false;
			}
			if("q".equals(input)) {
				return true;
			} else if("-".equals(input)) {
				page--;
				if(page<0) {
					page = pages-1;
				}
			} else if("+".equals(input)) {
				page++;
				if(page==pages) {
					page=0;
				}
			} else if(input.startsWith("w")) {
				input = s.next();
					
				try {
					int width = Integer.parseInt(input);
					
					tableWidth = width;
				} catch(Exception e) {
					e.printStackTrace();
					System.out.println("Invalid input");
				}
			} else {
				System.out.println("Invalid input");
			}
		} while(!exit);

		return false;
	}

	private void showMore(MatchableTableRow candidate) throws UnsupportedEncodingException {
		
		String resourceUri = candidate.get(0).toString();
		
		System.out.println(resourceUri);
		
		String abs = DBpediaClient.getAbstract(resourceUri);
		
		System.out.println(abs);
	}
	
	private void saveResults() throws IOException {
		annotations.writeToTSVFile(new File(resultLocation));
		System.out.println("*** Saved annotations ***");
	}
}
