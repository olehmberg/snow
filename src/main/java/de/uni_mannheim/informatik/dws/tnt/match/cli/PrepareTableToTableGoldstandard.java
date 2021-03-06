package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.File;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandardCreator;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.CandidateKeyBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.DeterminantBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.EntityLabelBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.LabelBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.TableToTableMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.ValueBasedMatcher;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;

public class PrepareTableToTableGoldstandard  extends Executable {

	public static enum MatcherType {
		Trivial,
		NonTrivialPartial,
		NonTrivialFull,
		CandidateKey,
		Label,
		Entity
	}
	
	@Parameter(names = "-matcher")
	private MatcherType matcher = MatcherType.Label;
	
	@Parameter(names = "-web", required=true)
	private String webLocation;
	
	@Parameter(names = "-eval", required=true) 
	private String evaluationLocation;
	
	@Parameter(names = "-serialise")
	private boolean serialise;
	
	public static void main(String[] args) throws Exception {
		PrepareTableToTableGoldstandard app = new PrepareTableToTableGoldstandard();
		
		if(app.parseCommandLine(PrepareTableToTableGoldstandard.class, args)) {
			app.run();
		}
	}
	
	public void run() throws Exception {
		System.err.println("Loading Web Tables");
		WebTables web = WebTables.loadWebTables(new File(webLocation), true, true, false, serialise);
		web.removeHorizontallyStackedTables();
		
		System.err.println("Matching Union Tables");
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = runTableMatching(web);
		
		N2NGoldStandardCreator gsc = new N2NGoldStandardCreator();
		File evalFile = new File(evaluationLocation);
		evalFile.mkdirs();
    	gsc.writeInterUnionMapping(new File(evalFile, "inter_union_mapping_generated.tsv"), schemaCorrespondences, web);
		
		System.err.println("Done.");
	}
	
	private Processable<Correspondence<MatchableTableColumn, Matchable>> runTableMatching(WebTables web) throws Exception {
		TableToTableMatcher matcher = null;
		
    	switch(this.matcher) {
		case CandidateKey:
			matcher = new CandidateKeyBasedMatcher();
			break;
		case Label:
			matcher = new LabelBasedMatcher();
			break;
		case NonTrivialFull:
			matcher = new DeterminantBasedMatcher();
			break;
		case Trivial:
			matcher = new ValueBasedMatcher();
			break;
		case Entity:
			matcher = new EntityLabelBasedMatcher();
			break;
		default:
			break;
    		
    	}
    	
    	DisjointHeaders dh = DisjointHeaders.fromTables(web.getTables().values());
    	Map<String, Set<String>> disjointHeaders = dh.getAllDisjointHeaders();
    	
    	matcher.setWebTables(web);
    	matcher.setMatchingEngine(new MatchingEngine<>());
    	matcher.setDisjointHeaders(disjointHeaders);
		
    	matcher.initialise();
    	matcher.match();
    	
    	Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = matcher.getSchemaCorrespondences();
    	
    	return schemaCorrespondences;
	}
}

	
