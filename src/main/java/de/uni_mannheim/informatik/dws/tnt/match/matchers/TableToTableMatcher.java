package de.uni_mannheim.informatik.dws.tnt.match.matchers;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DurationFormatUtils;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.CorrespondenceFormatter;
import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.DeterminantBasedDuplicateDetectionRule;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.blocking.DeterminantRecordBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.CorrespondenceTransitivityMaterialiser;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.EqualHeaderComparator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.ValueBasedSchemaMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.duplicatebased.DuplicateBasedSchemaVotingRule;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.DisjointHeaderMatchingRule;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.GraphBasedRefinement;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.VotingAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.NoSchemaBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardRecordBlocker;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;

public abstract class TableToTableMatcher extends TnTTask {

	public String getTaskName() {
		return this.getClass().getSimpleName();
	}
	
	private long runtime = 0;
	/**
	 * @return the runtime
	 */
	public long getRuntime() {
		return runtime;
	}
	
	private boolean verbose = false;
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public boolean isVerbose() {
		return verbose;
	}

	protected WebTables web;
	protected Map<String, Set<String>> disjointHeaders;
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences;
	protected File logDirectory;
	protected boolean matchContextColumnsByHeader = true;

	public void setLogDirectory(File f) {
		logDirectory = f;
	}

	
	/**
	 * @param disjointHeaders the disjointHeaders to set
	 */
	public void setDisjointHeaders(Map<String, Set<String>> disjointHeaders) {
		this.disjointHeaders = disjointHeaders;
	}
	/**
	 * @return the disjointHeaders
	 */
	public Map<String, Set<String>> getDisjointHeaders() {
		return disjointHeaders;
	}
	
	/**
	 * @param web the web to set
	 */
	public void setWebTables(WebTables web) {
		this.web = web;
	}

	/**
	 * @param matchContextColumnsByHeader the matchContextColumnsByHeader to set
	 */
	public void setMatchContextColumnsByHeader(boolean matchContextColumnsByHeader) {
		this.matchContextColumnsByHeader = matchContextColumnsByHeader;
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> getSchemaCorrespondences() {
		return schemaCorrespondences;
	}
	
	@Override
	public void initialise() throws Exception {

	}
	@Override
	public void match() throws Exception {
		long start = System.currentTimeMillis();
		
		if(verbose) {
			System.out.println("*** Pair-wise Matcher ***");
		}
		
		runMatching();
		System.out.println(String.format("Schema Correspondences are a %s", getSchemaCorrespondences().getClass().getName()));
		
		
		if(verbose) {
			CorrespondenceFormatter.printAttributeClusters(getSchemaCorrespondences());
//			CorrespondenceFormatter.printTableLinkageFrequency(getSchemaCorrespondences());
			
			System.out.println("*** Pair-wise Refinement ***");
		}
		
		schemaCorrespondences = runPairwiseRefinement(web, getSchemaCorrespondences(), new DisjointHeaders(disjointHeaders));
		System.out.println(String.format("Schema Correspondences are a %s", getSchemaCorrespondences().getClass().getName()));
		
		if(verbose) {
			CorrespondenceFormatter.printAttributeClusters(getSchemaCorrespondences());
//			CorrespondenceFormatter.printTableLinkageFrequency(getSchemaCorrespondences());
			
			System.out.println("*** Graph-based Refinement ***");
		}
		
		schemaCorrespondences = runGraphbasedRefinement(getSchemaCorrespondences(), new DisjointHeaders(disjointHeaders));
		
		if(verbose) {
			CorrespondenceFormatter.printAttributeClusters(getSchemaCorrespondences());
//			CorrespondenceFormatter.printTableLinkageFrequency(getSchemaCorrespondences());
			
			System.out.println("*** Transitivity ***");
		}
		
		schemaCorrespondences = applyTransitivity(schemaCorrespondences);
		
		if(verbose) {
			CorrespondenceFormatter.printAttributeClusters(getSchemaCorrespondences());
//			CorrespondenceFormatter.printTableLinkageFrequency(getSchemaCorrespondences());
		}
		
    	long end = System.currentTimeMillis();
    	
    	runtime = end-start;
    	
    	System.out.println(String.format("[%s] Matching finished after %s", this.getClass().getName(), DurationFormatUtils.formatDurationHMS(runtime)));
	}
	
	protected abstract void runMatching();
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> removeContextMatches(Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		// remove matches between context columns
		return schemaCorrespondences.where((c) -> 
				!(ContextColumns.isContextColumn(c.getFirstRecord()) && ContextColumns.isContextColumn(c.getSecondRecord()))
				);
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> runValueBased(WebTables web) {
		ValueBasedSchemaMatcher matcher = new ValueBasedSchemaMatcher();
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> cors = matcher.run(web.getRecords());

		if(verbose) {
			for(Correspondence<MatchableTableColumn, Matchable> cor : cors.get()) {
				MatchableTableColumn c1 = cor.getFirstRecord();
				MatchableTableColumn c2 = cor.getSecondRecord();
				System.out.println(String.format("{%d}[%d]%s <-> {%d}[%d]%s (%f)", c1.getTableId(), c1.getColumnIndex(), c1.getHeader(), c2.getTableId(), c2.getColumnIndex(), c2.getHeader(), cor.getSimilarityScore()));
			}
		}
		
		return cors;
	}
	
	protected Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> findDuplicates(
			WebTables web,
			Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> detCors) {
		
		MatchingEngine<MatchableTableRow, MatchableTableDeterminant> engine = new MatchingEngine<>();
		
		DataSet<MatchableTableRow, MatchableTableDeterminant> ds = new ParallelHashedDataSet<>(web.getRecords().get());
		DeterminantRecordBlockingKeyGenerator bkg = new DeterminantRecordBlockingKeyGenerator();
		DeterminantBasedDuplicateDetectionRule rule = new DeterminantBasedDuplicateDetectionRule(0.0);
		
		Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> duplicates = engine.runDuplicateDetection(ds, detCors, rule, new StandardRecordBlocker<>(bkg));
		
		return duplicates;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> runDuplicateBasedSchemaMatching(WebTables web, Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> duplicates) {
		
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();
		DuplicateBasedSchemaVotingRule rule = new DuplicateBasedSchemaVotingRule(0.0);
		VotingAggregator<MatchableTableColumn, MatchableTableRow> voteAggregator = new VotingAggregator<>(false, 1.0);
		NoSchemaBlocker<MatchableTableColumn, MatchableTableRow> schemaBlocker = new NoSchemaBlocker<>();
		
		// we must flatten the duplicates, such that each determinant that created a duplicate is processed individually by the matching rule
		// only then can we group the votes by determinant and make sense of the number of votes
		Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> flatDuplicates = new ProcessableCollection<>();
		Correspondence.flatten(duplicates, flatDuplicates);
		
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = engine.runDuplicateBasedSchemaMatching(web.getSchema(), web.getSchema(), flatDuplicates, rule, null, voteAggregator, schemaBlocker);

		return schemaCorrespondences;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> runPairwiseRefinement(WebTables web, Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences, DisjointHeaders disjointHeaders) throws Exception {
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();
		
		// run label-based matcher with equality comparator to generate correspondences
		EqualHeaderComparator labelComparator = new EqualHeaderComparator();
		labelComparator.setMatchContextColumns(matchContextColumnsByHeader);
		
		Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> labelBasedCors = engine.runLabelBasedSchemaMatching(web.getSchema(), labelComparator, 1.0);
		
		labelBasedCors = labelBasedCors.where((c)->c.getFirstRecord().getDataSourceIdentifier()!=c.getSecondRecord().getDataSourceIdentifier());
		
		// filter out correspondences between union context columns (are generated when creating the union tables)
		// - we cannot use the labels of union context columns for matching, because the names are generated
		// - generated columns for different union table clusters still have the same name, but this does not imply that they match!
		// labelBasedCors = labelBasedCors.where((c)->!ContextColumns.isRenamedUnionContextColumn(c.getFirstRecord().getHeader()));
		//BEGIN CHANGE 
		// labelBasedCors = labelBasedCors.where((c)->!ContextColumns.isContextColumnIgnoreCase(c.getFirstRecord().getHeader()));
		//NEW CODE:
		labelBasedCors = labelBasedCors.where((c)->!ContextColumns.isRenamedUnionContextColumn(c.getFirstRecord().getHeader()));
		//EFFECT: changes result of t2t matching (at least for itunes), why?
		//END CHANGE 

//		for(Correspondence<MatchableTableColumn, MatchableTableColumn> cor : labelBasedCors.get()) {
//			System.out.println(String.format("[LabelBased] %s <-> %s", cor.getFirstRecord(), cor.getSecondRecord()));
//		}
		
		// combine label- and value-based correspondences
		Processable<Correspondence<MatchableTableColumn, Matchable>> cors = schemaCorrespondences.append(Correspondence.toMatchable(labelBasedCors)).distinct();
		
		// apply disjoint header rule to remove inconsistencies
		DisjointHeaderMatchingRule rule = new DisjointHeaderMatchingRule(disjointHeaders, 0.0);
		// rule.setVerbose(verbose);

		cors = cors.map(rule);
		
		return cors;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> runGraphbasedRefinement(Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences, DisjointHeaders disjointHeaders) {

		GraphBasedRefinement refiner = new GraphBasedRefinement(true); 
		refiner.setVerbose(verbose);
		refiner.setLogDirectory(logDirectory);
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> refined = refiner.match(correspondences, disjointHeaders);
		
		return refined;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> applyTransitivity(Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		CorrespondenceTransitivityMaterialiser transitivity = new CorrespondenceTransitivityMaterialiser();
		Processable<Correspondence<MatchableTableColumn, Matchable>> transitiveCorrespondences = transitivity.aggregate(correspondences);
    	correspondences = correspondences.append(transitiveCorrespondences);
    	correspondences = correspondences.distinct();
    	
    	return correspondences;
	}
}
