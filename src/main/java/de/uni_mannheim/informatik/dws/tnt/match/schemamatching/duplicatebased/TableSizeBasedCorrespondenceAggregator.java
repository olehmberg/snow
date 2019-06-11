package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.duplicatebased;

import java.util.Map;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.VotingAggregator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;

public class TableSizeBasedCorrespondenceAggregator extends VotingAggregator<MatchableTableColumn, MatchableTableRow> {

	private Map<Integer, Integer> rowCounts;
	
	public TableSizeBasedCorrespondenceAggregator(boolean useWeightedVoting, double finalThreshold, Map<Integer, Integer> rowCounts) {
		super(useWeightedVoting, finalThreshold);
		this.rowCounts = rowCounts;
	}

	@Override
	public Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Object> aggregate(
			Correspondence<MatchableTableColumn, MatchableTableRow> previousResult,
			Correspondence<MatchableTableColumn, MatchableTableRow> record, Object state) {
		
		// remove votes with similarity score 0 from causal correspondences
		if(record.getSimilarityScore()>0.0) {
			return super.aggregate(previousResult, record, state);
		} else {
			return stateless(previousResult);
		}
	}
	
	private static final long serialVersionUID = 1L;

	@Override
	public Correspondence<MatchableTableColumn, MatchableTableRow> createFinalValue(
			Pair<MatchableTableColumn, MatchableTableColumn> keyValue,
			Correspondence<MatchableTableColumn, MatchableTableRow> result, Object state) {
		if(result!=null) {
			int leftTableId = result.getFirstRecord().getTableId();
			int leftRowCount = rowCounts.get(leftTableId);
	
			result.setsimilarityScore(result.getSimilarityScore() / (double)leftRowCount);
	
			if(result.getSimilarityScore()>0.0 && result.getSimilarityScore()>=getFinalThreshold() && result.getFirstRecord().getType()==result.getSecondRecord().getType()) {
				return result;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}
}
