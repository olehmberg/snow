package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.duplicatebased;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.VotingMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * Determines if the two given columns have the same value in the record combination given by the instance correspondence.
 * Creates a vote for matching values that is grouped by both columns and the determiant correspondence that created the instance correspondence
 * 
 * @author Oliver
 *
 */
public class DuplicateBasedSchemaVotingRule extends VotingMatchingRule<MatchableTableColumn, MatchableTableRow> {

	public DuplicateBasedSchemaVotingRule(double finalThreshold) {
		super(finalThreshold);
	}

	private static final long serialVersionUID = 1L;

	@Override
	public double compare(MatchableTableColumn record1, MatchableTableColumn record2,
			Correspondence<MatchableTableRow, Matchable> correspondence) {
		
		if(record1.getDataSourceIdentifier()!=record2.getDataSourceIdentifier()) {
			MatchableTableRow row1 = null;
			MatchableTableRow row2 = null;
			
			if(record1.getDataSourceIdentifier()==correspondence.getFirstRecord().getDataSourceIdentifier()) {
				row1 = correspondence.getFirstRecord();
				row2 = correspondence.getSecondRecord();
			} else {
				row2 = correspondence.getFirstRecord();
				row1 = correspondence.getSecondRecord();
			}		
			
			Object value1 = row1.get(record1.getColumnIndex());
			Object value2 = row2.get(record2.getColumnIndex());
				
			return Q.equals(value1, value2, true) ? 1.0 : 0.0;
		} else {
			return 0.0;
		}
	}

	@Override
	protected Pair<MatchableTableColumn, MatchableTableColumn> generateAggregationKey(
			Correspondence<MatchableTableColumn, MatchableTableRow> cor) {
		
		// the correspondence that we get here is created from the result of the compare method.
		// it connects the two schema elements, and has the correspondence that is the duplicate as cause.
		// in turn, this duplicate is created from a determinant, and this is the one that we want to group by
		Correspondence<MatchableTableRow, Matchable> duplicate = Q.firstOrDefault(cor.getCausalCorrespondences().get());
		
		Correspondence<Matchable, Matchable> det = Q.firstOrDefault(duplicate.getCausalCorrespondences().get());

		if(cor.getCausalCorrespondences().size()>1) {
			System.out.println("More than one duplicate for this pair!");
		}
		
		if(duplicate.getCausalCorrespondences().size()>1) {
			System.out.println("More than one determinant for this duplicate!");
		}

		
		return new PairWithDeterminant<MatchableTableColumn, MatchableTableColumn, Correspondence<Matchable, Matchable>>(
				cor.getFirstRecord(), 
				cor.getSecondRecord(), 
				det);
	}
}
