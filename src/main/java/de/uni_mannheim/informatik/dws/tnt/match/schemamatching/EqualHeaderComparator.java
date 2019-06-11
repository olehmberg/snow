package de.uni_mannheim.informatik.dws.tnt.match.schemamatching;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

public class EqualHeaderComparator implements Comparator<MatchableTableColumn, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;

	private boolean nullEqualsNull = false;
	private boolean matchContextColumns = true;
	/**
	 * If set to false, this comparator will not consider context columns as potential matches for any other column
	 * 
	 * @param matchContextColumns the matchContextColumns to set
	 */
	public void setMatchContextColumns(boolean matchContextColumns) {
		this.matchContextColumns = matchContextColumns;
	}
	
	/**
	 * If set to true, two column headers which are null will be considered as equal
	 * @param nullEqualsNull the nullEqualsNull to set
	 */
	public void setNullEqualsNull(boolean nullEqualsNull) {
		this.nullEqualsNull = nullEqualsNull;
	}

	@Override
	public double compare(MatchableTableColumn record1, MatchableTableColumn record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {

		String h1 = record1.getHeader();
		String h2 = record2.getHeader();
		
		// do not match context columns to each other - they might have different semantics for different pages 
		if("null".equals(h1) || (!matchContextColumns && ContextColumns.isContextColumn(record1))) {
			h1 = null;
		}
		if("null".equals(h2) || (!matchContextColumns && ContextColumns.isContextColumn(record2))) {
			h2 = null;
		}
		
		return Q.equals(h1, h2, nullEqualsNull) ? 1.0 : 0.0;
	}

}
