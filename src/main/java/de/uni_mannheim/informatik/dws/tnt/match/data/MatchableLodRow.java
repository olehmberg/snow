package de.uni_mannheim.informatik.dws.tnt.match.data;

import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

public class MatchableLodRow extends MatchableTableRow {

	private static final long serialVersionUID = 1L;
	
	public MatchableLodRow(TableRow row, int tableId, MatchableTableColumn[] schema) { 
		super(row, tableId, schema);

//		this.id = String.format("%s:%s", row.getTable().getPath(), row.getIdentifier());		
	}
	
}
