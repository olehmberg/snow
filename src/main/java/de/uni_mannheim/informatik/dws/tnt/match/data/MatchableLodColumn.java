package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.io.Serializable;

import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.lod.LodTableColumn;

/**
 * 
 * Model of a property from the knowledge base.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableLodColumn extends MatchableTableColumn implements Serializable{

	private static final long serialVersionUID = 1L;

	private String range;
	private String uri;
	
	public static MatchableLodColumn fromCSV(String[] values) {
		MatchableLodColumn c = new MatchableLodColumn();
		
		c.tableId = -1;
		c.columnIndex = -1;
		c.type = DataType.valueOf(values[1]);
		c.id = values[0];
		
		return c;
	}
	
	public static final int CSV_LENGTH = 2;
	
	public MatchableLodColumn() {
		
	}
	
	public MatchableLodColumn(String identifier, int tableId, int columnIndex, String header) {
		this.id = identifier;
		this.tableId = tableId;
		this.columnIndex = columnIndex;
		this.header = header;
		this.range = "none";
	}
	
	public MatchableLodColumn(String className, String uri, String range, int tableId, int columnIndex, String header, DataType type) {
		this.id = String.format("%s::%s", className, uri);
		this.tableId = tableId;
		this.columnIndex = columnIndex;
		this.header = header;
		this.range = range;
		this.type = type;
		this.uri = uri;
	}
	
	public MatchableLodColumn(int tableId, TableColumn c) {
		super(tableId, c);
		
		this.id = String.format("%s::%s", c.getTable().getPath(), c.getIdentifier());
		this.uri = c.getUri();
		
		if(c instanceof LodTableColumn) {
			this.range = ((LodTableColumn)c).getRange();
		}
	}

	public String getRange() {
		return range;
	}
	public void setRange(String range) {
		this.range = range;
	}
	
	public String getUri() {
		return uri;
	}
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s", getIdentifier());
	}
	
}
