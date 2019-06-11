package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.util.Collection;
import java.util.Comparator;
import org.apache.commons.lang.StringUtils;
import de.uni_mannheim.informatik.dws.winter.model.Fusible;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

public class MatchableTableColumn implements Matchable, Fusible<MatchableTableColumn>, Comparable<MatchableTableColumn> {

	public static class ColumnIdProjection implements Func<String, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public String invoke(MatchableTableColumn in) {
			return in.getIdentifier();
		}
		
	}
	
	public static class ColumnIndexProjection implements Func<Integer, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public Integer invoke(MatchableTableColumn in) {
			return in.getColumnIndex();
		}
		
	}
	
	public static class ColumnHeaderProjection implements Func<String, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public String invoke(MatchableTableColumn in) {
			return in.getHeader();
		}
		
	}
	
	public static class ColumnHeaderWithIndexProjection implements Func<String, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public String invoke(MatchableTableColumn in) {
			return String.format("[%d]%s", in.getColumnIndex(), in.getHeader());
		}
		
	}
	
	public static class IsStringColumnProjection implements Func<Boolean, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public Boolean invoke(MatchableTableColumn in) {
			return in.getType()==DataType.string;
		}
		
	}
	
	public static class ColumnIndexComparator implements Comparator<MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(MatchableTableColumn o1, MatchableTableColumn o2) {
			return Integer.compare(o1.getColumnIndex(), o2.getColumnIndex());
		}
		
	}
	
	public static class TableIdColumnIndexComparator implements Comparator<MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(MatchableTableColumn o1, MatchableTableColumn o2) {
			int result = Integer.compare(o1.getTableId(), o2.getTableId());
			
			if(result!=0) {
				return result;
			}
			
			return Integer.compare(o1.getColumnIndex(), o2.getColumnIndex());
		}
		
	}
	
	protected int tableId;
	protected int columnIndex;
	protected String id;
	protected DataType type;
	protected String header;
	
	/**
	 * @return the header
	 */
	public String getHeader() {
		return header;
	}

	/**
	 * @param header the header to set
	 */
	public void setHeader(String header) {
		this.header = header;
	}
	public int getTableId() {
		return tableId;
	}
	@Override
	public int getDataSourceIdentifier() {
		return getTableId();
	}
	public int getColumnIndex() {
		return columnIndex;
	}
	public MatchableTableColumn() {
		
	}
	
	public MatchableTableColumn(String identifier) {
		this.id = identifier;
	}
	
	public MatchableTableColumn(int tableId, TableColumn c) {
		this.tableId = tableId;
		this.columnIndex = c.getColumnIndex();
		this.type = c.getDataType();
		this.header = c.getHeader();
		
		// this controls the schema that we are matching to!
		// using c.getIdentifier() all dbp properties only exist once! (i.e. we cannot handle "_label" columns and the value of tableId is more or less random
		this.id = c.getUniqueName();
	}
	
	@Override
	public String getIdentifier() {
		return id;
	}

	@Override
	public String getProvenance() {
		return null;
	}

	/**
	 * @return the type
	 */
	public DataType getType() {
		return type;
	}
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Fusable#hasValue(java.lang.Object)
	 */
	@Override
	public boolean hasValue(MatchableTableColumn attribute) {
		return false;
	}
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(MatchableTableColumn o) {
		return getIdentifier().compareTo(o.getIdentifier());
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return getIdentifier().hashCode();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof MatchableTableColumn) {
			MatchableTableColumn col = (MatchableTableColumn)obj;
			return getIdentifier().equals(col.getIdentifier());
		} else {
			return super.equals(obj);
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("{#%d}[%d]%s", getTableId(), getColumnIndex(), getHeader());
	}

	public static String formatCollection(Collection<MatchableTableColumn> collection) {
		return StringUtils.join(Q.project(collection, new MatchableTableColumn.ColumnHeaderProjection()), ",");
	}
}
