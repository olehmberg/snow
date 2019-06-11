package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.winter.model.Fusible;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.utils.SparseArray;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

public class MatchableTableRow implements Matchable, Fusible<MatchableTableColumn>, Serializable, Comparable<MatchableTableRow> {

	public static class MatchableTableRowToTableId implements Function<Integer, MatchableTableRow> {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Integer execute(MatchableTableRow input) {
			return input.getTableId();
		}
	}
	
	public static class RowNumberProjection implements Func<Integer, MatchableTableRow>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public Integer invoke(MatchableTableRow in) {
			return in.getRowNumber();
		}
		
	}
	
	public static class RowNumberComparator implements Comparator<MatchableTableRow>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(MatchableTableRow o1, MatchableTableRow o2) {
			return Integer.compare(o1.getRowNumber(), o2.getRowNumber());
		}
		
	}
	
	public static class TableIdComparator implements Comparator<MatchableTableRow>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(MatchableTableRow o1, MatchableTableRow o2) {
			return Integer.compare(o1.getTableId(), o2.getTableId());
		}
		
	}
	
	private static final long serialVersionUID = 1L;

	public MatchableTableRow(String id) {
		this.id = id;
		this.tableId = -1;
		this.rowNumber = -1;
	}
	
	@Deprecated
	public MatchableTableRow(TableRow row, int tableId) { 
		this.tableId = tableId;
		this.rowNumber = row.getRowNumber();
		this.id = row.getIdentifier();
		this.rowLength = row.getTable().getSchema().getSize();
		
		ArrayList<DataType> types = new ArrayList<>();
		ArrayList<TableColumn> cols = new ArrayList<>(row.getTable().getSchema().getRecords());
		Collections.sort(cols, new TableColumn.TableColumnByIndexComparator());
		for(TableColumn c : cols) {
			types.add(c.getDataType());
		}
		
		if(types.size()<row.getValueArray().length) {
			System.err.println("problem");
		}
		
		SparseArray<Object> valuesSparse = new SparseArray<>(row.getValueArray());
		this.values = valuesSparse.getValues();
		this.indices = valuesSparse.getIndices();
		
		this.types = new DataType[values.length];
		for (int i = 0; i < indices.length; i++) {
			this.types[i] = types.get(indices[i]);
		}
	}
	
	public MatchableTableRow(TableRow row, int tableId, MatchableTableColumn[] schema) { 
		this.tableId = tableId;
		this.rowNumber = row.getRowNumber();
		this.id = row.getIdentifier();
		this.rowLength = row.getTable().getSchema().getSize();
		
		ArrayList<DataType> types = new ArrayList<>();
		ArrayList<TableColumn> cols = new ArrayList<>(row.getTable().getSchema().getRecords());
		Collections.sort(cols, new TableColumn.TableColumnByIndexComparator());
		for(TableColumn c : cols) {
			types.add(c.getDataType());
		}
		
		if(types.size()<row.getValueArray().length) {
			System.err.println(String.format("TableRow %s has more values than column types!\n\t%s\n\t%s\n\t%s", 
					id,
					StringUtils.join(schema, ","),
					StringUtils.join(types, ","),
					StringUtils.join(row.getValueArray(), ",")
					));
		}
		
		SparseArray<Object> valuesSparse = new SparseArray<>(row.getValueArray());
		this.values = valuesSparse.getValues();
		this.indices = valuesSparse.getIndices();
		
		this.types = new DataType[values.length];
		for (int i = 0; i < indices.length; i++) {
			this.types[i] = types.get(indices[i]);
		}
		
		this.schema = schema;
		keys = new MatchableTableColumn[row.getTable().getSchema().getCandidateKeys().size()][];
		int i = 0;
		for(Collection<TableColumn> candKey : row.getTable().getSchema().getCandidateKeys()) {
			keys[i++] = Q.project(schema, Q.toPrimitiveIntArray(Q.sort(Q.project(candKey, new TableColumn.ColumnIndexProjection()))));
		}
		
	}
	
	protected String id;
	protected DataType[] types;
	protected Object[] values;
	protected int[] indices;
	protected int rowNumber;
	protected int tableId;
	protected int rowLength; // total number of columns (including null values)
	protected MatchableTableColumn[] schema;
	protected MatchableTableColumn[][] keys;
	
	@Override
	public String getIdentifier() {
		return id;
	}

	@Override
	public String getProvenance() {
		return null;
	}

	public int getNumCells() {
		return values.length;
	}
	public Object get(int columnIndex) {
		if(indices!=null) {
			return SparseArray.get(columnIndex, values, indices);
		} else {
			return values[columnIndex];
		}
	}
	public Object[] get(int[] columnIndices) {
		Object[] result = new Object[columnIndices.length];
		
		int j=0;
		for(int i : columnIndices) {
			result[j++] = get(i); 
		}
		
		return result;
	}
	
	/**
	 * Sets the respective value. If the value didn't exist before, the sparse representation is replaced by a dense representation, which can lead to higher memory consumption 
	 * @param columnIndex
	 * @param value
	 */
	public void set(int columnIndex, Object value) {
		int maxLen = columnIndex+1;
		
		if(indices!=null) {
			maxLen = Math.max(maxLen, indices[indices.length-1]+1);
			
			Object[] allValues = new Object[maxLen];
			for(int i=0;i<indices.length;i++) {
				allValues[indices[i]] = values[i];
			}
			
			values = allValues;
			indices = null;
		} else {
			if(maxLen>values.length) {
				values = Arrays.copyOf(values, maxLen);
			}
		}
		
		values[columnIndex] = value;
	}
	
	public DataType getType(int columnIndex) {
		if(indices!=null) {
			int idx = SparseArray.translateIndex(columnIndex, indices);
			
			if(idx==-1) {
				return null;
			} else {
				return types[idx];
			}
		} else {
			return types[columnIndex];
		}
	}
	public Object[] getValues() {
		Object[] result = new Object[rowLength];
		
		int j=0;
		for(int i=0; i< rowLength; i++) {
			result[j++] = get(i); 
		}
		
		return result;
	}
	public DataType[] getTypes() {
		return types;
	}
	public int getRowNumber() {
		return rowNumber;
	}
	public int getTableId() {
		return tableId;
	}
	@Override
	public int getDataSourceIdentifier() {
		return getTableId();
	}
	public MatchableTableColumn[][] getKeys() {
		return keys;
	}
	/**
	 * @return the schema
	 */
	public MatchableTableColumn[] getSchema() {
		return schema;
	}
	/**
	 * @return the rowLength
	 */
	public int getRowLength() {
		return rowLength;
	}
	
	public boolean hasColumn(int columnIndex) {
		if(indices!=null) {
			int idx = SparseArray.translateIndex(columnIndex, indices);
		
			return idx!=-1;
		} else {
			return columnIndex < values.length;
		}
	}


	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Fusable#hasValue(java.lang.String)
	 */
	@Override
	public boolean hasValue(MatchableTableColumn attribute) {
		return hasColumn(attribute.getColumnIndex()) && get(attribute.getColumnIndex())!=null;		
	}
	
	public String format(int columnWidth) {
		StringBuilder sb = new StringBuilder();
		
		boolean first=true;
		for(MatchableTableColumn c : getSchema()) {
			
			if(!first) {
				sb.append(" | ");
			}
			
			String value;
			if(hasColumn(c.getColumnIndex())) {
				Object v = get(c.getColumnIndex());
				
				if(v.getClass().isArray()) {
					value = StringUtils.join((Object[])v, "|");
				} else {
					value = v.toString();
				}
			} else {
				value = "null";
			}
			
			sb.append(padRight(value,columnWidth));

			first = false;
		}
		
		return sb.toString();
	}
	
	public String formatSchema(int columnWidth) {
		StringBuilder sb = new StringBuilder();
		
		boolean first=true;
		for(MatchableTableColumn c : getSchema()) {
			
			if(!first) {
				sb.append(" | ");
			}
			
			String value;
			Object v = String.format("[%d] %s", c.getColumnIndex(), c.getHeader());
			
			if(v.getClass().isArray()) {
				value = StringUtils.join((Object[])v, "|");
			} else {
				value = v.toString();
			}
			
			sb.append(padRight(value,columnWidth));

			first = false;
		}
		
		return sb.toString();
	}

    protected String padRight(String s, int n) {
        if(n==0) {
            return "";
        }
        if (s.length() > n) {
            s = s.substring(0, n);
        }
        s = s.replace("\n", " ");
        return String.format("%1$-" + n + "s", s);
    }

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(MatchableTableRow o) {
		return getIdentifier().compareTo(o.getIdentifier());
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("{#%d} #%d", getTableId(), getRowNumber());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MatchableTableRow other = (MatchableTableRow) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
	
	
}
