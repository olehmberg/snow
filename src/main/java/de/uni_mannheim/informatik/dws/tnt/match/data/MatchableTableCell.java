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

public class MatchableTableCell implements Matchable, Fusible<MatchableTableColumn>, Comparable<MatchableTableCell> {

    private String id;
    // private String rowId;
    // private String colId;
    // private Object value;
    // private int tableId;
    // private int columnIndex;
    // private int rowIndex;
    private MatchableTableColumn column;
    private MatchableTableRow row;

    public MatchableTableCell(MatchableTableRow row, MatchableTableColumn col) {
        this.id = String.format("%s/%s", col.getIdentifier(), row.getIdentifier());
        // this.value = row.get(col.getColumnIndex());
        // this.tableId = row.getTableId();
        // this.columnIndex = col.getColumnIndex();
        // this.rowIndex = row.getRowNumber();
        this.column = col;
        this.row = row;
    }

    /**
     * @return the column
     */
    public MatchableTableColumn getColumn() {
        return column;
    }

    /**
     * @return the row
     */
    public MatchableTableRow getRow() {
        return row;
    }

    /**
     * @return the columnIndex
     */
    public int getColumnIndex() {
        return column.getColumnIndex();
    }

    /**
     * @return the rowIndex
     */
    public int getRowIndex() {
        return row.getRowNumber();
    }

    @Override
    public int getDataSourceIdentifier() {
        return column.getDataSourceIdentifier();
    }

    @Override
    public int compareTo(MatchableTableCell o) {
        return getIdentifier().compareTo(o.getIdentifier());
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    @Override
    public String getProvenance() {
        return null;
    }

    @Override
    public boolean hasValue(MatchableTableColumn attribute) {
        return getColId().equals(attribute.getIdentifier());
    }

    /**
     * @return the value
     */
    public Object getValue() {
        return row.get(column.getColumnIndex());
    }

    /**
     * @return the rowId
     */
    public String getRowId() {
        return row.getIdentifier();
    }

    /**
     * @return the colId
     */
    public String getColId() {
        return column.getIdentifier();
    }

    public String formatValue() {
        Object[] listValues = null;
        if(getValue().getClass().isArray()) {
            listValues = (Object[])getValue();
        } else {
            listValues = new Object[] { getValue() };
        }
        return StringUtils.join(listValues, "|");
    }

}