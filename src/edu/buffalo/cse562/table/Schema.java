/**
 * 
 */
package edu.buffalo.cse562.table;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.Helper;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/**
 * @author ketkiram
 * 
 */
public class Schema {

	private List<Column> columns;
	private String tableName;
	private String alias;

	public Schema() {
		super();
		columns = new ArrayList<Column>();
		tableName = null;
		alias = tableName;
	}

	public Schema(List<ColumnDefinition> columnDefinitions, String fileName) {

		this.setTableName(fileName);
		alias = tableName;

		columns = new ArrayList<Column>(columnDefinitions.size());
		for (ColumnDefinition colDef : columnDefinitions) {
			Column column = new Column();
			column.setName(colDef.getColumnName());
			Helper.setColumnType(column, colDef.getColDataType().getDataType());
//			column.setType(colDef.getColDataType().getDataType());
			column.setTable(alias);
			columns.add(column);
		}
	}

	/**
	 * @return the columns
	 */
	public List<Column> getColumns() {
		return columns;
	}

	/**
	 * @return the attributeCount
	 */
	public int getNumAttribute() {
		return columns.size();
	}

	public String getTableName() {
		return tableName;
	}

	public String getColumnDataType(String ColumnName) {
		for (Column column : columns) {
			if (column.getWholeColumnName().equals(ColumnName))
				return column.getType();
		}
		return null;
	}
	
	public String getColumnDataTypeColumnName(String ColumnName) {
		for (Column column : columns) {
			if (column.getName().equals(ColumnName))
				return column.getType();
		}
		return null;
	}

	/**
	 * @param tableName
	 *            the tableName to set
	 */
	public void setTableName(String fileName) {
		this.tableName = fileName;
	}

	/**
	 * @return the alias
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * @param alias
	 *            the alias to set
	 */
	public void setAlias(String alias) {
		this.alias = alias;
		for (Column col : columns) {
			col.setTable(alias);
		}
	}

	@Override
	public String toString(){
		return this.tableName + "@" + this.getNumAttribute();
	}
}
