/**
 * 
 */
package edu.buffalo.cse562.relationalAlgebra;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import edu.buffalo.cse562.table.Tuple;

/**
 * @author tanvi
 * 
 */
public class SelectOperator extends Operator {

	private Expression condition;
	private Tuple current;
	private Map<String, Integer> columnMap;

	public SelectOperator(Expression condition, Operator child) {
		this.condition = condition;
		this.child = child;
		this.schema = child.getSchema();
		columnMap = new HashMap<String, Integer>();
	}

	@Override
	public void open() {
		child.open();
	}

	/**
	 * @return the condition
	 */
	public Expression getCondition() {
		return condition;
	}

	/**
	 * @param condition
	 *            the condition to set
	 */
	public void setCondition(Expression condition) {
		this.condition = condition;
	}

	@Override
	public Tuple getNext() {
		try {
			while ((this.current = child.getNext()) != null) {

				// if (condition != null) {
				LeafValue val = this.eval(condition);
				BooleanValue bool_leaf = (BooleanValue) val;

				if (bool_leaf.getValue() == true)
					return current;
				// } else
				// return current;

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void close() {
		child.close();

	}

	@Override
	public LeafValue eval(Column arg0) throws SQLException {

		List<edu.buffalo.cse562.table.Column> columns = schema.getColumns();

		Expression exp = null;
		int colIdx = -1;
		String arg_n = arg0.getWholeColumnName();
		Integer id = columnMap.get(arg_n);
		if (id != null) {
			colIdx = id;
		} else {
			String arg_name = arg_n.toLowerCase();
			String colName;
			Table t = arg0.getTable();

			for (int i = 0; i < columns.size(); i++) {
				edu.buffalo.cse562.table.Column col = columns.get(i);
				// if
				// (Helper.columnAliasMap.containsKey(col.getWholeColumnName())
				// && arg0.getTable() != null && t.getName() != null) {
				// exp = Helper.columnAliasMap.get(col.getWholeColumnName());
				// if (exp instanceof Column) {
				// col.setAlias(col.getName());
				// col.setName(((Column) exp).getColumnName());
				// col.setTable(((Column) exp).getTable().getName());
				// }
				// }
				if (t == null || t.getName() == null) {
					colName = col.getName().toLowerCase();
				} else {
					colName = col.getWholeColumnName().toLowerCase();
				}
				if (arg_name.equals(colName)) {
					colIdx = i;
					break;
				}
			}
			columnMap.put(arg_n, colIdx);
		}
		return current.getData().get(colIdx);
	}

	@Override
	public void createSchema() {
		this.schema = child.getSchema();
	}
}
