package edu.buffalo.cse562.relationalAlgebra;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.table.Schema;
import edu.buffalo.cse562.table.Tuple;

public class ProjectOperator extends Operator {

	private List<SelectItem> selectItems;
	private Tuple current;
	private Schema childSchema;
	private Map<String, Integer> columnMap;

	/**
	 * @param selectItems
	 * @param child
	 */
	public ProjectOperator(List<SelectItem> selectItems, Operator child) {
		this.selectItems = selectItems;
		this.child = child;
		this.schema = child.getSchema();
		this.childSchema = child.getSchema();
		this.columnMap = new HashMap<String, Integer>();
		createSchema();
	}

	@Override
	public void createSchema() {
		Schema result = new Schema();

		for (SelectItem item : selectItems) {
			if (item instanceof SelectExpressionItem) {
				SelectExpressionItem expItem = (SelectExpressionItem) item;
				Expression expression = expItem.getExpression();

				String name = null;
				if (expItem.getAlias() != null)
					name = expItem.getAlias();
				else {
					if (expression instanceof Column) {
						name = ((Column) expression).getWholeColumnName();
					} else if (expression instanceof Function) {
						name = ((Function) expression).getName();
					}
				}
				edu.buffalo.cse562.table.Column column = new edu.buffalo.cse562.table.Column();
				column.setName(name);
				result.getColumns().add(column);
			}
		}
		if (!result.getColumns().isEmpty())
			schema = result;

		// System.out.println("@@@@@@@@@@@@@@@@@");
		// for (edu.buffalo.cse562.table.Column col : schema.getColumns())
		// System.out.println(col.getWholeColumnName());
	}

	@Override
	public void open() {
		this.child.open();
	}

	@Override
	public Tuple getNext() {
		Tuple result = null;
		current = child.getNext();
		if (current != null) {
			result = new Tuple();
			try {
				for (SelectItem item : selectItems) {
					if (item instanceof SelectExpressionItem) {
						SelectExpressionItem expItem = (SelectExpressionItem) item;
						Expression expression = expItem.getExpression();
						if (expression instanceof Function) {
							Function arg0 = (Function) expression;
							List<edu.buffalo.cse562.table.Column> columns = childSchema
									.getColumns();

							String name = arg0.getName(), expName = "all";
							List<Expression> tempExp = null;
							if (arg0.getParameters() != null)
								tempExp = arg0.getParameters().getExpressions();
							if (tempExp != null && !tempExp.isEmpty())
								expName = tempExp.get(0).toString();
							String arg_name = (name + expName).toLowerCase();
							String colName;
							int colIdx = -1;
							for (int i = 0; i < columns.size(); i++) {
								edu.buffalo.cse562.table.Column col = columns
										.get(i);
								colName = col.getName().toLowerCase();
								if (arg_name.equals(colName)) {
									colIdx = i;
									break;
								}
							}
							// System.out.println(arg_name+":"+colIdx);
							result.getData().add(current.getData().get(colIdx));
						} else {
							LeafValue val = null;

							// System.out.println(expression);
							val = this.eval(expression);

							if (val != null) {
								result.getData().add(val);
							}
						}
					}
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		return result;
	}

	@Override
	public void close() {
		child.close();

	}

	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		List<edu.buffalo.cse562.table.Column> columns = childSchema
				.getColumns();
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
				// && t != null && t.getName() != null) {
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

		// System.out.println(arg_name+":"+colIdx);
		return current.getData().get(colIdx);
	}

}
