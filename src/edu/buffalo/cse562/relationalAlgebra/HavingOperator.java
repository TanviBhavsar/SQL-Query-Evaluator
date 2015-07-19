package edu.buffalo.cse562.relationalAlgebra;

import java.sql.SQLException;
import java.util.List;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.table.Tuple;

public class HavingOperator extends Operator {
	private Expression havingCondition;
	private Operator source;
	private Tuple current;

	public HavingOperator(Expression havingCondition, Operator child) {
		this.havingCondition = havingCondition;
		this.source = child;
		this.schema = child.getSchema();
	}

	@Override
	public void open() {
		source.open();
	}

	@Override
	public Tuple getNext() {
		this.current = source.getNext();
		if (current != null) {
			try {
				if (havingCondition != null) {
					LeafValue val = this.eval(havingCondition);
					BooleanValue bool_leaf = (BooleanValue) val;

					if (bool_leaf.getValue() == false)
						this.current = getNext();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return current;
	}

	@Override
	public void close() {
		source.close();

	}

	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		/*
		 * List<edu.buffalo.cse562.table.Column> columns = schema.getColumns();
		 * int colIdx = columns.indexOf(arg0); return
		 * current.getData().get(colIdx);
		 */

		List<edu.buffalo.cse562.table.Column> columns = schema.getColumns();
		String arg_name = arg0.getColumnName();
		int colIdx = -1;
		for (int i = 0; i < columns.size(); i++) {
			if (arg_name.equals(columns.get(i).getName())) {
				colIdx = i;
				break;
			}
		}
		return current.getData().get(colIdx);
	}

	@Override
	public void createSchema() {
		// TODO Auto-generated method stub

	}
}
