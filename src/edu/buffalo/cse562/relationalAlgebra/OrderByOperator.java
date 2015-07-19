/**
 * 
 */
package edu.buffalo.cse562.relationalAlgebra;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import edu.buffalo.cse562.table.Tuple;

/**
 * @author Dell
 * 
 */
public class OrderByOperator extends Operator {

	private List<OrderByElement> orderBy;
	private int currentIdx;
	private List<Tuple> orderedList;

	public OrderByOperator(List<OrderByElement> orderByList, Operator root) {
		this.orderBy = orderByList;
		this.child = root;
		currentIdx = 0;
		this.schema = child.getSchema();

		orderedList = new ArrayList<Tuple>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.buffalo.cse562.relationalAlgebra.Operator#open()
	 */
	@Override
	public void open() {
		child.open();

		Tuple tuple = null;
		while ((tuple = child.getNext()) != null) {
			orderedList.add(tuple);
		}

		Collections.sort(orderedList, new Comparator<Tuple>() {
			private int compareNext(int index, Tuple a, Tuple b) {
				OrderByElement order = null;
				int colIdx = -1;
				order = orderBy.get(index);
				Expression exp = order.getExpression();

				String col = ((Column) exp).getWholeColumnName().toLowerCase();
				List<edu.buffalo.cse562.table.Column> colList = schema
						.getColumns();
				for (int i = 0; i < colList.size(); i++) {
					if (col.equals(
							colList.get(i).getWholeColumnName().toLowerCase())
							|| colList.get(i).getWholeColumnName().toLowerCase()
									.contains(col)) {
						colIdx = i;
						break;
					}
				}
				if (colIdx == -1)
					return 0;
				LeafValue val1 = a.getData().get(colIdx);
				LeafValue val2 = b.getData().get(colIdx);

				int retVal = val1.toString().compareTo(val2.toString());

				// if (val1 instanceof DateValue) {
				// Date d1 = ((DateValue) val1).getValue();
				// Date d2 = ((DateValue) val2).getValue();
				// retVal = d1.compareTo(d2);
				// } else
				if (val1 instanceof DoubleValue) {
					Double d1 = ((DoubleValue) val1).getValue();
					Double d2 = ((DoubleValue) val2).getValue();
					retVal = d1.compareTo(d2);
				} else if (val1 instanceof LongValue) {
					Long l1 = ((LongValue) val1).getValue();
					Long l2 = ((LongValue) val2).getValue();
					retVal = l1.compareTo(l2);
				}
				return (order.isAsc() ? retVal : -retVal);
			}

			// @Override
			@Override
			public int compare(Tuple a, Tuple b) {
				int index = 0, retVal = 0;

				while ((retVal = compareNext(index, a, b)) == 0) {
					index++;
					if (index >= orderBy.size()) {
						break;
					}
				}
				return retVal;

			}
		});

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.buffalo.cse562.relationalAlgebra.Operator#getNext()
	 */
	@Override
	public Tuple getNext() {
		if (currentIdx < orderedList.size()) {
			Tuple result = orderedList.get(currentIdx);
			currentIdx++;

			return result;
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.buffalo.cse562.relationalAlgebra.Operator#close()
	 */
	@Override
	public void close() {
		child.close();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.buffalo.cse562.Eval#eval(net.sf.jsqlparser.schema.Column)
	 */
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void createSchema() {
		this.schema = child.getSchema();
	}

}
