/**
 * 
 */
package edu.buffalo.cse562.relationalAlgebra;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import edu.buffalo.cse562.Helper;
import edu.buffalo.cse562.optimizer.ExternalSorter;
import edu.buffalo.cse562.table.Schema;
import edu.buffalo.cse562.table.Tuple;

/**
 * @author tanvi
 * 
 */
public class JoinOperator extends Operator {

	private Expression condition;
	private Operator leftChild;
	private Operator rightChild;
	private Tuple leftCurrent;
	private Tuple rightCurrent;
	private Tuple current;
	private Schema schema;
	private int index;
	private List<Tuple> joinTable;
	private List<Expression> expressionList;
	private int joinTableIdx;
	private RelationOperator relation;
	private Map<String, Integer> columnMap;

	public JoinOperator(Expression where, Operator fromOperator, Operator right) {
		condition = where;
		leftChild = fromOperator;
		rightChild = right;
		current = null;

		index = 0;
		joinTableIdx = 0;
		relation = null;
		joinTable = new ArrayList<Tuple>();
		columnMap = new HashMap<String, Integer>();
		createSchema();
	}

	@Override
	public void createSchema() {
		schema = new Schema();

		schema.getColumns().addAll(leftChild.getSchema().getColumns());
		schema.getColumns().addAll(rightChild.getSchema().getColumns());

	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
		createJoinTable();
	}

	private void createJoinTable() {
		if (Helper.swapParam != null) {
			joinWithSortMerge();
		} else
			joinWithGraceHash();
		// joinWithNestedLoop();
	}

	private void joinWithNestedLoop() {
		Tuple temp;

		leftChild.open();
		try {
			while ((leftCurrent = leftChild.getNext()) != null) {
				rightChild.open();
				while ((rightCurrent = rightChild.getNext()) != null) {
					if (condition != null) {
						LeafValue val = null;

						val = this.eval(condition);

						BooleanValue bool_leaf = (BooleanValue) val;
						if (bool_leaf != null && bool_leaf.getValue() == true) {
							temp = new Tuple();
							temp.getData().addAll(leftCurrent.getData());
							temp.getData().addAll(rightCurrent.getData());
							joinTable.add(temp);
						}
					} else {
						temp = new Tuple();
						temp.getData().addAll(leftCurrent.getData());
						temp.getData().addAll(rightCurrent.getData());
						joinTable.add(temp);
					}
				}
				rightChild.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void joinWithGraceHash() {
		Map<String, List<Tuple>> graceHashMap;
		expressionList = new ArrayList<Expression>();

		BinaryExpression exp;
		if (condition instanceof AndExpression) {
			while ((exp = (BinaryExpression) condition).getLeftExpression() instanceof BinaryExpression) {
				condition = exp.getRightExpression();
				expressionList.add(condition);
				condition = exp.getLeftExpression();
			}
		}
		expressionList.add(condition);
		List<Column> leftList = new ArrayList<Column>(), rightList = new ArrayList<Column>();
		for (Expression ex : expressionList) {
			exp = (BinaryExpression) ex;
			Column leftJoinColumn, rightJoinColumn = (Column) exp
					.getRightExpression();

			edu.buffalo.cse562.table.Column col = new edu.buffalo.cse562.table.Column();
			col.setTable(rightJoinColumn.getTable().getName());
			col.setName(rightJoinColumn.getColumnName());

			if (rightChild.getSchema().getColumns().contains(col)) {
				leftJoinColumn = (Column) exp.getLeftExpression();
			} else {
				leftJoinColumn = rightJoinColumn;
				rightJoinColumn = (Column) exp.getLeftExpression();
			}

			leftList.add(leftJoinColumn);
			rightList.add(rightJoinColumn);
		}
		graceHashMap = new HashMap<String, List<Tuple>>();

		StringBuilder keyString;
		List<Tuple> values = null;
		try {
			while ((leftCurrent = leftChild.getNext()) != null) {
				keyString = new StringBuilder();
				for (Column leftJoinColumn : leftList) {
					LeafValue val = null;

					val = this.eval(leftJoinColumn);

					keyString.append(val);
					if (leftList.size() > 1)
						keyString.append(Helper.delimitor);
				}
				values = graceHashMap.get(keyString.toString());
				if (values == null)
					values = new ArrayList<Tuple>();
				values.add(leftCurrent);
				graceHashMap.put(keyString.toString(), values);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		leftChild.close();
		// rightChild.open();
		Tuple temp;
		try {
			while ((rightCurrent = rightChild.getNext()) != null) {
				keyString = new StringBuilder();
				for (Column rightJoinColumn : rightList) {
					LeafValue val = null;

					val = this.eval(rightJoinColumn);

					keyString.append(val);
					if (leftList.size() > 1)
						keyString.append(Helper.delimitor);
				}
				values = graceHashMap.get(keyString.toString());
				if (values != null) {
					for (Tuple value : values) {
						temp = new Tuple();
						temp.getData().addAll(value.getData());
						temp.getData().addAll(rightCurrent.getData());
						joinTable.add(temp);
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		rightChild.close();
	}

	private void joinWithSortMerge() {
		Long time = System.currentTimeMillis();
		int maxSize = 20000;
		Tuple temp, prevLeft = null, prevRight = null;

		BinaryExpression exp;
		exp = (BinaryExpression) condition;
		Column leftJoinColumn, rightJoinColumn = (Column) exp
				.getRightExpression();

		edu.buffalo.cse562.table.Column col = new edu.buffalo.cse562.table.Column();
		col.setTable(rightJoinColumn.getTable().getName());
		col.setName(rightJoinColumn.getColumnName());

		if (rightChild.getSchema().getColumns().contains(col)) {
			leftJoinColumn = (Column) exp.getLeftExpression();
		} else {
			leftJoinColumn = rightJoinColumn;
			rightJoinColumn = (Column) exp.getLeftExpression();
		}

		if (leftChild instanceof JoinOperator) {
			((JoinOperator) leftChild).doExternalSort(leftJoinColumn);

		}
		leftCurrent = leftChild.getNext();
		rightCurrent = rightChild.getNext();
		LeafValue val1 = null, val2 = null, preVal1 = null, preVal2 = null;
		try {
			while (leftCurrent != null && rightCurrent != null) {

				val1 = this.eval(leftJoinColumn);
				val2 = this.eval(rightJoinColumn);

				int retVal;
				if (preVal1 != null) {
					while (val2.equals(preVal1)) {
						temp = new Tuple();
						temp.getData().addAll(prevLeft.getData());
						temp.getData().addAll(rightCurrent.getData());
						joinTable.add(temp);
						if (joinTable.size() >= maxSize) {
							if (joinTableIdx == 0)
								joinTableIdx = Helper.joinTableIdx++;
							String fileName = Helper.swapParam + File.separator
									+ "joinTable" + joinTableIdx
									+ Helper.fileFormat;
							ExternalSorter.writeToDisk(joinTable, fileName);
							joinTable.clear();
						}
						rightCurrent = rightChild.getNext();
						if (rightCurrent == null) {
							if (joinTableIdx > 0 && joinTable.size() > 0) {
								String fileName = Helper.swapParam
										+ File.separator + "joinTable"
										+ joinTableIdx + Helper.fileFormat;
								ExternalSorter.writeToDisk(joinTable, fileName);
								joinTable.clear();
							}
							return;
						}
						val2 = this.eval(rightJoinColumn);

					}
					preVal1 = null;
				}

				if (preVal2 != null) {
					while (val1.equals(preVal2)) {
						temp = new Tuple();
						temp.getData().addAll(leftCurrent.getData());
						temp.getData().addAll(prevRight.getData());
						joinTable.add(temp);
						if (joinTable.size() >= maxSize) {
							if (joinTableIdx == 0)
								joinTableIdx = Helper.joinTableIdx++;
							String fileName = Helper.swapParam + File.separator
									+ "joinTable" + joinTableIdx
									+ Helper.fileFormat;
							ExternalSorter.writeToDisk(joinTable, fileName);
							joinTable.clear();
						}
						leftCurrent = leftChild.getNext();
						if (leftCurrent == null) {
							if (joinTableIdx > 0 && joinTable.size() > 0) {
								String fileName = Helper.swapParam
										+ File.separator + "joinTable"
										+ joinTableIdx + Helper.fileFormat;
								ExternalSorter.writeToDisk(joinTable, fileName);
								joinTable.clear();
							}
							return;
						}
						val1 = this.eval(leftJoinColumn);

					}
					preVal2 = null;
				}

				if (val1 instanceof LongValue) {
					Long l1 = ((LongValue) val1).getValue();
					Long l2 = ((LongValue) val2).getValue();
					retVal = l1.compareTo(l2);
				} else {
					retVal = val1.toString().compareTo(val2.toString());
				}

				if (retVal < 0) {
					leftCurrent = leftChild.getNext();
				} else if (retVal > 0) {
					rightCurrent = rightChild.getNext();
				} else {
					temp = new Tuple();
					temp.getData().addAll(leftCurrent.getData());
					temp.getData().addAll(rightCurrent.getData());
					joinTable.add(temp);
					if (joinTable.size() >= maxSize) {
						if (joinTableIdx == 0)
							joinTableIdx = Helper.joinTableIdx++;
						String fileName = Helper.swapParam + File.separator
								+ "joinTable" + joinTableIdx
								+ Helper.fileFormat;
						ExternalSorter.writeToDisk(joinTable, fileName);
						joinTable.clear();
					}
					prevLeft = leftCurrent;
					prevRight = rightCurrent;
					leftCurrent = leftChild.getNext();
					rightCurrent = rightChild.getNext();
					preVal1 = val1;
					preVal2 = val2;
				}
			}
			if (joinTableIdx > 0 && joinTable.size() > 0) {
				String fileName = Helper.swapParam + File.separator
						+ "joinTable" + joinTableIdx + Helper.fileFormat;
				ExternalSorter.writeToDisk(joinTable, fileName);
				joinTable.clear();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (OutOfMemoryError e) {
			System.out.println("time taken so far: "
					+ (time - System.currentTimeMillis()));
		}
	}

	@Override
	public Tuple getNext() {
		Tuple tuple;
		if (joinTableIdx > 0) {
			if (relation == null) {
				String fileName = "joinTable" + joinTableIdx;
				relation = new RelationOperator(fileName);
				relation.setSchema(schema);
				relation.openSwap();
			}
			tuple = relation.getNext();
			if (tuple != null)
				return tuple;
			else {
				relation.close();
				return null;
				// joinTableIdx = 0;
			}
		}
		if (index < joinTable.size()) {
			tuple = joinTable.get(index);
			index++;
			return tuple;
		} else {
			joinTable.clear();
		}
		return null;
	}

	private void doExternalSort(Column leftJoinColumn) {
		if (joinTableIdx == 0) {
			int colIdx = 0;
			for (edu.buffalo.cse562.table.Column column : schema.getColumns()) {
				if (column
						.getWholeColumnName()
						.toLowerCase()
						.equals(leftJoinColumn.getWholeColumnName()
								.toLowerCase()))
					break;
				colIdx++;
			}
			final int sortBy = colIdx;
			Collections.sort(joinTable, new Comparator<Tuple>() {
				@Override
				public int compare(Tuple a, Tuple b) {
					LeafValue val1 = a.getData().get(sortBy);
					LeafValue val2 = b.getData().get(sortBy);

					int retVal;

					// if (val1 instanceof DateValue) {
					// Date d1 = ((DateValue) val1).getValue();
					// Date d2 = ((DateValue) val2).getValue();
					// retVal = d1.compareTo(d2);
					// } else if (val1 instanceof DoubleValue) {
					// Double d1 = ((DoubleValue) val1).getValue();
					// Double d2 = ((DoubleValue) val2).getValue();
					// retVal = d1.compareTo(d2);
					// } else
					if (val1 instanceof LongValue) {
						Long l1 = ((LongValue) val1).getValue();
						Long l2 = ((LongValue) val2).getValue();
						retVal = l1.compareTo(l2);
					} else {
						retVal = val1.toString().compareTo(val2.toString());
					}
					return retVal;
				}
			});
		} else {
			String fileName = "joinTable" + joinTableIdx;
			RelationOperator relation1 = new RelationOperator(fileName);
			relation1.setSchema(schema);
			ExternalSorter sorter = new ExternalSorter(relation1,
					leftJoinColumn.getColumnName(), 2);
			File f1 = new File(Helper.swapParam + File.separator
					+ sorter.sortAndMerge() + Helper.fileFormat);
			File f2 = new File(Helper.swapParam + File.separator + fileName
					+ Helper.fileFormat);
			f2.delete();
			f1.renameTo(f2);
		}
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
	}

	@Override
	public LeafValue eval(Column arg0) throws SQLException {

		String arg_n = arg0.getWholeColumnName();
		String table = arg0.getTable().getWholeTableName().toLowerCase();
		int colIdx = -1;
		Integer id = columnMap.get(arg_n);
		if (id != null) {
			colIdx = id;
		} else {
			List<edu.buffalo.cse562.table.Column> columns;
			Table t = arg0.getTable();

			Expression exp = null;

			if (current == null) {
				if ((rightChild.getSchema().getTableName() != null && table
						.equals(rightChild.getSchema().getTableName()
								.toLowerCase()))
						|| (rightChild.getSchema().getAlias() != null && table
								.equals(rightChild.getSchema().getAlias()
										.toLowerCase()))) {
					columns = rightChild.getSchema().getColumns();
				} else {
					columns = leftChild.getSchema().getColumns();
				}
			} else {
				columns = this.schema.getColumns();
			}

			String arg_name = arg_n.toLowerCase();

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
				if (arg_name.equals(columns.get(i).getWholeColumnName()
						.toLowerCase())) {
					colIdx = i;
					break;
				}
			}
			columnMap.put(arg_n, colIdx);
		}

		if (colIdx == -1)
			return null;
		if (current != null)
			return current.getData().get(colIdx);
		if ((rightChild.getSchema().getTableName() != null && table
				.equals(rightChild.getSchema().getTableName().toLowerCase()))
				|| (rightChild.getSchema().getAlias() != null && table
						.equals(rightChild.getSchema().getAlias().toLowerCase())))
			return rightCurrent.getData().get(colIdx);
		else
			return leftCurrent.getData().get(colIdx);
	}

	@Override
	public Schema getSchema() {
		return this.schema;
	}

	@Override
	public boolean hasChild() {
		return true;
	}

	@Override
	public Operator getChild() {
		return leftChild;
	}

	@Override
	public void setChild(Operator child) {
		leftChild = child;
	}

	/**
	 * @return the leftChild
	 */
	public Operator getLeftChild() {
		return leftChild;
	}

	/**
	 * @param leftChild
	 *            the leftChild to set
	 */
	public void setLeftChild(Operator leftChild) {
		this.leftChild = leftChild;
	}

	/**
	 * @return the rightChild
	 */
	public Operator getRightChild() {
		return rightChild;
	}

	/**
	 * @param rightChild
	 *            the rightChild to set
	 */
	public void setRightChild(Operator rightChild) {
		this.rightChild = rightChild;
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

}
