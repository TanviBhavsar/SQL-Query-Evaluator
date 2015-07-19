/**
 * 
 */
package edu.buffalo.cse562.optimizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Helper;
import edu.buffalo.cse562.relationalAlgebra.JoinOperator;
import edu.buffalo.cse562.relationalAlgebra.Operator;
import edu.buffalo.cse562.relationalAlgebra.RelationOperator;
import edu.buffalo.cse562.relationalAlgebra.SelectOperator;

/**
 * @author ketkiram
 * 
 */
public class Optimizer {
	public static Operator optimize(Operator root) {
		Operator current, parent = root;

		current = parent.getChild();
		while (current != null && current.hasChild()) {
			if (current instanceof SelectOperator) {
				SelectOperator selection = (SelectOperator) current;
				Operator temp = optimizeSelect(selection);
				if (temp != null) {
					parent.setChild(temp);
					current = parent.getChild();
				}
			}
			parent = current;
			current = parent.getChild();
		}
		return root;
	}

	private static Operator optimizeSelect(SelectOperator selection) {

		if (selection.getChild() instanceof JoinOperator) {
			List<Expression> expressionList = null;
			Expression temp, joinCondition = null, condition;
			JoinOperator join = (JoinOperator) selection.getChild();
			condition = selection.getCondition();
			RelationOperator rightChild = (RelationOperator) join
					.getRightChild();
			RelationOperator leftChild = ((join.getLeftChild() instanceof RelationOperator) ? (RelationOperator) join
					.getLeftChild() : null);
			if (condition instanceof BinaryExpression) {
				temp = condition;
				BinaryExpression exp;
				expressionList = new ArrayList<Expression>();

				while ((exp = (BinaryExpression) temp).getLeftExpression() instanceof BinaryExpression) {
					temp = exp.getRightExpression();
					expressionList.add(temp);
					temp = exp.getLeftExpression();
				}
				expressionList.add(temp);
				
				Iterator<Expression> expIterator = expressionList.iterator();
				Expression e;
				while (expIterator.hasNext()) {
					e = expIterator.next();
					if (e instanceof EqualsTo
							&& ((EqualsTo) e).getRightExpression() instanceof Column) {
						Column col = (Column) ((EqualsTo) e)
								.getLeftExpression();
						String leftTable = col.getTable().getName();
						col = (Column) ((EqualsTo) e).getRightExpression();
						String rightTable = col.getTable().getName();
						if (leftTable.equalsIgnoreCase(rightChild.toString())
								|| rightTable.equalsIgnoreCase(rightChild
										.toString())) {
							if (joinCondition == null)
								joinCondition = e;
							else {
								AndExpression and = new AndExpression();
								and.setLeftExpression(e);
								and.setRightExpression(joinCondition);
								joinCondition = and;
							}
							expIterator.remove();
						
						}
					} else if (e instanceof BinaryExpression) {
						exp = (BinaryExpression) e;
						if (exp.getLeftExpression() instanceof Column) {
							String leftTable = ((Column) exp
									.getLeftExpression()).getTable().getName();

							if (leftTable.equalsIgnoreCase(rightChild
									.toString())) {
								Operator right = join.getRightChild();
								if (right instanceof SelectOperator) {
									AndExpression and = new AndExpression();
									and.setLeftExpression(e);
									and.setRightExpression(((SelectOperator) right)
											.getCondition());
									((SelectOperator) right).setCondition(and);
								} else
									join.setRightChild(new SelectOperator(e,
											rightChild));
								expIterator.remove();
							
							}
						}
					} else if (e instanceof Parenthesis) {
						Expression tempExp = ((Parenthesis) e).getExpression();
						exp = (BinaryExpression) tempExp;
						exp = (BinaryExpression) exp.getRightExpression();
						if (exp.getLeftExpression() instanceof Column) {
							String leftTable = ((Column) exp
									.getLeftExpression()).getTable().getName();
							if (leftTable.equalsIgnoreCase(rightChild
									.toString())) {
								Operator right = join.getRightChild();
								if (right instanceof SelectOperator) {
									AndExpression and = new AndExpression();
									and.setRightExpression(e);
									and.setLeftExpression(((SelectOperator) right)
											.getCondition());
									((SelectOperator) right).setCondition(and);
								} else
									join.setRightChild(new SelectOperator(e,
											rightChild));
								expIterator.remove();
								
							}
						}
					}
				}
				if (expressionList.size() == 1)
					temp = expressionList.get(0);
				else if (expressionList.size() >= 2) {
					AndExpression andExp = new AndExpression();
					andExp.setLeftExpression(expressionList.get(0));
					AndExpression tempAnd;
					for (int i = 1; i < expressionList.size(); i++) {
						if (andExp.getRightExpression() != null) {
							tempAnd = new AndExpression();
							tempAnd.setLeftExpression(andExp);
							tempAnd.setRightExpression(expressionList.get(i));
							andExp = tempAnd;
						} else
							andExp.setRightExpression(expressionList.get(i));

					}
					temp = andExp;
				} else
					temp = null;

				if (temp != null) {
					Operator left = join.getLeftChild();
					join.setLeftChild(new SelectOperator(temp, left));
				}
			}
			join.setCondition(joinCondition);
			if (Helper.swapParam != null) {
				BinaryExpression exp;
				exp = (BinaryExpression) joinCondition;
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

				ExternalSorter sorter = new ExternalSorter(rightChild,
						rightJoinColumn.getColumnName(), 2);
				sorter.sortAndMerge();
				if (leftChild != null) {
					sorter = new ExternalSorter(leftChild,
							leftJoinColumn.getColumnName(), 2);
					sorter.sortAndMerge();
				}

			}
			return join;
		}
		return null;
	}

}
