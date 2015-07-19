/**
 * 
 */
package edu.buffalo.cse562.relationalAlgebra;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.table.Schema;
import edu.buffalo.cse562.table.Tuple;

/**
 * @author tanvi
 * 
 */
public class UnionOperator extends Operator {

	private Operator left;
	private Operator right;
	private Tuple current;

	public UnionOperator(Operator leftChild, Operator rightChild) {
		left = leftChild;
		right = rightChild;
	}

	@Override
	public void open() {
		left.open();
		right.open();
	}

	@Override
	public Tuple getNext() {
		Tuple t;
		while ((t = left.getNext()) != null)
			return t;
		while ((t = right.getNext()) != null)
			return t;
		return null;
	}

	@Override
	public void close() {
		left.close();
		right.close();
	}

	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		return null;
	}

	@Override
	public Schema getSchema() {
		return left.getSchema();
	}

	@Override
	public boolean hasChild() {
		return true;
	}

	/**
	 * @return the left
	 */
	public Operator getLeft() {
		return left;
	}

	/**
	 * @param left
	 *            the left to set
	 */
	public void setLeft(Operator left) {
		this.left = left;
	}

	/**
	 * @return the right
	 */
	public Operator getRight() {
		return right;
	}

	/**
	 * @param right
	 *            the right to set
	 */
	public void setRight(Operator right) {
		this.right = right;
	}

	@Override
	public void createSchema() {
		// this.schema = child.getSchema();
	}

}
