/**
 * 
 */
package edu.buffalo.cse562.relationalAlgebra;

import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.table.Schema;
import edu.buffalo.cse562.table.Tuple;

/**
 * @author tanvi
 * 
 */
public abstract class Operator extends Eval {
	Schema schema;
	Operator child;

	public abstract void open();

	public abstract Tuple getNext();

	public abstract void close();

	public Schema getSchema() {
		return this.schema;
	}

	public boolean hasChild() {
		return !(this.child == null);
	}

	public Operator getChild() {
		return this.child;
	}

	public void setChild(Operator child) {
		this.child = child;
	}

	public abstract void createSchema();
}
