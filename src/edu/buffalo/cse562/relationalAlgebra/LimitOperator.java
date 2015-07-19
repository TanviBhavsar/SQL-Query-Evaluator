package edu.buffalo.cse562.relationalAlgebra;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.table.Schema;
import edu.buffalo.cse562.table.Tuple;

public class LimitOperator extends Operator {
	private Schema schema;
	private Tuple current;
	long Limit;
	long ctr;

	public LimitOperator(long l, Operator child) {
		this.child = child;
		Limit = l;
		ctr = 0;

		/* this.source = child; */
		this.schema = child.getSchema();
	}

	@Override
	public void open() {
		// TODO Auto-generated method stub
		child.open();
		// this.source.get(1).open();
	}

	@Override
	public Tuple getNext() {
		// TODO Auto-generated method stub
		Tuple t;
		while ((this.current = child.getNext()) != null) {
			ctr++;

			if (ctr > Limit)
				return null;
			else
				return this.current;

		}
		// root.open();

		return null;
	}

	@Override
	public void close() {
		child.close();
	}

	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		return null;
	}

	@Override
	public Schema getSchema() {
		return this.schema;
	}

	@Override
	public void createSchema() {
		this.schema = child.getSchema();
	}
}
