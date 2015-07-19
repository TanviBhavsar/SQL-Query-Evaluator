package edu.buffalo.cse562;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.table.Column;
import edu.buffalo.cse562.table.Schema;
import edu.buffalo.cse562.table.Tuple;


public class ProcessTuple extends Thread {
	private String line;
	private Tuple tuple;
	private Schema schema;
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("In run");
		String[] values = line.split("\\|");
		LeafValue[] data = new LeafValue[values.length];

		//Object schema;
		List<Column> columns = schema.getColumns();
		String type = null;
		for (int i = 0; i < values.length; i++) {
			type = columns.get(i).getType();

			if (type.equalsIgnoreCase("string")
					|| type.equalsIgnoreCase("char")
					|| type.equalsIgnoreCase("varchar")) {
				data[i] = new StringValue("\'" + values[i] + "\'");
			} else if (type.equalsIgnoreCase("int")) {
				data[i] = new LongValue(values[i]);
			} else if (type.equalsIgnoreCase("decimal")) {
				data[i] = new DoubleValue(values[i]);
			} else if (type.equalsIgnoreCase("date")) {
				data[i] = new DateValue("\'" + values[i] + "\'");
			}
		}

		tuple = new Tuple(data);
		System.out.println("Exiting run Tuple is "+tuple.toString());
	}
	public String getLine() {
		return line;
	}
	public void setLine(String line) {
		this.line = line;
	}
	public Tuple getTuple() {
		return tuple;
	}
	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}
	public Schema getSchema() {
		return schema;
	}
	public void setSchema(Schema schema) {
		this.schema = schema;
	}
	

}
