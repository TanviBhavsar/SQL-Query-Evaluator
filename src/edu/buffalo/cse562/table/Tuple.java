/**
 * 
 */
package edu.buffalo.cse562.table;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.Helper;

/**
 * @author ketkiram
 * 
 */
public class Tuple {
	private List<LeafValue> datum;

	public Tuple(LeafValue[] values) {
		this();
		for (LeafValue value : values) {
			datum.add(value);
		}
	}

	public Tuple(List<LeafValue> values) {
		this();
		datum = values;
	}

	public Tuple() {
		super();
		datum = new ArrayList<LeafValue>();
	}

	public List<LeafValue> getData() {
		return this.datum;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder retVal = new StringBuilder();
		if (datum == null || datum.isEmpty())
			return null;
		for (LeafValue val : datum) {
			if (val instanceof StringValue)
				retVal.append(((StringValue) val).getNotExcapedValue());
			else
				retVal.append(val);
			retVal.append(Helper.splitOnValue);
		}
		if (retVal.length() > 0)
			retVal.deleteCharAt(retVal.length() - 1);
		return retVal.toString();
	}

}
