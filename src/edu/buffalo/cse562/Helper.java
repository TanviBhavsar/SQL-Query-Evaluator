/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import edu.buffalo.cse562.table.Schema;

/**
 * @author ketkiram
 * 
 */
public class Helper {

	public static Map<String, Schema> schemaMap;
	public static String splitOnValue = "|";
	public static final String delimitor = "|";
	public static String fileFormat = ".dat";
	public static String fileFormat1 = ".tbl";
	public static String dataPath;
	public static String swapParam;
	public static String dbParam;
	public static int joinTableIdx = 1;
	public static boolean isLoad = false;

	public static Map<String, FromItem> tableAliasMap;
	public static Map<String, Expression> columnAliasMap;
	public static List<String> columnList;

	static {
		schemaMap = new HashMap<String, Schema>();
		tableAliasMap = new HashMap<String, FromItem>();
		columnAliasMap = new HashMap<String, Expression>();
	}

	public enum ColumnType {
		STRING, INT, DECIMAL, DATE
	}

	public static void setColumnType(edu.buffalo.cse562.table.Column column,
			String columnDataTypeColumnName) {
		// TODO Auto-generated method stub
		switch (columnDataTypeColumnName.toLowerCase()) {
		case "string":
		case "varchar":
		case "char":
			column.setType(ColumnType.STRING.toString());
			break;
		case "int":
			column.setType(ColumnType.INT.toString());
			break;
		case "decimal":
			column.setType(ColumnType.DECIMAL.toString());
			break;
		case "date":
			column.setType(ColumnType.DATE.toString());
			break;
		}
	}

	static {
		columnList = new ArrayList<String>();
		columnList.add("receiptdate");
		columnList.add("shipdate");
		columnList.add("commitdate");
		columnList.add("orderdate");
		columnList.add("shipmode");
		columnList.add("orderkey");
		columnList.add("suppkey");
		columnList.add("custkey");
		columnList.add("nationkey");
		columnList.add("regionkey");
		columnList.add("returnflag");
		columnList.add("linestatus");
		columnList.add("shippriority");
		columnList.add("mktsegment");
		columnList.add("name");
		columnList.add("quantity");
		columnList.add("discount");
		columnList.add("acctbal");
		columnList.add("address");
		columnList.add("phone");
		columnList.add("comment");
		columnList.add("orderpriority");
		columnList.add("extendedprice");
		columnList.add("tax");
	}
}
