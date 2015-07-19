/**
 * 
 */
package edu.buffalo.cse562.relationalAlgebra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.Helper;
import edu.buffalo.cse562.Helper.ColumnType;
import edu.buffalo.cse562.table.Column;
import edu.buffalo.cse562.table.Schema;
import edu.buffalo.cse562.table.Tuple;

/**
 * @author tanvi
 * 
 */
public class RelationOperator extends Operator {

	private String fileName;
	private String swapFileName;
	private BufferedReader reader;
	private List<Integer> indexes;

	public RelationOperator(String fileName) {
		this.fileName = fileName;
		this.schema = new Schema();

		if (Helper.schemaMap.get(fileName.toLowerCase()) != null) {
			List<Column> col1, col2 = Helper.schemaMap.get(
					fileName.toLowerCase()).getColumns();
			col1 = schema.getColumns();

			Column column;
			for (Column col : col2) {
				column = new Column();
				column.setName(col.getName());
				column.setTable(col.getTable());
				Helper.setColumnType(column, col.getType());
				// column.setType(col.getType());
				col1.add(column);
			}
			schema.setTableName(fileName);
		} else
			setSwapFileName(fileName);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.buffalo.cse562.relationalAlgebra.Operator#open()
	 */
	@Override
	public void open() {
		File relation = new File(Helper.dataPath + File.separator
				+ this.fileName + Helper.fileFormat);
		if (!relation.exists())
			relation = new File(Helper.dataPath + File.separator
					+ this.fileName + Helper.fileFormat1);

		try {
			this.reader = new BufferedReader(new FileReader(relation), 100000);
		} catch (FileNotFoundException e) {
			openSwap();
			// e.printStackTrace();
		}

		List<Column> columnList = schema.getColumns();
		indexes = new ArrayList<>();
		for (int i = 0; i < columnList.size(); i++) {
			Column col = columnList.get(i);
			if (Helper.columnList.contains(col.getName())) {
				indexes.add(i);
			}
		}
	}

	public void openSwap() {
		File relation = new File(Helper.swapParam + File.separator
				+ this.swapFileName + Helper.fileFormat);
		if (!relation.exists())
			relation = new File(Helper.dataPath + File.separator
					+ this.fileName + Helper.fileFormat1);

		try {
			this.reader = new BufferedReader(new FileReader(relation), 20000);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.buffalo.cse562.relationalAlgebra.Operator#getNext()
	 */
	@Override
	public Tuple getNext() {
		String line;
		try {
			if ((line = reader.readLine()) != null) {
				// List<String> values = new ArrayList<String>();
				String str = null;
				int cnt = 0;
				int idxOfNextWord = 0;
				// for (int j = 0; j < line.length(); j++) {
				// if (line.charAt(j) == '|' || j == line.length() - 1) {
				// values.add(line.substring(idxOfNextWord, j));
				// idxOfNextWord = j + 1;
				// }
				// }
				// LeafValue[] data = new LeafValue[values.length];
				List<Column> columns = schema.getColumns();

				List<LeafValue> data = new ArrayList<LeafValue>(columns.size());

				String type = null;
				int i = 0;
				for (; i < line.length(); i++) {

					if (line.charAt(i) == '|') {
						str = line.substring(idxOfNextWord, i);
						idxOfNextWord = i + 1;

						if (!indexes.contains(cnt)) {
							data.add(null);
						} else {
							type = columns.get(cnt).getType();

							if (type.equals(ColumnType.STRING.toString())) {
								data.add(new StringValue("\'" + str + "\'"));
								// data[i] = new StringValue("\'" + values[i] +
								// "\'");
							} else if (type.equals(ColumnType.INT.toString())) {
								data.add(new LongValue(str));
								// data[i] = new LongValue(values[i]);
							} else if (type.equals(ColumnType.DECIMAL
									.toString())) {
								data.add(new DoubleValue(str));
								// data[i] = new DoubleValue(values[i]);
							} else if (type.equals(ColumnType.DATE.toString())) {
								data.add(new DateValue("\'" + str + "\'"));
								// data[i] = new DateValue("\'" + values[i] +
								// "\'");
							}
						}
						cnt++;
					}
					// }
				}
				str = line.substring(idxOfNextWord, i);
				if (!str.trim().isEmpty()) {
					type = columns.get(cnt++).getType();

					if (type.equals(ColumnType.STRING.toString())) {
						data.add(new StringValue("\'" + str + "\'"));
						// data[i] = new StringValue("\'" + values[i] +
						// "\'");
					} else if (type.equals(ColumnType.INT.toString())) {
						data.add(new LongValue(str));
						// data[i] = new LongValue(values[i]);
					} else if (type.equals(ColumnType.DECIMAL.toString())) {
						data.add(new DoubleValue(str));
						// data[i] = new DoubleValue(values[i]);
					} else if (type.equals(ColumnType.DATE.toString())) {
						data.add(new DateValue("\'" + str + "\'"));
						// data[i] = new DateValue("\'" + values[i] + "\'");
					}
				}

				Tuple tuple = new Tuple(data);
				return tuple;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		try {
			reader.close();
			System.gc();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public LeafValue eval(net.sf.jsqlparser.schema.Column arg0)
			throws SQLException {
		return null;
	}

	@Override
	public String toString() {
		return fileName;
	}

	@Override
	public void createSchema() {
		// this.schema = child.getSchema();
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	/**
	 * @return the swapFileName
	 */
	public String getSwapFileName() {
		return swapFileName;
	}

	/**
	 * @param swapFileName
	 *            the swapFileName to set
	 */
	public void setSwapFileName(String swapFileName) {
		this.swapFileName = swapFileName;
	}

	public void setFileName(String finalName) {
		this.fileName = finalName;
	}

	public String getFileName() {
		return this.fileName;
	}
}
