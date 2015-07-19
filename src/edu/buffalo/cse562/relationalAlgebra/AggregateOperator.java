/**
 * 
 */
package edu.buffalo.cse562.relationalAlgebra;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import edu.buffalo.cse562.Helper;
import edu.buffalo.cse562.Helper.ColumnType;
import edu.buffalo.cse562.table.Schema;
import edu.buffalo.cse562.table.Tuple;

/**
 * @author tanvi
 * 
 */
public class AggregateOperator extends Operator {
	private List<Function> function;
	private Tuple current;
	private List<edu.buffalo.cse562.table.Column> newColumnList;
	private HashMap<String, List<LeafValue>> FinalResult;
	private Schema result;
	private List<Column> groupColumnList;
	private static int mapCtr;
	private static int countFlag;
	HashMap<String, Integer> sumExpMap;
	private List<Tuple> TupleResultList;
	private Map<String, Integer> columnMap;

	public List<Column> getGrouplist() {
		return groupColumnList;
	}

	public AggregateOperator(List<Function> FunctionList, Operator operator,
			List<Column> groupColumnList) {
		this.function = FunctionList;
		child = operator;
		this.groupColumnList = groupColumnList;
		mapCtr = 0;
		countFlag = -1;
		columnMap = new HashMap<String, Integer>();
		createSchema();
	}

	@Override
	public void createSchema() {
		schema = child.getSchema();
		result = new Schema();
		if (groupColumnList != null && !groupColumnList.isEmpty())
			newColumnList = new ArrayList(groupColumnList.size());
		String tableName = child.getSchema().getAlias();
		if (tableName == null)
			tableName = child.getSchema().getTableName();
		List<edu.buffalo.cse562.table.Column> colList = child.getSchema()
				.getColumns();
		edu.buffalo.cse562.table.Column column;
		if (groupColumnList != null && !groupColumnList.isEmpty()) {
			for (Column groupcolumn : groupColumnList) {
				column = new edu.buffalo.cse562.table.Column();
				// save schema of new tuple according to group by in member
				// variable
				column.setName(groupcolumn.getWholeColumnName());
				// column.setType(schema.getColumnDataTypeColumnName(groupcolumn
				// .getColumnName()));
				Helper.setColumnType(column, schema
						.getColumnDataTypeColumnName(groupcolumn
								.getColumnName()));

				newColumnList.add(column);
				result.getColumns().add(column);
			}
		}
		for (Function fun : function) {
			column = new edu.buffalo.cse562.table.Column();
			String param = "all";
			List<Expression> tempExp = null;
			if (fun.getParameters() != null)
				tempExp = fun.getParameters().getExpressions();
			if (tempExp != null && !tempExp.isEmpty())
				param = tempExp.get(0).toString();
			column.setName(fun.getName() + param);
			column.setType(ColumnType.DECIMAL.toString());
			// column.setType("decimal");
			result.getColumns().add(column);
			fun.setName(fun.getName().toLowerCase());
		}
		// System.out.println("$$$$$$$$$$$$$$$$$$");
		// for (edu.buffalo.cse562.table.Column col : result.getColumns())
		// System.out.println(col.getWholeColumnName());
	}

	@Override
	public void open() {
		mapCtr = 0;
		child.open();
	}

	@Override
	public Tuple getNext() {
		Tuple result = null;
		// if no group by call
		if (groupColumnList == null) {
			if (mapCtr == 0) {
				mapCtr = 1;
				return processAgg();
			} else
				return null;
		} else {
			// Object avgCtr;
			// int countFlag ;
			if (FinalResult == null) {
				countFlag = processGroupby();
				TupleResultList = new ArrayList<Tuple>();
				processResult();
			}
			if (mapCtr < TupleResultList.size()) {
				result = sendResult();
				mapCtr++;
			}
		}
		return result;
	}

	// To return result one tuple at a time after aggregate operation is
	// performed after group by operation is performed
	public void processResult() {
		Tuple result = null;
		List<LeafValue> mapValueList = new ArrayList<LeafValue>();
		// LeafValue [] lv=null;
		int sizeResult = function.size() + newColumnList.size();
		LeafValue[] lv;
		// int i = 0;
		int ctr = 0;
		// FinalResult.g
		// Set<String> keys = FinalResult.keySet();
		// To do make list of tuples send that one at a time
		// for (Iterator<String> it = FinalResult.keySet().iterator(); it
		// .hasNext();) {
		for (Entry<String, List<LeafValue>> entry : FinalResult.entrySet()) {
			// i = 0;
			// lv = new LeafValue[sizeResult];
			List<LeafValue> data = new ArrayList<LeafValue>(sizeResult);

			// String key = it.next();
			String key = entry.getKey();
			// mapCtr is index of tuple returned
			// if (ctr == mapCtr) {
			mapValueList = entry.getValue();
			// FinalResult.get(key);
			int keyindex = 0;
			int idxOfNextWord = 0;
			int i = 0;
			String retval = null;
			String ColumnType = null;
			for (; i < key.length(); i++) {

				if (key.charAt(i) == '|') {
					retval = key.substring(idxOfNextWord, i);
					idxOfNextWord = i + 1;
					if (retval.isEmpty())
						break;
					ColumnType = newColumnList.get(keyindex).getType();
					// lv[i]=returnNew(m,keyindex);
					// to do check for other types
					if (ColumnType == null) {
						data.add(new LongValue(retval));
						// lv[i] = new LongValue(retval);
						// i++;
					} else if (ColumnType.equals(Helper.ColumnType.INT
							.toString())) {
						data.add(new LongValue(retval));
						// lv[i] = new LongValue(retval);
						// i++;
					} else if (ColumnType.equals(Helper.ColumnType.DECIMAL
							.toString())) {
						data.add(new DoubleValue(retval));
						// lv[i] = new DoubleValue(retval);
						// i++;
					} else if (ColumnType.equals(Helper.ColumnType.STRING
							.toString())) {
						/*
						 * StringBuilder tempStr = new StringBuilder();
						 * tempStr.append("\'"); tempStr.append(retval);
						 * tempStr.append("\'");
						 */
						// System.out.println("insertimg value"+tempStr.toString()+"at index"+i);
						// data.add(new StringValue(tempStr.toString()));
						data.add(new StringValue("\'" + retval + "\'"));
						// lv[i] = new StringValue(tempStr.toString());
						// // new StringValue()
						// i++;
					} else if (ColumnType.equals(Helper.ColumnType.DATE
							.toString())) {
						/*
						 * StringBuilder tempStr = new StringBuilder();
						 * tempStr.append("\'"); tempStr.append(retval);
						 * tempStr.append("\'");
						 */
						data.add(new DateValue("\'" + retval + "\'"));
						// lv[i] = new DateValue(tempStr.toString());
						// i++;
					}
					keyindex++;
				}

			}
			retval = key.substring(idxOfNextWord, i);
			if (!retval.trim().isEmpty()) {

				ColumnType = newColumnList.get(keyindex).getType();
				// lv[i]=returnNew(m,keyindex);
				// to do check for other types
				if (ColumnType == null) {
					data.add(new LongValue(retval));
					// lv[i] = new LongValue(retval);
					// i++;
				} else if (ColumnType.equals(Helper.ColumnType.INT.toString())) {
					data.add(new LongValue(retval));
					// lv[i] = new LongValue(retval);
					// i++;
				} else if (ColumnType.equals(Helper.ColumnType.DECIMAL
						.toString())) {
					data.add(new DoubleValue(retval));
					// lv[i] = new DoubleValue(retval);
					// i++;
				} else if (ColumnType.equals(Helper.ColumnType.STRING
						.toString())) {
					/*
					 * StringBuilder tempStr = new StringBuilder();
					 * tempStr.append("\'"); tempStr.append(retval);
					 * tempStr.append("\'");
					 */
					// System.out.println("insertimg value"+tempStr.toString()+"at index"+i);
					// data.add(new StringValue(tempStr.toString()));
					data.add(new StringValue("\'" + retval + "\'"));
					// lv[i] = new StringValue(tempStr.toString());
					// // new StringValue()
					// i++;
				} else if (ColumnType.equals(Helper.ColumnType.DATE.toString())) {
					/*
					 * StringBuilder tempStr = new StringBuilder();
					 * tempStr.append("\'"); tempStr.append(retval);
					 * tempStr.append("\'");
					 */
					data.add(new DateValue("\'" + retval + "\'"));
					// lv[i] = new DateValue(tempStr.toString());
					// i++;
				}
				keyindex++;
			}
			// String[] splitval = key.split(Helper.delimitor);
			// // String retval=new String();
			// for (String retval : splitval) {
			// // lv[i]=new LeafValue();
			// if (retval.isEmpty())
			// break;
			// String ColumnType = newColumnList.get(keyindex).getType();
			// // lv[i]=returnNew(m,keyindex);
			// // to do check for other types
			// if (ColumnType == null) {
			// data.add(new LongValue(retval));
			// // lv[i] = new LongValue(retval);
			// // i++;
			// } else if (ColumnType.equalsIgnoreCase("int")) {
			// data.add(new LongValue(retval));
			// // lv[i] = new LongValue(retval);
			// // i++;
			// } else if (ColumnType.equalsIgnoreCase("decimal")) {
			// data.add(new DoubleValue(retval));
			// // lv[i] = new DoubleValue(retval);
			// // i++;
			// } else if (ColumnType.equalsIgnoreCase("char")
			// || ColumnType.equalsIgnoreCase("varchar")) {
			// StringBuilder tempStr = new StringBuilder();
			// tempStr.append("\'");
			// tempStr.append(retval);
			// tempStr.append("\'");
			// //
			// System.out.println("insertimg value"+tempStr.toString()+"at index"+i);
			// data.add(new StringValue(tempStr.toString()));
			// // lv[i] = new StringValue(tempStr.toString());
			// // // new StringValue()
			// // i++;
			// } else if (ColumnType.equalsIgnoreCase("date")) {
			// StringBuilder tempStr = new StringBuilder();
			// tempStr.append("\'");
			// tempStr.append(retval);
			// tempStr.append("\'");
			// data.add(new DateValue(tempStr.toString()));
			// // lv[i] = new DateValue(tempStr.toString());
			// // i++;
			// }
			// keyindex++;
			// }

			// Count result is stored at 0th index initially. Move it to
			// position specified in select

			int valueCtr = 0;

			// Put result in tuple
			for (LeafValue m : mapValueList) {
				// if (countFlag >= 0) {
				// if ((i == countPosition) && (i < (sizeResult - 1)))
				// i++;
				// }
				if ((m != null) && (valueCtr != 0)) {
					if (m instanceof LongValue) {
						LongValue l = (LongValue) m;
						if (l.getValue() != -1) {
							data.add(new LongValue(l.getValue()));
							// lv[i] = new LongValue(l.getValue());
							// i++;
						}
					} else if (m instanceof DoubleValue) {
						DoubleValue l = (DoubleValue) m;
						if (l.getValue() != -1) {
							double dTemp = l.getValue();

							if (dTemp != 0)
								data.add(new DoubleValue(dTemp));
							// lv[i] = new DoubleValue(dTemp);
							// i++;
						}
					}
				}
				valueCtr++;
			}

			int countPosition = countFlag + groupColumnList.size() - 1;
			if (countFlag >= 0)// Position count
			{
				LongValue lcnt = (LongValue) mapValueList.get(0);
				data.add(countPosition, new LongValue(lcnt.getValue()));
				// lv[countPosition] = new LongValue(lcnt.getValue());
			}
			result = new Tuple(data);
			TupleResultList.add(result);
			// break;
			// }
			// ctr++;
		}

		// mapCtr++;
		// return result;
	}

	public Tuple sendResult() {
		return TupleResultList.get(mapCtr);
	}

	// Find aggregate if group by is specified
	// long time1 = System.currentTimeMillis();

	public int processGroupby() {
		// long t1=System.currentTimeMillis();
		// Tuple result = null;
		long avgCtr = 0;
		int avgFlag = 0, functionindex = 1;
		// new HashMap
		FinalResult = new HashMap<String, List<LeafValue>>();
		// FinalResult = new HashMap(20000);
		sumExpMap = new HashMap<String, Integer>();
		List<LeafValue> mapValueList = new ArrayList<LeafValue>();
		Expression sumexp = null, avgexp = null;
		// List<Expression> avgExpList = new ArrayList<Expression>();
		// HashMap <Expression,Integer> sumExpMap =new ashMap <Expression,
		// Integer> ();
		List<Integer> indexList = new ArrayList<Integer>();
		List<edu.buffalo.cse562.table.Column> schemaColumn = schema
				.getColumns();
		for (edu.buffalo.cse562.table.Column columnElement : newColumnList) {

			int nIndex = 0;

			for (edu.buffalo.cse562.table.Column schemaColumnElement : schemaColumn) {

				String str1 = schemaColumnElement.getName().toLowerCase();
				String str3 = schemaColumnElement.getWholeColumnName()
						.toLowerCase();

				String str2 = columnElement.getWholeColumnName().toLowerCase();
				if (str1.equals(str2) || str3.equals(str2)) {
					indexList.add(nIndex);
					break;
				}
				nIndex++;
			}

		}

		try {
			StringBuilder strBuildKey = new StringBuilder();
			while ((current = child.getNext()) != null) {
				// System.out.println("Tuple got in process group by +"+current.getData().toString());
				// List<LeafValue> mapKey = new ArrayList<LeafValue>();

				strBuildKey.setLength(0);
				functionindex = 1;
				// To get index of column in group by column list
				// long k1 = System.currentTimeMillis();
				for (int nIndex : indexList) {

					LeafValue mv = current.getData().get(nIndex);
					LongValue lv = null;
					DoubleValue dv1 = null;
					StringValue sv = null;
					DateValue dtv = null;

					if (mv instanceof LongValue) {
						lv = (LongValue) mv;
						strBuildKey.append(lv.getValue());
					} else if (mv instanceof DateValue) {
						dtv = (DateValue) mv;
						strBuildKey.append(dtv.getValue());
					} else if (mv instanceof DoubleValue) {
						dv1 = (DoubleValue) mv;
						strBuildKey.append(dv1.getValue());
					} else if (mv instanceof StringValue) {
						sv = (StringValue) mv;
						strBuildKey.append(sv.getValue());
					}
					strBuildKey.append(Helper.delimitor);

				}
				String strKey = strBuildKey.toString();

				// System.out.println("Time to build key"+(k2-k2));

				// Calculate count

				// if (FinalResult.containsKey(strKey)) {
				mapValueList = FinalResult.get(strKey);
				if (mapValueList != null) {
					LeafValue leafValue = mapValueList.get(0);// Count
					LongValue countValue = (LongValue) leafValue;
					long longCount = countValue.getValue();
					longCount++;
					countValue.setValue(longCount);
					mapValueList.set(0, countValue);
				}
				// FinalResult.put(strKey, mapValueList);
				// } else {
				else {
					mapValueList = new ArrayList<LeafValue>();
					LongValue lv = new LongValue(1);
					mapValueList.add(0, lv);
					String s = "init";
					LongValue lv1 = new LongValue(-1);
					for (int k = 0; k < function.size(); k++)
						mapValueList.add(k + 1, lv1);
					// FinalResult.put(strKey, mapValueList);
				}
				for (Function f : function) {
					sumexp = null;
					avgexp = null;
					String functionName = f.getName();
					long longSumVal = 0;
					if (functionName.equals("count")) {
						countFlag = functionindex;
					} else if (functionName.equals("sum")) {
						LeafValue value = null;
						if (sumexp == null) {
							List<Expression> expList = f.getParameters()
									.getExpressions();
							sumexp = expList.get(0);

							sumExpMap.put(sumexp.toString(), functionindex);
						}

						value = this.eval(sumexp);

						double doubleSumVal = 0;
						int doubleFlagS = 0;

						if (value instanceof LongValue) {
							LongValue dv = (LongValue) value;
							longSumVal = dv.getValue();
							// doubleflag=1;
						}

						else if (value instanceof DoubleValue) {
							DoubleValue dv = (DoubleValue) value;
							doubleSumVal = dv.getValue();
							doubleFlagS = 1;
						}
						// if (FinalResult.containsKey(strKey)) {
						// mapValueList = FinalResult.get(strKey);
						LongValue countValue1;
						int editsumFlag = 0;
						LeafValue leafValue = mapValueList.get(functionindex);// Count
																				// sum
						if (leafValue instanceof LongValue) {
							countValue1 = (LongValue) leafValue;
							if (countValue1.getValue() == -1)// Average not
							// stored yet
							{
								editsumFlag = 1;
							}
						}
						if (editsumFlag == 0)// add tuple value to sum in map
						{

							if (doubleFlagS == 0) {// index 1
								LongValue countValue = (LongValue) leafValue;
								long longSum = countValue.getValue()
										+ longSumVal;
								// longCount++;
								countValue.setValue(longSum);
								mapValueList.set(functionindex, countValue);
								// FinalResult.put(strKey, mapValueList);
							} else {
								DoubleValue countValue = (DoubleValue) leafValue;
								double longSum = countValue.getValue()
										+ doubleSumVal;
								// longCount++;
								countValue.setValue(longSum);
								mapValueList.set(functionindex, countValue);
								// FinalResult.put(strKey, mapValueList);
							}
						} else { // insert current tuple value as sum
							if (doubleFlagS == 0) {
								LongValue lv = new LongValue(longSumVal);
								mapValueList.add(functionindex, lv);
							} else {
								DoubleValue lv = new DoubleValue(doubleSumVal);
								mapValueList.add(functionindex, lv);
							}
						}
						// FinalResult.put(strKey, mapValueList);
						/*
						 * } else { mapValueList = new ArrayList<LeafValue>(5);
						 * for (int j = 0; j < function.size(); j++)
						 * mapValueList.add(null); if (doubleFlagS == 0) {
						 * LongValue lv = new LongValue(longSumVal);
						 * mapValueList.add(functionindex, lv); } else {
						 * DoubleValue lv = new DoubleValue(doubleSumVal);
						 * mapValueList.add(functionindex, lv); } }
						 */

						// FinalResult.put(strKey, mapValueList);
					} else if (functionName.equals("avg")) {
						avgFlag = 1;
						LeafValue value = null;
						// avgCtr++;
						if (avgexp == null) {
							avgexp = (Expression) f.getParameters()
									.getExpressions().get(0);
							// avgExpList.add(avgexp);
						}

						if (sumExpMap.containsKey(avgexp.toString())) {

							functionindex++;
							continue;
						}

						value = this.eval(avgexp);

						double doubleSumVal = 0;
						Date dateVal = null;
						int doubleFlagS = 0;

						if (value instanceof LongValue) {
							LongValue dv = (LongValue) value;
							longSumVal = dv.getValue();

						}

						else if (value instanceof DoubleValue) {
							DoubleValue dv = (DoubleValue) value;
							doubleSumVal = dv.getValue();
							doubleFlagS = 1;
						} else if (value instanceof DateValue) {
							DateValue dt2 = (DateValue) value;
							dateVal = dt2.getValue();
							// doubleFlagS = 1;
						}
						// if (FinalResult.containsKey(strKey)) {
						// mapValueList = FinalResult.get(strKey);
						// mapValueList.
						LeafValue leafValue = mapValueList.get(functionindex);// Average
						LongValue countValue = null;
						DoubleValue countValueDb = null;
						if (leafValue instanceof LongValue) {
							countValue = (LongValue) leafValue;
						} else if (leafValue instanceof DoubleValue) {
							countValueDb = (DoubleValue) leafValue;
						}
						// to do if leafValue is doubleValue
						long countLong = 0;
						if (countValue != null)
							countLong = countValue.getValue();
						if (countLong == -1)// Average not
						// stored yet
						{
							if (doubleFlagS == 0) {
								LongValue lv = new LongValue(longSumVal);
								mapValueList.set(functionindex, lv);
								// FinalResult.put(strKey, mapValueList);
							} else {
								DoubleValue lv = new DoubleValue(doubleSumVal);
								mapValueList.set(functionindex, lv);
								// FinalResult.put(strKey, mapValueList);
							}
						} else {
							// is at
							if (doubleFlagS == 0) {// index 1
								long longSum = countValue.getValue()
										+ longSumVal;
								// longCount++;
								countValue.setValue(longSum);
								mapValueList.set(functionindex, countValue);
								// FinalResult.put(strKey, mapValueList);
							} else {
								DoubleValue countValueD = (DoubleValue) leafValue;
								double longSum = countValueD.getValue()
										+ doubleSumVal;
								// longCount++;
								countValueD.setValue(longSum);
								mapValueList.set(functionindex, countValueD);
								// FinalResult.put(strKey, mapValueList);
							}
						}
						// FinalResult.put(strKey, mapValueList);
						/*
						 * }else { mapValueList = new ArrayList<LeafValue>(5);
						 * for (int j = 0; j < function.size(); j++)
						 * mapValueList.add(null);
						 * 
						 * if (doubleFlagS == 0) { LongValue lv = new
						 * LongValue(longSumVal);
						 * mapValueList.add(functionindex, lv); } else {
						 * DoubleValue lv = new DoubleValue(doubleSumVal);
						 * mapValueList.add(functionindex, lv); } }
						 */
						// mapValueList.set(0, lv);
						// FinalResult.put(strKey, mapValueList);
					}

					functionindex++;
				}
				FinalResult.put(strKey, mapValueList);
			}

			if (avgFlag == 1)
				processAverage(countFlag);
			else {
				if (countFlag == 0)
					resetCount();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// long t2=System.currentTimeMillis();
		// System.out.println("Time taken by group by"+(t2-t1));
		return countFlag;
	}

	public void processAverage(int countFlag) {
		List<LeafValue> mapValueList = new ArrayList<LeafValue>();
		// for (Iterator<String> it = FinalResult.keySet().iterator(); it
		// .hasNext();)
		for (Entry<String, List<LeafValue>> entry : FinalResult.entrySet())

		{
			String key = entry.getKey();
			// it.next();

			mapValueList = entry.getValue();
			// FinalResult.get(key);
			LongValue lctr = (LongValue) (mapValueList.get(0));

			int funCtr = 1;
			for (Function fElement : function) {

				String fName = fElement.getName();
				if (fName.equals("avg")) {
					List<Expression> expList = fElement.getParameters()
							.getExpressions();
					Expression avgexp = expList.get(0);
					LeafValue temp1;
					if (sumExpMap.containsKey(avgexp.toString()))
						temp1 = mapValueList.get(sumExpMap.get(avgexp
								.toString()));
					else
						temp1 = mapValueList.get(funCtr);
					DoubleValue lsumd = null;
					LongValue lsuml = null;
					double temp;
					if (temp1 instanceof DoubleValue)
						lsumd = (DoubleValue) temp1;
					else
						lsuml = (LongValue) temp1;

					if (lsumd != null)
						temp = (lsumd.getValue() / lctr.getValue());
					else
						temp = (lsuml.getValue() / lctr.getValue());

					DoubleValue finalAvg = new DoubleValue(temp);

					mapValueList.set(funCtr, finalAvg);
					if (countFlag == 0) {
						LongValue newCount = new LongValue(-1);
						mapValueList.set(0, newCount);
					}
					FinalResult.put(key, mapValueList);
				}

				funCtr++;
			}
		}
	}

	// If count is not there in select reset entry where count is stored
	public void resetCount() {
		List<LeafValue> mapValueList = new ArrayList<LeafValue>();
		// for (Iterator<String> it = FinalResult.keySet().iterator(); it
		// .hasNext();)
		for (Entry<String, List<LeafValue>> entry : FinalResult.entrySet()) {
			String key = entry.getKey();
			// it.next();
			mapValueList = entry.getValue();
			// FinalResult.get(key);
			LongValue newCount = new LongValue(-1);
			mapValueList.set(0, newCount);

		}
	}

	// Process aggregate when group by is not given
	public Tuple processAgg() {
		int count = 0;
		Expression sumexp = null, avgexp = null, minexp = null, maxexp = null;
		double sumd = 0, asumd = 0, mind = 0, finalMinD = 0, finalMaxD = 0, maxd = 0;
		long suml = 0, asuml = 0, minl = 0, finalMinL = 0, finalMaxL = 0, maxl = 0;
		int ctr = 0, firstElement = 0;
		int doubleflag = 0, doubleflagAvg = 0, doubleflagMin = 0, doubleflagMax = 0, firstElementMax = 0;
		LeafValue value = null;
		while ((current = child.getNext()) != null) {
			for (Function f : function) {
				String functionName = f.getName();
				if (functionName.equals("count"))
					count++;
				else if (functionName.equals("sum")) {
					if (sumexp == null)
						sumexp = (Expression) f.getParameters()
								.getExpressions().get(0);
					try {
						value = this.eval(sumexp);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (value instanceof DoubleValue) {
						DoubleValue dv = (DoubleValue) value;
						sumd += dv.getValue();
						doubleflag = 1;
					} else if (value instanceof LongValue) {
						LongValue dv = (LongValue) value;
						suml += dv.getValue();

					}
					// test
				} else if (functionName.equals("avg")) {
					// TODO: add avg code here

					doubleflagAvg = 0;
					if (avgexp == null)
						avgexp = (Expression) f.getParameters()
								.getExpressions().get(0);

					try {
						value = this.eval(avgexp);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (value instanceof DoubleValue) {
						DoubleValue dv = (DoubleValue) value;
						asumd += dv.getValue();
						ctr++;
						doubleflag = 1;
					} else if (value instanceof LongValue) {
						LongValue dv = (LongValue) value;
						asuml += dv.getValue();
						ctr++;

					}
				} else if (functionName.equals("min")) {
					doubleflag = 0;
					if (minexp == null)
						minexp = (Expression) f.getParameters()
								.getExpressions().get(0);
					try {
						value = this.eval(minexp);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (value instanceof DoubleValue) {
						DoubleValue dv = (DoubleValue) value;
						mind = dv.getValue();
						if (firstElement == 0) {
							finalMinD = mind;
						}
						if (finalMinD < mind)
							finalMinD = mind;
						doubleflagMin = 1;
					} else if (value instanceof LongValue) {
						LongValue dv = (LongValue) value;
						minl = dv.getValue();
						if (firstElement == 0) {
							finalMinL = minl;
							firstElement = 1;
						} else if (finalMinL > minl)
							finalMinL = minl;

					}
					// test
				} else if (functionName.equals("max")) {

					firstElement = 0;
					if (maxexp == null)
						maxexp = (Expression) f.getParameters()
								.getExpressions().get(0);
					try {
						value = this.eval(maxexp);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (value instanceof DoubleValue) {
						DoubleValue dv = (DoubleValue) value;
						mind = dv.getValue();
						if (firstElement == 0) {
							finalMaxD = maxd;
						}
						if (finalMaxD < mind)
							finalMaxD = mind;
						doubleflagMax = 1;
					}
					if (value instanceof LongValue) {
						LongValue dv = (LongValue) value;
						maxl = dv.getValue();
						if (firstElementMax == 0) {
							finalMaxL = maxl;
							firstElementMax = 1;
						} else if (finalMaxL < maxl)
							finalMaxL = maxl;

					}
					// test
				}
			}
		}
		// Process Result
		LeafValue[] lv = new LeafValue[function.size()];
		int i = 0;
		for (Function f : function) {
			String functionName = f.getName();
			if (functionName.equals("count")) {
				lv[i] = new LongValue(count);
				i++;
			} else if ((functionName.equals("sum"))) {
				if (doubleflag == 1) {
					lv[i] = new DoubleValue(sumd);
					i++;
				} else {
					lv[i] = new LongValue(suml);
					i++;

				}
			} else if ((functionName.equals("avg"))) {
				long rem = asuml % ctr;
				if (rem != 0)
					asumd = asuml;
				if (doubleflagAvg == 1 || rem != 0) {

					LeafValue temp = new DoubleValue(asumd / ctr);
					lv[i] = temp;
				} else {
					lv[i] = new LongValue(asuml);
				}
			} else if ((functionName.equals("min"))) {
				if (doubleflagMin == 1) {

					LeafValue temp = new DoubleValue(finalMinD);
					lv[i] = temp;
				} else {
					lv[i] = new LongValue(finalMinL);
				}
			} else if ((functionName.equals("max"))) {
				if (doubleflagMin == 1) {

					LeafValue temp = new DoubleValue(finalMaxD);
					lv[i] = temp;
				} else {
					lv[i] = new LongValue(finalMaxL);
				}
			}
		}
		Tuple result = new Tuple(lv);
		return result;
	}

	@Override
	public void close() {
		child.close();
	}

	@Override
	public LeafValue eval(Column arg0) throws SQLException {

		List<edu.buffalo.cse562.table.Column> columns = schema.getColumns();
		Table t = arg0.getTable();
		Expression exp = null;
		String arg_n = arg0.getWholeColumnName();
		int colIdx = -1;
		Integer id = columnMap.get(arg_n);
		if (id != null) {
			colIdx = id;
		} else {
			String arg_name = arg_n.toLowerCase();
			String colName;
			for (int i = 0; i < columns.size(); i++) {
				edu.buffalo.cse562.table.Column col = columns.get(i);
				// if
				// (Helper.columnAliasMap.containsKey(arg0.getWholeColumnName())
				// && t != null && t.getName() != null) {
				// exp = Helper.columnAliasMap.get(arg0.getWholeColumnName());
				// if (exp instanceof Column) {
				// col.setAlias(col.getName());
				// col.setName(((Column) exp).getColumnName());
				// col.setTable(((Column) exp).getTable().getName());
				// }
				// }
				if (t == null || t.getName() == null) {
					colName = col.getName().toLowerCase();
				} else {
					colName = col.getWholeColumnName().toLowerCase();
				}
				if (arg_name.equals(colName)) {
					colIdx = i;
					break;
				}
			}
			columnMap.put(arg_n, colIdx);
		}

		return current.getData().get(colIdx);
	}

	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return result;
	}
}
