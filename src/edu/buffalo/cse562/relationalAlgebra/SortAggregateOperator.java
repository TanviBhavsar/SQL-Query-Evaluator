/**
 * 
 */
package edu.buffalo.cse562.relationalAlgebra;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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
public class SortAggregateOperator extends Operator {
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
	private static String previousKey;

	public List<Column> getGrouplist() {
		return groupColumnList;
	}

	public SortAggregateOperator(List<Function> FunctionList,
			Operator operator, List<Column> groupColumnList) {
		FinalResult = new HashMap<String, List<LeafValue>>();
		this.function = FunctionList;
		child = operator;
		this.groupColumnList = groupColumnList;
		mapCtr = 0;
		countFlag = -1;
		previousKey = null;
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
//				column.setType(schema.getColumnDataTypeColumnName(groupcolumn
//						.getColumnName()));
				Helper.setColumnType(column, schema.getColumnDataTypeColumnName(groupcolumn
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
//			column.setType("decimal");
			result.getColumns().add(column);
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
			/*
			 * if (FinalResult == null) { countFlag = processGroupby();
			 * TupleResultList=new ArrayList<Tuple>(); processResult(); }
			 * if(mapCtr<TupleResultList.size()) { result=sendResult();
			 * mapCtr++; }
			 */
			TupleResultList = new ArrayList<Tuple>();
			result = processGroupby();

		}
		return result;
	}

	// To return result one tuple at a time after aggregate operation is
	// performed after group by operation is performed
	public void processResult(String keyMap) {

		// System.out.println("in processresult, keymap is"+keyMap);
		Tuple result = null;
		// Tuple result3=null;
		List<LeafValue> mapValueList = new ArrayList<LeafValue>();
		// LeafValue [] lv=null;
		int sizeResult = function.size() + newColumnList.size();
		LeafValue[] lv;
		int i = 0;
		int ctr = 0;
		// FinalResult.g
		// Set<String> keys = FinalResult.keySet();
		// To do make list of tuples send that one at a time
		// for (Iterator<String> it = FinalResult.keySet().iterator(); it
		// .hasNext();) {
		i = 0;
		lv = new LeafValue[sizeResult];
		String key = keyMap;
		// it.next();
		// mapCtr is index of tuple returned
		// if (ctr == mapCtr) {
		mapValueList = FinalResult.get(key);
		int keyindex = 0;
		int idxOfNextWord = 0;
		int j = 0;
		String retval = null;
		String ColumnType = null;
		for (; j < key.length(); j++) {

			if (key.charAt(j) == '|') {
				retval = key.substring(idxOfNextWord, j);
				idxOfNextWord = j + 1;
				if (retval.isEmpty())
					break;
				// lv[i]=new LeafValue();
				ColumnType = newColumnList.get(keyindex).getType();
				// lv[i]=returnNew(m,keyindex);
				// to do check for other types
				if (ColumnType == null) {
					lv[i] = new LongValue(retval);
					i++;
				} else if (ColumnType.equalsIgnoreCase("int")) {
					lv[i] = new LongValue(retval);
					i++;
				} else if (ColumnType.equalsIgnoreCase("decimal")) {
					lv[i] = new DoubleValue(retval);
					i++;
				} else if (ColumnType.equalsIgnoreCase("string")) {
					lv[i] = new StringValue(retval);
					i++;
				} else if (ColumnType.equalsIgnoreCase("char")
						|| ColumnType.equalsIgnoreCase("varchar")) {
					/*
					 * StringBuilder tempStr = new StringBuilder();
					 * tempStr.append("\'"); tempStr.append(retval);
					 * tempStr.append("\'");
					 */
					// System.out.println("insertimg value"+tempStr.toString()+"at index"+i);
					lv[i] = new StringValue("\'" + retval + "\'");
					// new StringValue()
					i++;
				} else if (ColumnType.equalsIgnoreCase("date")) {
					/*
					 * StringBuilder tempStr = new StringBuilder();
					 * tempStr.append("\'"); tempStr.append(retval);
					 * tempStr.append("\'");
					 */
					lv[i] = new DateValue("\'" + retval + "\'");
					i++;
				}
				keyindex++;
			}
		}

		retval = key.substring(idxOfNextWord, j);
		if (!retval.trim().isEmpty()) {

			// lv[i]=new LeafValue();
			ColumnType = newColumnList.get(keyindex).getType();
			// lv[i]=returnNew(m,keyindex);
			// to do check for other types
			if (ColumnType == null) {
				lv[i] = new LongValue(retval);
				i++;
			} else if (ColumnType.equalsIgnoreCase("int")) {
				lv[i] = new LongValue(retval);
				i++;
			} else if (ColumnType.equalsIgnoreCase("decimal")) {
				lv[i] = new DoubleValue(retval);
				i++;
			} else if (ColumnType.equalsIgnoreCase("string")) {
				lv[i] = new StringValue(retval);
				i++;
			} else if (ColumnType.equalsIgnoreCase("char")
					|| ColumnType.equalsIgnoreCase("varchar")) {
				/*
				 * StringBuilder tempStr = new StringBuilder();
				 * tempStr.append("\'"); tempStr.append(retval);
				 * tempStr.append("\'");
				 */
				// System.out.println("insertimg value"+tempStr.toString()+"at index"+i);
				lv[i] = new StringValue("\'" + retval + "\'");
				// new StringValue()
				i++;
			} else if (ColumnType.equalsIgnoreCase("date")) {
				/*
				 * StringBuilder tempStr = new StringBuilder();
				 * tempStr.append("\'"); tempStr.append(retval);
				 * tempStr.append("\'");
				 */
				lv[i] = new DateValue("\'" + retval + "\'");
				i++;
			}
			keyindex++;
		}

		// String[] splitval = key.split(Helper.delimitor);
		// String retval=new String();
		// for (String retval : splitval) {
		// // lv[i]=new LeafValue();
		// if (retval.isEmpty())
		// break;
		// String ColumnType = newColumnList.get(keyindex).getType();
		// // lv[i]=returnNew(m,keyindex);
		// // to do check for other types
		// if (ColumnType == null) {
		// lv[i] = new LongValue(retval);
		// i++;
		// } else if (ColumnType.equalsIgnoreCase("int")) {
		// lv[i] = new LongValue(retval);
		// i++;
		// } else if (ColumnType.equalsIgnoreCase("decimal")) {
		// lv[i] = new DoubleValue(retval);
		// i++;
		// } else if (ColumnType.equalsIgnoreCase("string")) {
		// lv[i] = new StringValue(retval);
		// i++;
		// } else if (ColumnType.equalsIgnoreCase("char")
		// || ColumnType.equalsIgnoreCase("varchar")) {
		// StringBuilder tempStr = new StringBuilder();
		// tempStr.append("\'");
		// tempStr.append(retval);
		// tempStr.append("\'");
		// //
		// System.out.println("insertimg value"+tempStr.toString()+"at index"+i);
		// lv[i] = new StringValue(tempStr.toString());
		// // new StringValue()
		// i++;
		// } else if (ColumnType.equalsIgnoreCase("date")) {
		// StringBuilder tempStr = new StringBuilder();
		// tempStr.append("\'");
		// tempStr.append(retval);
		// tempStr.append("\'");
		// lv[i] = new DateValue(tempStr.toString());
		// i++;
		// }
		// keyindex++;
		// }

		// Count result is stored at 0th index initially. Move it to
		// position specified in select
		int countPosition = countFlag + groupColumnList.size() - 1;
		if (countFlag >= 0)// Position count
		{
			LongValue lcnt = (LongValue) mapValueList.get(0);
			lv[countPosition] = new LongValue(lcnt.getValue());
		}
		int valueCtr = 0;

		// Put result in tuple
		for (LeafValue m : mapValueList) {
			if (countFlag >= 0) {
				if ((i == countPosition) && (i < (sizeResult - 1)))
					i++;
			}
			if ((m != null) && (valueCtr != 0)) {
				if (m instanceof LongValue) {
					LongValue l = (LongValue) m;
					if (l.getValue() != -1) {
						lv[i] = new LongValue(l.getValue());
						i++;
					}
				} else if (m instanceof DoubleValue) {
					DoubleValue l = (DoubleValue) m;
					if (l.getValue() != -1) {
						double dTemp = l.getValue();

						if (dTemp != 0)
							lv[i] = new DoubleValue(dTemp);
						i++;
					}
				}
			}
			valueCtr++;
		}

		result = new Tuple(lv);
		TupleResultList.add(result);
		// break;
		// }
		// ctr++;
		// }

		// mapCtr++;
		// return result;
	}

	public Tuple sendResult() {
		// return TupleResultList.get(mapCtr);
		return TupleResultList.get(0);
	}

	// Find aggregate if group by is specified

	public Tuple processGroupby() {
		// Tuple result = null;
		Tuple result3 = null;
		long avgCtr = 0;
		int avgFlag = 0, functionindex = 1, keyNotPresent = 0;
		// String previousKey=null;
		// FinalResult = new HashMap<String, List<LeafValue>>();
		sumExpMap = new HashMap<String, Integer>();
		List<LeafValue> mapValueList = new ArrayList<LeafValue>();
		Expression sumexp = null, avgexp = null;
		List<Expression> avgExpList = new ArrayList<Expression>();
		// HashMap <Expression,Integer> sumExpMap =new HashMap <Expression,
		// Integer> ();
		List<Integer> indexList = new ArrayList<Integer>();
		for (edu.buffalo.cse562.table.Column columnElement : newColumnList) {

			int nIndex = 0;
			List<edu.buffalo.cse562.table.Column> schemaColumn = schema
					.getColumns();
			for (edu.buffalo.cse562.table.Column schemaColumnElement : schemaColumn) {

				String str1 = schemaColumnElement.getName();
				String str3 = schemaColumnElement.getWholeColumnName();

				String str2 = columnElement.getWholeColumnName();
				if (str1.equalsIgnoreCase(str2) || str3.equalsIgnoreCase(str2)) {
					indexList.add(nIndex);
					break;
				}
				nIndex++;
			}

		}
		try {
			while ((current = child.getNext()) != null) {
				List<LeafValue> mapKey = new ArrayList<LeafValue>();
				StringBuilder strBuildKey = new StringBuilder();
				functionindex = 1;
				keyNotPresent = 0;
				// To get index of column in group by column list
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

				// Calculate count
				keyNotPresent = 0;
				if (FinalResult.containsKey(strKey)) {
					mapValueList = FinalResult.get(strKey);
					LeafValue leafValue = mapValueList.get(0);// Count
					LongValue countValue = (LongValue) leafValue;
					long longCount = countValue.getValue();
					longCount++;
					countValue.setValue(longCount);
					mapValueList.set(0, countValue);
					FinalResult.put(strKey, mapValueList);
					previousKey = strKey;
				} else {

					/*
					 * if (previousKey != null) previousKey=strKey;
					 */
					/*
					 * if (previousKey == null) previousKey = strKey;
					 */
					// if(result!=null)
					// Tuple result=null;
					keyNotPresent = 1;
					/*
					 * if (FinalResult.size() > 0) { processResult(); result3 =
					 * sendResult(); FinalResult.clear(); }
					 */
					// FinalResult.clear();
					mapValueList = new ArrayList<LeafValue>();
					LongValue lv = new LongValue(1);
					mapValueList.add(0, lv);
					// String s = "init";
					LongValue lv1 = new LongValue(-1);
					for (int k = 0; k < function.size(); k++)
						mapValueList.add(k + 1, lv1);
					FinalResult.put(strKey, mapValueList);
					// Tuple result=sendResult();
					if (result3 != null)
						return result3;
				}
				for (Function f : function) {
					sumexp = null;
					avgexp = null;
					String functionName = f.getName();
					long longSumVal = 0;
					if (functionName.equalsIgnoreCase("count")) {
						countFlag = functionindex;
					}
					if (functionName.equalsIgnoreCase("avg")) {
						avgFlag = 1;
						LeafValue value = null;
						// avgCtr++;
						if (avgexp == null) {
							avgexp = (Expression) f.getParameters()
									.getExpressions().get(0);
							avgExpList.add(avgexp);
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
						if (FinalResult.containsKey(strKey)) {
							mapValueList = FinalResult.get(strKey);
							// mapValueList.
							LeafValue leafValue = mapValueList
									.get(functionindex);// Average
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
									FinalResult.put(strKey, mapValueList);
								} else {
									DoubleValue lv = new DoubleValue(
											doubleSumVal);
									mapValueList.set(functionindex, lv);
									FinalResult.put(strKey, mapValueList);
								}
							} else {
								// is at
								if (doubleFlagS == 0) {// index 1
									long longSum = countValue.getValue()
											+ longSumVal;
									// longCount++;
									countValue.setValue(longSum);
									mapValueList.set(functionindex, countValue);
									FinalResult.put(strKey, mapValueList);
								} else {
									DoubleValue countValueD = (DoubleValue) leafValue;
									double longSum = countValueD.getValue()
											+ doubleSumVal;
									// longCount++;
									countValueD.setValue(longSum);
									mapValueList
											.set(functionindex, countValueD);
									FinalResult.put(strKey, mapValueList);
								}
							}
							FinalResult.put(strKey, mapValueList);
						} else {
							mapValueList = new ArrayList<LeafValue>(5);
							for (int j = 0; j < function.size(); j++)
								mapValueList.add(null);

							if (doubleFlagS == 0) {
								LongValue lv = new LongValue(longSumVal);
								mapValueList.add(functionindex, lv);
							} else {
								DoubleValue lv = new DoubleValue(doubleSumVal);
								mapValueList.add(functionindex, lv);
							}
						}
						// mapValueList.set(0, lv);
						FinalResult.put(strKey, mapValueList);
					}
					if (functionName.equalsIgnoreCase("sum")) {
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
						if (FinalResult.containsKey(strKey)) {
							mapValueList = FinalResult.get(strKey);
							LongValue countValue1;
							int editsumFlag = 0;
							LeafValue leafValue = mapValueList
									.get(functionindex);// Count
														// sum
							if (leafValue instanceof LongValue) {
								countValue1 = (LongValue) leafValue;
								if (countValue1.getValue() == -1)// Average not
								// stored yet
								{
									editsumFlag = 1;
								}
							}
							if (editsumFlag == 0)// add tuple value to sum in
													// map
							{

								if (doubleFlagS == 0) {// index 1
									LongValue countValue = (LongValue) leafValue;
									long longSum = countValue.getValue()
											+ longSumVal;
									// longCount++;
									countValue.setValue(longSum);
									mapValueList.set(functionindex, countValue);
									FinalResult.put(strKey, mapValueList);
								} else {
									DoubleValue countValue = (DoubleValue) leafValue;
									double longSum = countValue.getValue()
											+ doubleSumVal;
									// longCount++;
									countValue.setValue(longSum);
									mapValueList.set(functionindex, countValue);
									FinalResult.put(strKey, mapValueList);
								}
							} else { // insert current tuple value as sum
								if (doubleFlagS == 0) {
									LongValue lv = new LongValue(longSumVal);
									mapValueList.add(functionindex, lv);
								} else {
									DoubleValue lv = new DoubleValue(
											doubleSumVal);
									mapValueList.add(functionindex, lv);
								}
							}
							FinalResult.put(strKey, mapValueList);
						} else {
							mapValueList = new ArrayList<LeafValue>(5);
							for (int j = 0; j < function.size(); j++)
								mapValueList.add(null);
							if (doubleFlagS == 0) {
								LongValue lv = new LongValue(longSumVal);
								mapValueList.add(functionindex, lv);
							} else {
								DoubleValue lv = new DoubleValue(doubleSumVal);
								mapValueList.add(functionindex, lv);
							}
						}

						FinalResult.put(strKey, mapValueList);
					}
					functionindex++;
				}
				if ((FinalResult.size() > 0) && (keyNotPresent == 1)) {
					if (previousKey == null)
						previousKey = strKey;
					else {

						if (avgFlag == 1)
							processAverage(countFlag, previousKey);
						else {
							if (countFlag == 0)
								resetCount();

						}
						processResult(previousKey);
						Tuple result2 = sendResult();
						keyNotPresent = 0;
						FinalResult.remove(previousKey);
						previousKey = strKey;
						return result2;
					}
				}

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * if (FinalResult.size() > 0) { processResult(previousKey); result3 =
		 * sendResult(); FinalResult.clear(); } if (result3 != null) return
		 * result3;
		 */
		if (FinalResult.size() > 0) {

			if (avgFlag == 1)
				processAverage(countFlag, previousKey);
			else {
				if (countFlag == 0)
					resetCount();
			}
			processResult(previousKey);
			Tuple result2 = sendResult();
			keyNotPresent = 0;
			FinalResult.remove(previousKey);
			// previousKey=strKey;
			return result2;

		}

		return null;

	}

	public void processAverage(int countFlag, String key) {
		List<LeafValue> mapValueList = new ArrayList<LeafValue>();
		// for (Iterator<String> it = FinalResult.keySet().iterator(); it
		// .hasNext();) {
		// String key = it.next();

		mapValueList = FinalResult.get(key);
		LongValue lctr = (LongValue) (mapValueList.get(0));

		int funCtr = 1;
		for (Function fElement : function) {

			String fName = fElement.getName();
			if (fName.equalsIgnoreCase("avg")) {
				List<Expression> expList = fElement.getParameters()
						.getExpressions();
				Expression avgexp = expList.get(0);
				LeafValue temp1;
				if (sumExpMap.containsKey(avgexp.toString()))
					temp1 = mapValueList.get(sumExpMap.get(avgexp.toString()));
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
		// }
	}

	// If count is not there in select reset entry where count is stored
	public void resetCount() {
		List<LeafValue> mapValueList = new ArrayList<LeafValue>();
		for (Iterator<String> it = FinalResult.keySet().iterator(); it
				.hasNext();) {
			String key = it.next();
			mapValueList = FinalResult.get(key);
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
				if (functionName.equalsIgnoreCase("count"))
					count++;
				else if (functionName.equalsIgnoreCase("sum")) {
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
				} else if (functionName.equalsIgnoreCase("avg")) {
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
				} else if (functionName.equalsIgnoreCase("min")) {
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
				} else if (functionName.equalsIgnoreCase("max")) {

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
			if (functionName.equalsIgnoreCase("count")) {
				lv[i] = new LongValue(count);
				i++;
			} else if ((functionName.equalsIgnoreCase("sum"))) {
				if (doubleflag == 1) {
					lv[i] = new DoubleValue(sumd);
					i++;
				} else {
					lv[i] = new LongValue(suml);
					i++;

				}
			} else if ((functionName.equalsIgnoreCase("avg"))) {
				long rem = asuml % ctr;
				if (rem != 0)
					asumd = asuml;
				if (doubleflagAvg == 1 || rem != 0) {

					LeafValue temp = new DoubleValue(asumd / ctr);
					lv[i] = temp;
				} else {
					lv[i] = new LongValue(asuml);
				}
			} else if ((functionName.equalsIgnoreCase("min"))) {
				if (doubleflagMin == 1) {

					LeafValue temp = new DoubleValue(finalMinD);
					lv[i] = temp;
				} else {
					lv[i] = new LongValue(finalMinL);
				}
			} else if ((functionName.equalsIgnoreCase("max"))) {
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
		String arg_name = arg0.getWholeColumnName();
		String colName;
		int colIdx = -1;
		for (int i = 0; i < columns.size(); i++) {
			edu.buffalo.cse562.table.Column col = columns.get(i);
			if (Helper.columnAliasMap.containsKey(arg0.getWholeColumnName())
					&& t != null && t.getName() != null) {
				exp = Helper.columnAliasMap.get(arg0.getWholeColumnName());
				if (exp instanceof Column) {
					col.setAlias(col.getName());
					col.setName(((Column) exp).getColumnName());
					col.setTable(((Column) exp).getTable().getName());
				}
			}
			if (t == null || t.getName() == null) {
				colName = col.getName();
			} else {
				colName = col.getWholeColumnName();
			}
			if (arg_name.equalsIgnoreCase(colName)) {
				colIdx = i;
				break;
			}
		}
		return current.getData().get(colIdx);
	}

	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return result;
	}
}
