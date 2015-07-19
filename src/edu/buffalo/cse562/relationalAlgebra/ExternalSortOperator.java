package edu.buffalo.cse562.relationalAlgebra;

//import com.sun.corba.se.impl.transport.ReadTCPTimeoutsImpl;

public class ExternalSortOperator {
	// private RelationOperator readTuple = null;
	// private String sortFile = "lineitem";
	// List<RelationOperator> pass1RelationList, pass2RelationList;
	// private String stringPass = "pass";
	// private String stringFile = "File";
	// private int noOfRecords;
	//
	// public ExternalSortOperator() {
	// pass1RelationList = new ArrayList<RelationOperator>();
	// pass2RelationList = new ArrayList<RelationOperator>();
	// }
	//
	// public Tuple loadTuple() {
	// if (readTuple == null) {
	// readTuple = new RelationOperator(sortFile);
	// readTuple.open();
	// }
	// // readTuple.fileName="R.dat";
	//
	// Tuple tuple = readTuple.getNext();
	// return tuple;
	//
	// }
	//
	// private void passFunction(int passNo) throws IOException {
	// // TODO Auto-generated method stub
	//
	// System.out.println("no of records is" + noOfRecords);
	// for (RelationOperator rOp : pass1RelationList)
	// rOp.openSwap();
	// int size = pass1RelationList.size();
	// int fileCounter = 0;
	// for (int i = 0; i < size; i++) {
	// fileCounter++;
	// RelationOperator operatorRelation1 = pass1RelationList.get(i);
	// RelationOperator operatorRelation2 = null;
	// i++;
	// Tuple tuple1, tuple2;
	// if (i == size) {
	// tuple2 = null;
	// } else {
	// operatorRelation2 = pass1RelationList.get(i);
	// tuple2 = operatorRelation2.getNext();
	// }
	// tuple1 = operatorRelation1.getNext();
	//
	// while ((tuple1 != null) && (tuple2 != null)) {
	// List<LeafValue> tupleValues1 = tuple1.getData();
	// List<LeafValue> tupleValues2 = tuple2.getData();
	// int compareNext = compareTuples(tupleValues1.get(0),
	// tupleValues2.get(0));
	//
	// // writeTuples(tupleValues1, tupleValues2, i, 1, compareNext);
	// if (compareNext < 0) {
	// writeTuples(tupleValues1, null, fileCounter, passNo,
	// compareNext);
	// tuple1 = operatorRelation1.getNext();
	// } else if (compareNext > 0) {
	// writeTuples(tupleValues2, null, fileCounter, passNo,
	// compareNext);
	// tuple2 = operatorRelation2.getNext();
	//
	// } else if (compareNext == 0) {
	// writeTuples(tupleValues1, tupleValues2, fileCounter,
	// passNo, compareNext);
	// tuple1 = operatorRelation1.getNext();
	// tuple2 = operatorRelation2.getNext();
	// }
	// }
	// while (tuple1 != null) {
	// List<LeafValue> tupleValues1 = tuple1.getData();
	// writeTuples(tupleValues1, null, fileCounter, passNo, 0);
	// tuple1 = operatorRelation1.getNext();
	// }
	// while (tuple2 != null) {
	// List<LeafValue> tupleValues2 = tuple2.getData();
	// writeTuples(tupleValues2, null, fileCounter, passNo, 0);
	// tuple2 = operatorRelation2.getNext();
	// }
	//
	// }
	// for (RelationOperator rOp : pass1RelationList)
	// rOp.close();
	//
	// }
	//
	// public void printResult() {
	// Tuple tuple = loadTuple();
	// int countFile = 0;
	// int compareResult = 0;
	// BufferedReader reader = null;
	// try {
	// reader = new BufferedReader(new FileReader(Helper.dataPath
	// + File.separator + sortFile + Helper.fileFormat));
	// } catch (FileNotFoundException e1) {
	// // TODO Auto-generated catch block
	// e1.printStackTrace();
	// }
	// noOfRecords = 0;
	// try {
	// while (reader.readLine() != null)
	// noOfRecords++;
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	//
	// try {
	// reader.close();
	// } catch (IOException e1) {
	// // TODO Auto-generated catch block
	// e1.printStackTrace();
	// }
	// while (tuple != null) {
	// countFile++;
	// compareResult = 0;
	// System.out.println("Read tuple" + tuple.toString());
	//
	// List<LeafValue> tupleValues1 = tuple.getData();
	// tuple = loadTuple();
	// // List<LeafValue> tupleValues2=null;
	// StringBuilder sb = null;
	// String writeString2 = null;
	// if (tuple != null) {
	// // tuple = loadTuple();
	// List<LeafValue> tupleValues2 = tuple.getData();
	// tuple = loadTuple();
	//
	// // compareResult=compareTuples(val1,val2);
	// // sorting on first value TO do sort based on join value
	// LeafValue val1 = tupleValues1.get(0);
	// LeafValue val2 = tupleValues2.get(0);
	// compareResult = compareTuples(val1, val2);
	//
	// writeTuples(tupleValues1, tupleValues2, countFile, 0,
	// compareResult);
	// // compareResult=
	// //
	// tupleValues1.get(0).toLong().compareTo(tupleValues2.get(0).toString());
	//
	// } else {
	//
	// writeTuples(tupleValues1, null, countFile, 0, 0);
	// }
	// }
	// double no=(noOfRecords/2.0);
	// //double no=(double)(5/2.0);
	// double noOfFrames = Math.ceil(no);
	// // int noOfFrames=(int)temp1;
	// double temp = Math.ceil(Math.log(noOfFrames) / Math.log(2));
	// // Math.log(arg0)
	// int noOfPasses = (int) temp;
	// noOfPasses++;
	// for (int j = 1; j < noOfPasses; j++) {
	// try {
	// passFunction(j);
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// // To do delete files in pass1Relationlist
	// deletefiles();
	// pass1RelationList = new ArrayList<RelationOperator>(
	// pass2RelationList);
	// pass2RelationList.clear();
	// }
	// /*
	// * passFunction(2); pass1RelationList=new
	// * ArrayList<RelationOperator>(pass2RelationList);
	// * pass2RelationList.clear(); passFunction(3);
	// */
	// System.out.println("break");
	//
	// }
	//
	// private void deletefiles() {
	// // TODO Auto-generated method stub
	// for (RelationOperator op : pass1RelationList) {
	// File f = new File(Helper.swapParam + File.separator + op.fileName
	// + Helper.fileFormat);
	// f.delete();
	// // Files.delete(op.fileName);
	// }
	// }
	//
	// private void writeTuples(List<LeafValue> tupleValues1,
	// List<LeafValue> tupleValues2, int countFile, int passNo,
	// int compareResult) {
	// StringBuilder sb = null;
	// String writeString2 = null;
	// sb = new StringBuilder();
	// if (tupleValues2 != null) {
	// for (LeafValue lv : tupleValues2) {
	// //To do remove ' from string
	// if (lv instanceof StringValue)
	// sb.append(((StringValue) lv).getNotExcapedValue());
	// else
	// sb.append(lv.toString());
	// sb.append("|");
	// }
	//
	// sb.setLength(sb.length() - 1);
	// writeString2 = sb.toString();
	//
	// sb = new StringBuilder();
	// }
	// for (LeafValue lv : tupleValues1) {
	//
	// if (lv instanceof StringValue)
	// sb.append(((StringValue) lv).getNotExcapedValue());
	// else
	// sb.append(lv.toString());
	// sb.append("|");
	// }
	// sb.setLength(sb.length() - 1);
	// String writeString1 = sb.toString();
	//
	// String writeFileName = stringPass + passNo + stringFile + countFile;
	// // to do change to swappath
	// // File writeFile=new File(Helper.swapParam +
	// // File.separator+writeFileName+".dat");
	// String fileNameFinal = Helper.swapParam + File.separator + writeFileName
	// + ".dat";
	// // pass1RelationList.add(new RelationOperator(writeFileName));
	// // TO do create file in swap directory
	// PrintWriter writer = null;
	// try {
	// writer = new PrintWriter(new BufferedWriter(new FileWriter(
	// fileNameFinal, true)));
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// // writer = new PrintWriter(writeFile);
	//
	// if (compareResult < 0) {
	// writer.println(writeString1);
	// if (writeString2 != null)
	// writer.println(writeString2);
	// } else {
	// if (writeString2 != null)
	// writer.println(writeString2);
	// writer.println(writeString1);
	// }
	// writer.close();
	// // to do no need to add everything schema for all sub files in map as
	// // schemas same
	// Schema schema = Helper.schemaMap.get(sortFile.toLowerCase());
	// Helper.schemaMap.put(writeFileName.toLowerCase(), schema);
	// if (passNo == 0)
	// pass1RelationList.add(new RelationOperator(writeFileName));
	// else {
	// // if(pass2RelationList.contains(new Rela))\
	// int flag = 0;
	// for (RelationOperator searchOp : pass2RelationList) {
	// if (searchOp.fileName.equals(writeFileName)) {
	// flag = 1;
	// break;
	// }
	// }
	// if (flag == 0)
	// pass2RelationList.add(new RelationOperator(writeFileName));
	//
	// }
	// }
	//
	// private int compareTuples(LeafValue val1, LeafValue val2) {
	// // TODO Auto-generated method stub
	//
	// int compareResult = val1.toString().compareTo(val2.toString());
	//
	// if (val1 instanceof DateValue) {
	// Date d1 = ((DateValue) val1).getValue();
	// Date d2 = ((DateValue) val2).getValue();
	// compareResult = d1.compareTo(d2);
	// } else if (val1 instanceof DoubleValue) {
	// Double d1 = ((DoubleValue) val1).getValue();
	// Double d2 = ((DoubleValue) val2).getValue();
	// compareResult = d1.compareTo(d2);
	// } else if (val1 instanceof LongValue) {
	// Long l1 = ((LongValue) val1).getValue();
	// Long l2 = ((LongValue) val2).getValue();
	// compareResult = l1.compareTo(l2);
	// }
	// return compareResult;
	// }
}
