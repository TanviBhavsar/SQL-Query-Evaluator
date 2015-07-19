/**
 * 
 */
package edu.buffalo.cse562.optimizer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import edu.buffalo.cse562.Helper;
import edu.buffalo.cse562.relationalAlgebra.RelationOperator;
import edu.buffalo.cse562.table.Column;
import edu.buffalo.cse562.table.Schema;
import edu.buffalo.cse562.table.Tuple;

/**
 * @author tanvi
 * 
 */
public class ExternalSorter {

	private RelationOperator relation;
	private int maxSize;
	private int numPartions;
	private String colName;
	private int colIdx;
	private boolean eof;

	public ExternalSorter(RelationOperator relation, String sortColumn, int k) {
		this.relation = relation;
		maxSize = 20000;
		numPartions = 1;
		eof = false;

		colName = sortColumn;
		Schema schema = relation.getSchema();
		colIdx = 0;
		for (Column column : schema.getColumns()) {
			if (column.getName().equalsIgnoreCase(sortColumn))
				break;
			colIdx++;
		}
	}

	public String sortAndMerge() {
		if (colIdx == 0) {
			String finalName = "Sorted_" + relation.getFileName() + "_"
					+ colName;
			File f = new File(Helper.dataPath + File.separator
					+ relation.getFileName() + Helper.fileFormat);
			try {
				Files.copy(f.toPath(),
						new File(Helper.swapParam + File.separator + finalName
								+ Helper.fileFormat).toPath());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			relation.setSwapFileName(finalName);
		
			return finalName;
		}
		relation.open();
		while (!eof) {
			sort();
		}
		relation.close();

		int numStart = 1, numEnd = numPartions - 1;
		int numSort = numEnd - numStart;

		while (numSort >= 1) {
			if (numEnd % 2 != 0)
				numEnd--;
			

			merge(numStart, numEnd);
			numStart = numEnd + 1;
			numEnd = numPartions - 1;
			numSort = numEnd - numStart;
		}

		String finalName = "Sorted_" + relation.getFileName() + "_" + colName;
		String fileName = finalName + "_" + (--numPartions);
		File f = new File(Helper.swapParam + File.separator + fileName
				+ Helper.fileFormat);
		f.renameTo(new File(Helper.swapParam + File.separator + finalName
				+ Helper.fileFormat));
		relation.setSwapFileName(finalName);
		relation.setFileName(finalName);

		return finalName;
	}

	public void sort() {
		List<Tuple> sortedList = new ArrayList<Tuple>(maxSize);
		Tuple current;

		for (int i = 0; i < maxSize; i++) {
			current = relation.getNext();
			if (current == null) {
				eof = true;
				break;
			}
			sortedList.add(current);
		}

		final int sortBy = colIdx;

		Collections.sort(sortedList, new Comparator<Tuple>() {
			@Override
			public int compare(Tuple a, Tuple b) {
				LeafValue val1 = a.getData().get(sortBy);
				LeafValue val2 = b.getData().get(sortBy);

				int retVal;

			
				if (val1 instanceof LongValue) {
					Long l1 = ((LongValue) val1).getValue();
					Long l2 = ((LongValue) val2).getValue();
					retVal = l1.compareTo(l2);
				} else {
					retVal = val1.toString().compareTo(val2.toString());
				}
				return retVal;
			}
		});

		String fileName = Helper.swapParam + File.separator + "Sorted_"
				+ relation.getFileName() + "_" + colName + "_" + numPartions
				+ Helper.fileFormat;

		writeToDisk(sortedList, fileName);
		numPartions++;
	}

	public void merge(int numStart, int numEnd) {
	
		StringBuilder finalList;
		RelationOperator relation1, relation2;

		String fileName1, fileName2;
		int j = numStart;
		int i = j + 1;
		for (; i <= numEnd; i = j + 1) {
			int cnt = 0;
			fileName1 = "Sorted_" + relation.getFileName() + "_" + colName
					+ "_" + j;
			relation1 = new RelationOperator(fileName1);
			relation1.setSchema(relation.getSchema());
			fileName2 = "Sorted_" + relation.getFileName() + "_" + colName
					+ "_" + i;
			relation2 = new RelationOperator(fileName2);
			relation2.setSchema(relation.getSchema());

			
			finalList = new StringBuilder();

			Tuple a, b;

			relation1.openSwap();
			relation2.openSwap();
			a = relation1.getNext();
			b = relation2.getNext();
			int retVal;

			while (a != null && b != null) {

				LeafValue val1 = a.getData().get(colIdx);
				LeafValue val2 = b.getData().get(colIdx);

			
				if (val1 instanceof LongValue) {
					Long l1 = ((LongValue) val1).getValue();
					Long l2 = ((LongValue) val2).getValue();
					retVal = l1.compareTo(l2);
				} else {
					retVal = val1.toString().compareTo(val2.toString());
				}

				if (retVal < 0) {
					finalList.append(a);
					finalList.append("\n");
					cnt++;
			
					a = relation1.getNext();
				} else if (retVal > 0) {
					finalList.append(b);
					finalList.append("\n");
					cnt++;
			
					b = relation2.getNext();
				} else {
					finalList.append(a);
					finalList.append("\n");
					cnt++;

					finalList.append(b);
					finalList.append("\n");
					cnt++;

				
					a = relation1.getNext();
					b = relation2.getNext();
				}

				if (cnt >= maxSize) {
					writeToDisk(finalList);
					finalList = new StringBuilder();
					cnt = 0;
				}
				
			}

			if (a != null) {
				finalList.append(a);
				finalList.append("\n");
				cnt++;

			
				while ((a = relation1.getNext()) != null) {
					finalList.append(a);
					finalList.append("\n");
					cnt++;

					if (cnt >= maxSize) {
						writeToDisk(finalList);
						finalList = new StringBuilder();
						cnt = 0;
					}
					
				}
			}

			if (b != null) {
			
				finalList.append(b);
				finalList.append("\n");
				cnt++;

				while ((b = relation2.getNext()) != null) {
					// finalList.add(b);
					finalList.append(b);
					finalList.append("\n");
					cnt++;

					if (cnt >= maxSize) {
						writeToDisk(finalList);
						finalList = new StringBuilder();
						cnt = 0;
					}
				
				}
			}
			writeToDisk(finalList);
			finalList = null;
			numPartions++;

			relation1.close();
			relation2.close();

			deleteFile(fileName1);
			deleteFile(fileName2);

			j = i + 1;
		}

	}

	public static void writeToDisk(List<Tuple> sortedList, String fileName) {
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(new BufferedWriter(new FileWriter(
					fileName, true), 50000));
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (Tuple t : sortedList) {
			writer.write(t.toString());
			writer.write("\n");
		}

		writer.close();
		System.gc();

	}

	private void writeToDisk(StringBuilder sb) {
		String fileName = Helper.swapParam + File.separator + "Sorted_"
				+ relation.getFileName() + "_" + colName + "_" + numPartions
				+ Helper.fileFormat;
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(new BufferedWriter(new FileWriter(
					fileName, true)));
		} catch (IOException e) {
			e.printStackTrace();
		}

		writer.write(sb.toString());


		writer.close();
		System.gc();
	}

	private void deleteFile(String fileName) {
		File f = new File(Helper.swapParam + File.separator + fileName
				+ Helper.fileFormat);
		f.deleteOnExit();
	
	}
}
