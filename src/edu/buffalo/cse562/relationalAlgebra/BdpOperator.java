package edu.buffalo.cse562.relationalAlgebra;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

import edu.buffalo.cse562.Helper;
import edu.buffalo.cse562.Helper.ColumnType;
import edu.buffalo.cse562.table.Tuple;

public class BdpOperator extends Operator {
	private Environment myDbEnvironment=null;
	private Database myDatabase=null;
	RelationOperator rop;

	private String fileName;
	public BdpOperator(String fileName) {
		// TODO Auto-generated constructor stub

		//to to Initialize schema
		rop=new RelationOperator(fileName);
		rop.open();
		this.schema=rop.getSchema();


	}
	public void init()
	{
		try
		{
			EnvironmentConfig envConfig=new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			myDbEnvironment=new Environment(new File(Helper.dbParam), envConfig);
			DatabaseConfig dbConfig=new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			myDatabase=myDbEnvironment.openDatabase(null,"sampleDatabase", dbConfig);
		}

		catch(DatabaseException dbe)
		{
			dbe.printStackTrace();
		}
	}
	public void createIndex(int index)
	{
		RelationOperator rop=new RelationOperator("orders");
		rop.open();
		Tuple tuple;
		do
		{
			tuple=rop.getNext();
			if(tuple==null)
				continue;
			try {
				byte[] tupleData=tuple.toString().getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			List <LeafValue> tupledata=tuple.getData();
			LeafValue tupleLeaf= tupledata.get(index);
			String stringKey=null;
			if (tupleLeaf instanceof LongValue) {
				LongValue l = (LongValue) tupleLeaf;
				stringKey=l.toString();
			}
			try {
				DatabaseEntry theKey = new DatabaseEntry(stringKey.getBytes("UTF-8"));
				DatabaseEntry theTuple= new DatabaseEntry(tuple.toString().getBytes("UTF-8"));
				myDatabase.put(null, theKey, theTuple);  		
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}while(tuple!=null);
	}
	public Tuple getTuple(String key)
	{

		DatabaseEntry theKey = null;

		try {
			theKey = new DatabaseEntry(key.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DatabaseEntry tuple=new DatabaseEntry();
		myDatabase.get(null, theKey, tuple, LockMode.DEFAULT);
		byte [] tupleData=tuple.getData();
		try {
			String line=new String(tupleData,"UTF-8");
			//to do, have corresponding constructor

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
			List<edu.buffalo.cse562.table.Column> columns = schema.getColumns();

			List<LeafValue> data = new ArrayList<LeafValue>(columns.size());

			String type = null;
			int i = 0;
			for (; i < line.length(); i++) {

				if (line.charAt(i) == '|') {
					str = line.substring(idxOfNextWord, i);
					idxOfNextWord = i + 1;

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

			Tuple tuple1 = new Tuple(data);
			return tuple1;

			//Tuple t=new Tuple(tupleString.split("\\|"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	@Override
	public void open() {
		// TODO Auto-generated method stub

	}
	@Override
	public Tuple getNext() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
	@Override
	public void createSchema() {
		// TODO Auto-generated method stub

	}
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

}
