/**
 * 
 */
package edu.buffalo.cse562.parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import edu.buffalo.cse562.Helper;
import edu.buffalo.cse562.relationalAlgebra.AggregateOperator;
import edu.buffalo.cse562.relationalAlgebra.BdpOperator;
import edu.buffalo.cse562.relationalAlgebra.JoinOperator;
import edu.buffalo.cse562.relationalAlgebra.LimitOperator;
import edu.buffalo.cse562.relationalAlgebra.Operator;
import edu.buffalo.cse562.relationalAlgebra.OrderByOperator;
import edu.buffalo.cse562.relationalAlgebra.ProjectOperator;
import edu.buffalo.cse562.relationalAlgebra.RelationOperator;
import edu.buffalo.cse562.relationalAlgebra.SelectOperator;
import edu.buffalo.cse562.relationalAlgebra.UnionOperator;
import edu.buffalo.cse562.table.Schema;

/**
 * @author ketkiram, jlimaye
 * 
 */
public class Parser {

	private CCJSqlParser sqlParser;
	String stmt = null;
	private BdpOperator bdpOperator;

	/*
	 * Parses the given sql file which may contain only create and select type
	 * of statements
	 */
	public List<Operator> parse(String filePath) throws ParserException {

		// if (filePath == null || filePath.isEmpty())
		// throw new ParserException("No file specified");

		File inputFile = new File(filePath);

		// if (!inputFile.exists())
		// throw new ParserException("File does not exist");

		FileReader input = null;
		try {
			input = new FileReader(inputFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		sqlParser = new CCJSqlParser(input);
		Statement statement;

		List<Operator> rootList = new ArrayList<Operator>();
		Operator root = null;

		Schema schema = null;

		try {
			// Helper.schemaMap.clear();

			while ((statement = sqlParser.Statement()) != null) {

				if (statement instanceof CreateTable) {
					CreateTable createTable = (CreateTable) statement;
					schema = new Schema(createTable.getColumnDefinitions(),
							createTable.getTable().getName().toLowerCase());
					Helper.schemaMap.put(schema.getTableName().toLowerCase(),
							schema);
					if(Helper.isLoad &&
						createTable.getTable().getName().toLowerCase().equals("orders")){
						bdpOperator = new BdpOperator(createTable.getTable().getName().toLowerCase());
						bdpOperator.init();
						bdpOperator.createIndex(0);
					}
				} else if (statement instanceof Select) {
					Helper.tableAliasMap.clear();
					Helper.columnAliasMap.clear();
					Select select = (Select) statement;
					stmt = statement.toString();
					SelectBody body = select.getSelectBody();
					root = procesSelectBody(body);
				}
				if (root != null)
					rootList.add(root);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return rootList;
	}

	/*
	 * Function to process any type of Select. This will be called recursively
	 * for nested Selects.
	 */
	private Operator procesSelectBody(SelectBody body) {

		Operator root = null;
		if (body instanceof PlainSelect) {
			root = this.createPlainSelect(body);
		} else if (body instanceof Union) {
			Union unionBody = (Union) body;
			List<PlainSelect> plainSelectList = unionBody.getPlainSelects();
			Operator rightChild, leftChild, union;
			leftChild = createPlainSelect(plainSelectList.get(0));
			for (int i = 1; i < plainSelectList.size(); i++) {
				rightChild = createPlainSelect(plainSelectList.get(i));
				union = new UnionOperator(leftChild, rightChild);
				root = union;
				leftChild = root;
			}
		}
		return root;
	}

	private Operator createPlainSelect(SelectBody body) {
		Operator child, root = null;

		PlainSelect plainBody = (PlainSelect) body;
		Limit Iplimit = plainBody.getLimit();

		FromItem from = plainBody.getFromItem();
		Expression where = plainBody.getWhere();
		List<SelectItem> selectItems = plainBody.getSelectItems();

		Operator fromOperator;
		if (from instanceof Table) {
			// No nested queries
			fromOperator = new RelationOperator(
					((Table) from).getWholeTableName());
			String alias = from.getAlias();
			if (alias != null) {
				Helper.tableAliasMap.put(alias, from);
				fromOperator.getSchema().setAlias(alias);
			}
		} else {
			// Recursively process nested select
			SubSelect sub = (SubSelect) from;
			fromOperator = procesSelectBody(sub.getSelectBody());
		}
		child = fromOperator;
		// create join structure
		List<Join> joinList = plainBody.getJoins();

		Operator whereOperator;
		if (joinList != null && !joinList.isEmpty())
			child = processjoin(joinList, where, fromOperator);
		// else {
		if (where != null) {
			whereOperator = new SelectOperator(where, child);
			child = whereOperator;
		}
		// }

		List<Function> functionlist = new ArrayList<Function>();
		List<Column> GroupColumnList = null;
		// Expression HavingExpression = null;
		for (SelectItem item : selectItems) {
			// if (item instanceof AllColumns) {
			// return child;
			// } else
			if (item instanceof SelectExpressionItem) {
				SelectExpressionItem expItem = (SelectExpressionItem) item;
				Expression expression = expItem.getExpression();

				if (expItem.getAlias() != null)
					Helper.columnAliasMap.put(expItem.getAlias(), expression);

				// check if this is an aggregate expression
				if (expression instanceof Function) {
					// functionlist = new ArrayList<Function>();
					functionlist.add((Function) expression);
					// HavingExpression = plainBody.getHaving();
				}
			}
		}
		GroupColumnList = plainBody.getGroupByColumnReferences();


		// Operator agg;
		if (functionlist.size() > 0) {
			// if (Helper.swapParam == null)
			child = new AggregateOperator(functionlist, child, GroupColumnList);
			// else
			// child = new SortAggregateOperator(functionlist, child,
			// GroupColumnList);

			// if (HavingExpression == null)
			// child = agg;
			// else
			// child = new HavingOperator(HavingExpression, agg);

		}
		root = new ProjectOperator(selectItems, child);
		List<OrderByElement> orderList = plainBody.getOrderByElements();
		if (orderList != null && !orderList.isEmpty())
			root = new OrderByOperator(orderList, root);
		if (Iplimit != null)
			root = new LimitOperator(Iplimit.getRowCount(), root);
		return root;

	}

	private Operator processjoin(List<Join> joinList, Expression where,
			Operator leftOperator) {
		Operator root = null;
		Operator rightOperator;
		for (Join join : joinList) {
			FromItem rightItem = join.getRightItem();
			// Expression onExpression = join.getOnExpression();

			if (rightItem instanceof Table) {
				rightOperator = new RelationOperator(
						((Table) rightItem).getWholeTableName());
				String alias = rightItem.getAlias();
				if (alias != null) {
					Helper.tableAliasMap.put(alias, rightItem);
					rightOperator.getSchema().setAlias(alias);
				}
			} else {
				// Recursively process nested select
				SubSelect sub = (SubSelect) rightItem;
				rightOperator = procesSelectBody(sub.getSelectBody());
			}
			root = new JoinOperator(null, leftOperator, rightOperator);
			leftOperator = root;
		}
		return root;
	}
}
