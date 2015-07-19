/**
 * 
 */
package edu.buffalo.cse562;

import java.util.List;

import edu.buffalo.cse562.optimizer.Optimizer;
import edu.buffalo.cse562.parser.Parser;
import edu.buffalo.cse562.parser.ParserException;
import edu.buffalo.cse562.relationalAlgebra.Operator;
import edu.buffalo.cse562.table.Tuple;

/**
 * @author tanvi
 * 
 */
public class Main {

	/**
	 * @param args
	 * @throws ParserException
	 */
	public static void main(String[] args) {


		Parser parser = new Parser();
		List<Operator> rootList;
		Tuple t;

		try {
			for (int i = 0; i < args.length; i++) {
				if (args[i].equals("--data")) {
					Helper.dataPath = args[++i];
				} else if (args[i].equals("--swap")) {
					Helper.swapParam = args[++i];
				} else if (args[i].equals("--db")) {
					Helper.dbParam = args[++i];
				} else if (args[i].equals("--load"))
				{
					Helper.isLoad = true;
					i++;
					for(;i<args.length;i++){
						rootList = parser.parse(args[i]);
					}
					Helper.isLoad = false;
				}
				else {
					rootList = parser.parse(args[i]);
					for (Operator root : rootList) {
						root = Optimizer.optimize(root);
						root.open();
						while ((t = root.getNext()) != null) {
							System.out.println(t);
						}
						root.close();
					}
				}
			}
		} catch (ParserException e) {
			e.printStackTrace();
		}
		
	}
}
