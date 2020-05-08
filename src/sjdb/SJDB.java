/**
 * 
 */
package sjdb;
import java.io.*;

/**
 * @author nmg
 *
 */
public class SJDB {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Inspector inspector = new Inspector();
		// read serialised catalogue from file and parse
		String catFile = "data/cat.txt";
		Catalogue cat = new Catalogue();
		CatalogueParser catParser = new CatalogueParser(catFile, cat);
		catParser.parse();
		
		// read stdin, parse, and build canonical query plan
		QueryParser queryParser = new QueryParser(cat, new InputStreamReader(System.in));
		Operator plan = queryParser.parse();
				
		// create estimator visitor and apply it to canonical plan
		Estimator est = new Estimator();
		plan.accept(est);
		plan.accept(inspector);
		
		// create optimised plan
		Optimiser opt = new Optimiser(cat);
		Operator optPlan = opt.optimise(plan);
	}

}
