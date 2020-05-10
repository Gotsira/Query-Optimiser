package sjdb;

import java.io.*;
import java.util.ArrayList;
import sjdb.DatabaseException;

public class Test {
	private Catalogue catalogue;
	
	public Test() {
	}

	public static void main(String[] args) throws Exception {
		Catalogue catalogue = createCatalogue();
		Inspector inspector = new Inspector();
		Estimator estimator = new Estimator();

		Operator plan = query(catalogue);
		plan.accept(estimator);
		plan.accept(inspector);

		Optimiser optimiser = new Optimiser(catalogue);
		Operator planopt = optimiser.optimise(plan);
		planopt.accept(estimator);
		planopt.accept(inspector);
	}
	
	public static Catalogue createCatalogue() {
		Catalogue cat = new Catalogue();
		cat.createRelation("Employee", 1000);
		cat.createAttribute("Employee", "ESSN", 1000);
		cat.createAttribute("Employee", "BDATE", 1000);
		cat.createAttribute("Employee", "LNAME", 1000);
		cat.createRelation("Works_On", 2000);
		cat.createAttribute("Works_On", "PNO", 100);
		cat.createAttribute("Works_On", "SSN", 1000);
		cat.createRelation("Project", 100);
		cat.createAttribute("Project", "PNUMBER", 100);
		cat.createAttribute("Project", "PNAME", 1000);
		
		return cat;
	}

	public static Operator query(Catalogue cat) throws Exception {
		Scan employee = new Scan(cat.getRelation("Employee"));
		Scan work = new Scan(cat.getRelation("Works_On"));
		Scan project = new Scan(cat.getRelation("Project"));
		
		Product p1 = new Product(employee, work);
		Product p2 = new Product(p1, project);
		
		Select s1 = new Select(p2, new Predicate(new Attribute("PNAME"), "Aquarius"));
		Select s2 = new Select(s1, new Predicate(new Attribute("PNUMBER"), new Attribute("PNO")));
		Select s3 = new Select(s2, new Predicate(new Attribute("BDATE"), "1957-12-31"));
		Select s4 = new Select(s3, new Predicate(new Attribute("ESSN"), new Attribute("SSN")));
		
		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("LNAME"));
		atts.add(new Attribute("SSN"));
//		atts.add(new Attribute("b1"));
		
		Project plan = new Project(s4, atts);
		
		return plan;
	}
	
}

