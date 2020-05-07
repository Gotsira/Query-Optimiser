package sjdb;

public class Optimiser {
	Catalogue catalogue;

	public Optimiser(Catalogue catalogue) {
		this.catalogue = catalogue;
	}
	
	public Operator optimise(Operator plan) {
		return plan;
		
	}
}