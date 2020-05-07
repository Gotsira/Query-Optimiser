package sjdb;

import java.util.Iterator;

public class Estimator implements PlanVisitor {

	public Estimator() {
		// empty constructor
	}

	/*
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());

		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}

		op.setOutput(output);
	}

	public void visit(Project op) {
		Relation input = op.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());

		Iterator<Attribute> iter = op.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(input.getAttribute(iter.next())));
		}
		op.setOutput(output);
	}

	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		Predicate p = op.getPredicate();
		Attribute left = input.getAttribute(p.getLeftAttribute());

		Relation output;

		if (p.equalsValue()) {
			output = new Relation(input.getTupleCount() / left.getValueCount());

			for (Attribute attr : input.getAttributes()) {
				if (attr.equals(left)) {
					output.addAttribute(new Attribute(attr.getName(), 1));
				} else {
					output.addAttribute(new Attribute(attr));
				}
			}
		} else {
			Attribute right = input.getAttribute(p.getRightAttribute());
			
			output = new Relation(input.getTupleCount() / Math.max(left.getValueCount(), right.getValueCount()));
			
			for (Attribute attr : input.getAttributes()) {
				if (attr.equals(left) || attr.equals(right)) {
					output.addAttribute(new Attribute(attr.getName(), Math.min(left.getValueCount(), right.getValueCount())));
				} else {
					output.addAttribute(new Attribute(attr));
				}
			}
		}
		op.setOutput(output);
	}

	public void visit(Product op) {
		Relation leftInput = op.getLeft().getOutput();
		Relation rightInput = op.getRight().getOutput();
		
		Relation output = new Relation(leftInput.getTupleCount() * rightInput.getTupleCount());
		
		for (Attribute attr : leftInput.getAttributes()) {
			output.addAttribute(new Attribute(attr));
		}
		
		for (Attribute attr : rightInput.getAttributes()) {
			output.addAttribute(new Attribute(attr));
		}
		op.setOutput(output);
	}

	public void visit(Join op) {
		Relation leftInput = op.getLeft().getOutput();
		Relation rightInput = op.getRight().getOutput();
		
		Predicate p = op.getPredicate();
		Attribute left = leftInput.getAttribute(p.getLeftAttribute());
		Attribute right = rightInput.getAttribute(p.getRightAttribute());
		
		int tupleCount = (leftInput.getTupleCount() * rightInput.getTupleCount()) / Math.max(left.getValueCount(), right.getValueCount());
		int valueCount = Math.min(left.getValueCount(), right.getValueCount());
		
		Relation output = new Relation(tupleCount);
		
		for (Attribute attr : leftInput.getAttributes()) {
			if (attr.equals(left)) {
				output.addAttribute(new Attribute(attr.getName(), valueCount));
			} else {
				output.addAttribute(new Attribute(attr));
			}
		}
		
		for (Attribute attr : rightInput.getAttributes()) {
			if (attr.equals(right)) {
				output.addAttribute(new Attribute(attr.getName(), valueCount));
			} else {
				output.addAttribute(new Attribute(attr));
			}
		}
		
		op.setOutput(output);
	}
}