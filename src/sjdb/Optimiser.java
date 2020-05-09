package sjdb;

import java.util.*;
import java.util.Map.Entry;

/**
 *
 * @author Sirasath Piyapootinun
 */

public class Optimiser {

    /**
     * A hashmap containing the attributes required as the key and the amount
     * of times the attribute is required as the value. This allows the project
     * and select operator (attr=attr) to be added to the correct locations.
     */
    private HashMap<Attribute, Integer> requiredAttrs;

    /**
     * The selects list keeps all the predicates of (attr=val) which will be added
     * as a predicate of select operator later in the correct locations.
     *
     * The joins list keeps all the predicates
     */
    private ArrayList<Predicate> selects, joins;
    private Estimator estimator;
    private boolean addProjections = false;
    private ArrayList<Operator> allRelations;
    int count = 0;

    public Optimiser(Catalogue catalogue) {
        this.requiredAttrs = new HashMap<Attribute, Integer>();
        this.selects = new ArrayList<Predicate>();
        this.joins = new ArrayList<Predicate>();
        this.estimator = new Estimator();
        this.allRelations = new ArrayList<Operator>();
    }

    public Operator optimise(Operator plan) {
        switch (plan.getClass().getName()) {
            case "sjdb.Scan":
                return optimise((Scan) plan);
            case "sjdb.Select":
                return optimise((Select) plan);
            case "sjdb.Project":
                return optimise((Project) plan);
            case "sjdb.Product":
                return optimise((Product) plan);
            default:
                return null;
        }
    }

    public Operator optimise(Scan plan) {
        Relation r = plan.getRelation();
        List<Attribute> attributes = r.getAttributes();
        Operator output = new Scan((NamedRelation) r);
        this.estimator.visit((Scan) output);

        Iterator<Predicate> iterator = selects.iterator();

        while (iterator.hasNext()) {
            Predicate p = iterator.next();
            Attribute attr = p.getLeftAttribute();
            if (attributes.contains(attr)) {

                Attribute newAttr = new Attribute(attr);
                Predicate newPredicate = new Predicate(newAttr, p.getRightValue());

                output = new Select(output, newPredicate);
                this.estimator.visit((Select) output);
                removeRequiredAttribute(newAttr);
                iterator.remove();
            }
        }


        output = addProjectionsToQuery(output);
        if (output != null) {
            this.allRelations.add(output);
        }

        return output;
    }

    public Operator optimise(Select plan) {
        Predicate p = plan.getPredicate();
        addRequiredAttribute(p.getLeftAttribute());
        if (p.equalsValue()) {
            this.selects.add(p);
        } else {
            this.joins.add(p);
            addRequiredAttribute(p.getRightAttribute());
        }
        return optimise(plan.getInput());
    }

    public Operator optimise(Project plan) {
        for (Attribute attr : plan.getAttributes()) {
            addRequiredAttribute(attr);
        }
        this.addProjections = true;
        Operator output = optimise(plan.getInput());
        output = addProjectionsToQuery(output);
        this.addProjections = false;
        return output;
    }

    public Operator optimise(Product plan) {

        // The right side of the product operator is always a relation since it is a left deep tree.
        Operator rightOp = optimise(plan.getRight());
        Operator leftOp = optimise(plan.getLeft());


        if (rightOp == null) {
            return leftOp;
        } else if (leftOp == null){
            return rightOp;
        }

        Iterator<Predicate> allJoins = joins.iterator();
        Operator mostRestrictive = null;
        Predicate selectedPredicate = null;
        Operator outputLeft = null;
        Operator outputRight = null;
        boolean hasJoin = false;

        while (allJoins.hasNext()) {
            Predicate p = allJoins.next();

            Operator left = findRelation(p.getLeftAttribute());
            Operator right = findRelation(p.getRightAttribute());

            if (left == null || right == null) {
                continue;
            }

            hasJoin = true;

            Join testJoin = new Join(left, right, p);
            this.estimator.visit(testJoin);

            if (mostRestrictive == null || testJoin.getOutput().getTupleCount() < mostRestrictive.getOutput().getTupleCount()) {
                mostRestrictive = testJoin;
                selectedPredicate = p;
                outputLeft = left;
                outputRight = right;
            }

        }

        if (hasJoin) {
            removeRequiredAttribute(selectedPredicate.getLeftAttribute());
            if (!selectedPredicate.equalsValue()) {
                removeRequiredAttribute(selectedPredicate.getRightAttribute());
            }
            joins.remove(selectedPredicate);
            mostRestrictive = addProjectionsToQuery(mostRestrictive);
            allRelations.remove(outputLeft);
            allRelations.remove(outputRight);
            if (mostRestrictive == null) {
                mostRestrictive = getFirstRelation();
            }
            allRelations.add(mostRestrictive);

        } else {
            outputLeft = getFirstRelation();
            outputRight = getFirstRelation();

            if (outputRight == null) {
                return outputLeft;
            }
            Product product = new Product(outputLeft, outputRight);
            this.estimator.visit(product);
            mostRestrictive = addProjectionsToQuery(product);
        }

        return mostRestrictive;
    }

    public void addRequiredAttribute(Attribute attr) {
        if (this.requiredAttrs.containsKey(attr)) {
            this.requiredAttrs.put(attr, this.requiredAttrs.get(attr) + 1);
        } else {
            this.requiredAttrs.put(attr, 1);
        }
    }

    public void removeRequiredAttribute(Attribute attr) {
        this.requiredAttrs.put(attr, this.requiredAttrs.get(attr) - 1);
        if (this.requiredAttrs.get(attr) == 0) {
            this.requiredAttrs.remove(attr);
        }
    }

    public Operator addProjectionsToQuery(Operator plan) {
        if (this.addProjections) {
            List<Attribute> attributes = plan.getOutput().getAttributes();
            List<Attribute> projectedAttr = new ArrayList<Attribute>();

            Iterator<Entry<Attribute, Integer>> iterator = this.requiredAttrs.entrySet().iterator();

            while (iterator.hasNext()) {
                Entry<Attribute, Integer> pair = iterator.next();
                Attribute attr = pair.getKey();
                if (attributes.contains(attr) && !projectedAttr.contains(attr)) {
                    projectedAttr.add(attr);
                }
            }

            if (projectedAttr.isEmpty()) {
                return null;
            } else if (projectedAttr.size() != attributes.size()) {
                plan = new Project(plan, projectedAttr);
                this.estimator.visit((Project) plan);
            }
        }
        return plan;
    }

    public Operator findRelation(Attribute attr) {
        Iterator<Operator> relationIterator = allRelations.iterator();

        while (relationIterator.hasNext()) {
            Operator current = relationIterator.next();

            if (current.getOutput().getAttributes().contains(attr)) {
                return current;
            }
        }
        return null;
    }

    public Operator getFirstRelation() {
        if (allRelations.size() == 0) {
            return null;
        }
        Operator first = allRelations.get(0);
        allRelations.remove(first);
        return first;
    }
}