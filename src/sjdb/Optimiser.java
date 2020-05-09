package sjdb;

import java.util.*;
import java.util.Map.Entry;

public class Optimiser {
    private Catalogue catalogue;
    private HashMap<Attribute, Integer> requiredAttrs;
    private ArrayList<Predicate> selects, joins;
    private Estimator estimator;
    private boolean addProjections = false;
    private HashMap<Relation, Operator> allRelations;

    public Optimiser(Catalogue catalogue) {
        this.catalogue = catalogue;
        this.requiredAttrs = new HashMap<Attribute, Integer>();
        this.selects = new ArrayList<Predicate>();
        this.joins = new ArrayList<Predicate>();
        this.estimator = new Estimator();
        this.allRelations = new HashMap<Relation, Operator>();
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

        Iterator<Predicate> iterator = selects.iterator();

        while (iterator.hasNext()) {
            Predicate p = iterator.next();
            if (attributes.contains(p.getLeftAttribute())) {
                output = new Select(output, p);
                this.estimator.visit((Select) output);
                removeRequiredAttribute(p.getLeftAttribute());
                iterator.remove();
            }
        }


        output = addProjectionsToQuery(output);
        this.allRelations.put(r, output);

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
        optimise(plan.getRight());
        optimise(plan.getLeft());

        Iterator<Predicate> allJoins = joins.iterator();
        Operator mostRestrictive = null;
        Predicate selectedPredicate = null;
        Entry<Relation, Operator> outputLeft = null;
        Entry<Relation, Operator> outputRight = null;

        while (allJoins.hasNext()) {
            Predicate p = allJoins.next();

            Entry<Relation, Operator> left = findRelation(p.getLeftAttribute());
            Entry<Relation, Operator> right = findRelation(p.getRightAttribute());

            if (left == null || right == null) {
                continue;
            }

            Join testJoin = new Join(left.getValue(), right.getValue(), p);
            this.estimator.visit(testJoin);

            if (mostRestrictive == null || testJoin.getOutput().getTupleCount() < mostRestrictive.getOutput().getTupleCount()) {
                mostRestrictive = testJoin;
                selectedPredicate = p;
                outputLeft = left;
                outputRight = right;
            }

        }

        removeRequiredAttribute(selectedPredicate.getLeftAttribute());
        if (!selectedPredicate.equalsValue()) {
            removeRequiredAttribute(selectedPredicate.getRightAttribute());
        }
        joins.remove(selectedPredicate);
        mostRestrictive = addProjectionsToQuery(mostRestrictive);
        allRelations.put(outputLeft.getKey(), mostRestrictive);
        allRelations.put(outputRight.getKey(), mostRestrictive);


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
                plan = new Scan(new NamedRelation("Empty", 0));
            } else if (projectedAttr.size() != attributes.size()) {
                plan = new Project(plan, projectedAttr);
                this.estimator.visit((Project) plan);
            }
        }
        return plan;
    }

    public Entry<Relation, Operator> findRelation(Attribute attr) {
        Iterator<Entry<Relation, Operator>> relationIterator = allRelations.entrySet().iterator();

        while (relationIterator.hasNext()) {
            Entry<Relation, Operator> current = relationIterator.next();

            if (current.getValue().getOutput().getAttributes().contains(attr)) {
                return current;
            }
        }
        return null;
    }
}