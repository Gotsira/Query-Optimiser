package sjdb;

import java.util.*;
import java.util.Map.Entry;

public class Optimiser {
    private Catalogue catalogue;
    private HashMap<Attribute, Integer> requiredAttrs;
    private ArrayList<Predicate> selects, joins;
    private Estimator estimator;
    private boolean addProjections = false;

    public Optimiser(Catalogue catalogue) {
        this.catalogue = catalogue;
        this.requiredAttrs = new HashMap<Attribute, Integer>();
        this.selects = new ArrayList<Predicate>();
        this.joins = new ArrayList<Predicate>();
        this.estimator = new Estimator();
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

        for (Predicate p : this.selects) {
            if (attributes.contains(p.getLeftAttribute())) {
                output = new Select(output, p);
                this.estimator.visit((Select) output);
                removeRequiredAttribute(p.getLeftAttribute());
                this.selects.remove(p);
            }
        }

        return addProjectionsToQuery(output);
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
        return plan;
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

            for (Map.Entry<Attribute, Integer> reqAttribute : this.requiredAttrs.entrySet()) {
                Attribute attr = reqAttribute.getKey();
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
}