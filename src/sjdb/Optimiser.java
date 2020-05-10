package sjdb;

import java.util.*;
import java.util.Map.Entry;

/**
 * A class which attempts to optimise a given query plan.
 *
 * @author Sirasath Piyapootinun
 */

public class Optimiser {

    /**
     * A hashmap containing the attributes required as the key and the amount
     * of times the attribute is required as the value. This allows the project
     * and select operator to be added to the correct locations.
     */
    private HashMap<Attribute, Integer> requiredAttrs;

    /**
     * The selects list keeps all the predicates which are in the format of
     * attr=val which will be added as a predicate of select operator later
     * in the correct locations.
     *
     * The joins list keeps all the predicates which are in the format of
     * attr=attr which will be added as a predicate to the join operator later
     * in the correct location.
     */
    private ArrayList<Predicate> selects, joins;

    /**
     * An estimator which is used to estimate the cost of each operation.
     */
    private Estimator estimator;

    /**
     * A variable to keep check that the Project operator is present in the
     * query as if not present, it is not require to project meaning the query
     * is no needed.
     *
     * If true, the project operator can be added.
     */
    private boolean addProjections = false;

    /**
     * A list to keep all operators of a relation.
     */
    private ArrayList<Operator> allRelations;

    /**
     * Constructor to initialize all required attributes.
     * @param catalogue Not used but required in constructor to make the
     *                  application work.
     */
    public Optimiser(Catalogue catalogue) {
        this.requiredAttrs = new HashMap<Attribute, Integer>();
        this.selects = new ArrayList<Predicate>();
        this.joins = new ArrayList<Predicate>();
        this.estimator = new Estimator();
        this.allRelations = new ArrayList<Operator>();
    }

    /**
     * Optimises a given operator but this method just call the correct
     * optimise method based on the type of the operator.
     *
     * @param plan The operator to be optimised.
     * @return A new optimised operator.
     */
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

    /**
     * Optimises a Scan operator by adding a select operator of predicate
     * 'attr=val' above the Scan operator. It will also add the Project over
     * the results if all of the attributes are not required. But if the
     * Relation in the Scan is found not to be required by the query, the
     * method return a null.
     *
     * @param plan Scan operator to be optimised.
     * @return A new optimised operator with the appropriate Select and
     *         Project operator if required or a null if the Relation in the
     *         Scan is not required.
     */
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

    /**
     * Optimises a Select Operator. This method simply add the predicates of
     * the Select Operator to the appropriate lists where predicate 'attr=attr'
     * will be added to the joins list and 'attr=val' will be added to the selects
     * lists and these predicates will later on be added with the appropriate
     * operator respectively.
     *
     * @param plan Select operator to be optimised.
     * @return A new optimised operator with the Select operator moved down or
     *         replaced with Join operator.
     */
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

    /**
     * Optimises a Project operator. This method simply keep track of what
     * attributes a required to be projected. New Project Operators are added
     * into the tree whenever required by the other functions.
     *
     * @param plan Project operator to be optimised.
     * @return A new optimised operator
     */
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

    /**
     * Optimises Product operator. First I go down the tree to optimise the Scan
     * Operator. Then based on the predicates I collected for the join operator,
     * I would simulate the joining for each predicates and choose the join with
     * least number of tuples in the results of the joining. A Project operator
     * will also be added over the Join operator all the attributes of the Join
     * is not required.
     *
     * But if any optimised Scan returns a null, it would mean that the particular
     * Scan is not required by the query, we can simply return the operator which
     * is required by the query.
     *
     * @param plan Product operator to be optimised.
     * @return A new optimised operator with Join operator replacing the Product
     *         operator or a Product operator with the inputs optimised.
     */
    public Operator optimise(Product plan) {

        /**
         * The right side of the product operator is always a relation since it is
         * a left deep tree so the right of the Product operator is always a
         * Scan operator. So we optimise the right side of the operator first to get
         * the
         */
        Operator rightOp = optimise(plan.getRight());
        Operator leftOp = optimise(plan.getLeft());


        /**
         * In case any Scan returns a null, we can simply return the other optimised
         * Scan but if both is null, return a null as both Scans are not required by
         * the query.
         */
        if (leftOp == null && rightOp == null) {
            return null;
        } else if (rightOp == null) {
            return leftOp;
        } else if (leftOp == null){
            return rightOp;
        }

        Iterator<Predicate> allJoins = joins.iterator();
        Operator mostRestrictive = null;
        Predicate selectedPredicate = null;
        Operator outputLeft = null;
        Operator outputRight = null;

        // if true, a join is successful
        boolean hasJoin = false;

        while (allJoins.hasNext()) {
            Predicate p = allJoins.next();

            Operator left = findOperator(p.getLeftAttribute());
            Operator right = findOperator(p.getRightAttribute());

            // check if any attributes in the predicates does not exist in any relations.
            if (left == null || right == null) {
                continue;
            } else if (right.toString().length() > left.toString().length()) {
                Operator temp = left;
                left = right;
                right = temp;
                p = new Predicate(p.getRightAttribute(), p.getLeftAttribute());
            }

            hasJoin = true;

            Join testJoin = new Join(left, right, p);
            this.estimator.visit(testJoin);

            /**
             * Assign the testJoin to be the mostRestrictive on if it is the first one or if the
             * testJoin has lesser number of tuples than the mostRestrictive.
             */
            if (mostRestrictive == null || testJoin.getOutput().getTupleCount() < mostRestrictive.getOutput().getTupleCount()) {
                mostRestrictive = testJoin;
                selectedPredicate = p;
                outputLeft = left;
                outputRight = right;
            }

        }

        /**
         * If joining was successful, remove the predicate used in the join from the
         * joins list and remove the required attribute in the requiredAttrs hashMap.
         *
         * If the joining was unsuccessful, it would mean that there are no the rest
         * of the operator does not require any joining so we would simply get any
         * 2 operators and add the Product operator above them.
         */
        if (hasJoin) {
            removeRequiredAttribute(selectedPredicate.getLeftAttribute());
            removeRequiredAttribute(selectedPredicate.getRightAttribute());
            joins.remove(selectedPredicate);
            mostRestrictive = addProjectionsToQuery(mostRestrictive);
            allRelations.remove(outputLeft);
            allRelations.remove(outputRight);
            if (mostRestrictive == null) {
                mostRestrictive = getFirstOperator();
            }
            allRelations.add(mostRestrictive);

        } else {
            outputLeft = getFirstOperator();
            outputRight = getFirstOperator();

            if (outputRight == null) {
                return outputLeft;
            }
            Product product = new Product(outputLeft, outputRight);
            this.estimator.visit(product);
            mostRestrictive = addProjectionsToQuery(product);
        }

        return mostRestrictive;
    }

    /**
     * Add a count of the attribute in the hashMap containing all of the
     * required attributes.
     *
     * @param attr The attribute required to be added to the hashMap.
     */
    public void addRequiredAttribute(Attribute attr) {
        if (this.requiredAttrs.containsKey(attr)) {
            this.requiredAttrs.put(attr, this.requiredAttrs.get(attr) + 1);
        } else {
            this.requiredAttrs.put(attr, 1);
        }
    }

    /**
     * Remove a count of the attribute in the hashMap containing all of the
     * required attributes.
     * @param attr The attribute required to be removed to the hashMap.
     */
    public void removeRequiredAttribute(Attribute attr) {
        this.requiredAttrs.put(attr, this.requiredAttrs.get(attr) - 1);
        if (this.requiredAttrs.get(attr) == 0) {
            this.requiredAttrs.remove(attr);
        }
    }

    /**
     * Adds a Project operator above the given operator if all of the attributes
     * of the results of the given operator is not required.
     *
     * @param plan The Operator required to add a Project operator above it.
     * @return An operator with the required Project operator above the input
     *         operator but will return a null, if none of the attributes of the
     *         input operator is required.
     */
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

    /**
     * Finds the operator whose results contains the attribute.
     * @param attr Attribute whose operator is required.
     * @return The Operator whose results contains the attribute.
     */
    public Operator findOperator(Attribute attr) {
        Iterator<Operator> relationIterator = allRelations.iterator();

        while (relationIterator.hasNext()) {
            Operator current = relationIterator.next();

            if (current.getOutput().getAttributes().contains(attr)) {
                return current;
            }
        }
        return null;
    }

    /**
     * Gets the first operator in the allRelations list.
     *
     * @return The first operator in the all Relations or null if allRelations
     *         list is empty
     */
    public Operator getFirstOperator() {
        if (allRelations.isEmpty()) {
            return null;
        }
        Operator first = allRelations.get(0);
        allRelations.remove(first);
        return first;
    }
}