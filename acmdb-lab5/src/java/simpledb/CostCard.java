package simpledb;

import java.util.Vector;

/** Class returned by {@link JoinOptimizer#computeCostAndCardOfSubplan} specifying the
    cost and cardinality of the optimal plan represented by plan.
*/
public class CostCard {
    /** The cost of the optimal subplan */
    public double cost;
    /** The cardinality of the optimal subplan */
    public int card;
    /** The optimal subplan */
    public Vector<LogicalJoinNode> plan;

    CostCard() {
    }

    /** Constructor.
     * 
     * @param cost The cost of the optimal subplan
     * @param card The cardinality of the optimal subplan
     * @param plan he optimal subplan
     */
    CostCard(double cost, int card, Vector<LogicalJoinNode> plan) {
        this.cost = cost;
        this.card = card;
        this.plan = plan;
    }
}
