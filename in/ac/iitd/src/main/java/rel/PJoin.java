package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import convention.PConvention;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/*
    * Implement Hash Join
    * The left child is blocking, the right child is streaming
*/
public class PJoin extends Join implements PRel {

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
                super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
                assert getConvention() instanceof PConvention;
    }

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }

    private List<RexInputRef> getJoinAttributes(RexNode condition) {
        List<RexInputRef> joinAttributes = new ArrayList<>();

        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
                for (RexNode operand : call.getOperands()) {
                    if (operand instanceof RexInputRef) {
                        joinAttributes.add((RexInputRef) operand);
                    }
                }
            }
        }

        return joinAttributes;
    }

    private int hashFunction(Object[] row, List<RexInputRef> joinAttributes) {
        int hash = 13;
        for (RexInputRef attr : joinAttributes) {
            Object value = row[attr.getIndex()];
            hash = 37 * hash + (value == null ? 0 : value.hashCode());
        }
        return hash;
    }

    private HashMap<Integer, List<Object[]>> hashMap = new HashMap<>();
    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PJoin");
        /* Write your code here */
        if (this.left instanceof PRel) {
            ((PRel)this.left).open();
        }
        if (this.right instanceof PRel) {
            ((PRel)this.right).open();
        }
        List<RexInputRef> joinAttributes = getJoinAttributes(condition);
        for (int i = 0; ((PRel) this.left).hasNext(); i++) {
            Object[] row = ((PRel) this.left).next();

            int hashKey = hashFunction(row, joinAttributes);
            List<Object[]> bucket = hashMap.getOrDefault(hashKey, new ArrayList<>());
            bucket.add(row);
            hashMap.put(hashKey, bucket);
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        /* Write your code here */
        ((PRel) this.left).close();
        ((PRel) this.right).close(); 
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        /* Write your code here */
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        /* Write your code here */
        return null;
    }
}
