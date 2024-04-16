package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import convention.PConvention;

import java.util.ArrayList;
import java.util.List;

public class PFilter extends Filter implements PRel {

    public PFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RexNode condition) {
        super(cluster, traits, child, condition);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PFilter");
        /* Write your code here */
        if (this.input instanceof PRel) {
            return ((PRel) this.input).open();
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        /* Write your code here */
        if (this.input instanceof PRel) {
            ((PRel) this.input).close();
        }
        return;
    }

    private Object evalRexCallArithmetic(RexCall call, Object[] row) {
        // Evaluate the operands
        List<Object> operands = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            operands.add(evalRexNode(operand, row));
        }
        Object a = operands.get(0);
        Object b = operands.get(1);
        // print the types of a and b
        // logger.trace("Type of a: " + a.getClass().getName());
        // Evaluate the call
        // considers different datatypes too, such as integers, float, etc.
        // Assumes both operands are of the same type
        switch (call.getKind()) {
            
            case PLUS:
                if (a instanceof Integer) {
                    return ((Number) a).intValue() + ((Number) b).intValue();
                } else if (a instanceof Double) {
                    return ((Number) a).doubleValue() + ((Number) b).doubleValue();
                } else if (a instanceof Float) {
                    return ((Number) a).floatValue() + ((Number) b).floatValue();
                } else if (a instanceof Long) {
                    return ((Number) a).longValue() + ((Number) b).longValue();
                } else {
                    return null;
                }
            case MINUS:
                if (a instanceof Integer) {
                    return ((Number) a).intValue() - ((Number) b).intValue();
                } else if (a instanceof Double) {
                    return ((Number) a).doubleValue() - ((Number) b).doubleValue();
                } else if (a instanceof Float) {
                    return ((Number) a).floatValue() - ((Number) b).floatValue();
                } else if (a instanceof Long) {
                    return ((Number) a).longValue() - ((Number) b).longValue();
                } else {
                    return null;
                }
            case TIMES:
                if (a instanceof Integer) {
                    return ((Number) a).intValue() * ((Number) b).intValue();
                } else if (a instanceof Double) {
                    return ((Number) a).doubleValue() * ((Number) b).doubleValue();
                } else if (a instanceof Float) {
                    return ((Number) a).floatValue() * ((Number) b).floatValue();
                } else if (a instanceof Long) {
                    return ((Number) a).longValue() * ((Number) b).longValue();
                } else {
                    throw new RuntimeException("Unsupported type" + a.getClass().getName() + " for TIMES operation");
                }
            case DIVIDE:
                if (a instanceof Integer) {
                    return ((Number) a).intValue() / ((Number) b).intValue();
                } else if (a instanceof Double) {
                    return ((Number) a).doubleValue() / ((Number) b).doubleValue();
                } else if (a instanceof Float) {
                    return ((Number) a).floatValue() / ((Number) b).floatValue();
                } else if (a instanceof Long) {
                    return ((Number) a).longValue() / ((Number) b).longValue();
                } else {
                    return null;
                }
            case MOD:
                if (a instanceof Integer) {
                    return ((Number) a).intValue() % ((Number) b).intValue();
                } else if (a instanceof Double) {
                    return ((Number) a).doubleValue() % ((Number) b).doubleValue();
                } else if (a instanceof Float) {
                    return ((Number) a).floatValue() % ((Number) b).floatValue();
                } else if (a instanceof Long) {
                    return ((Number) a).longValue() % ((Number) b).longValue();
                } else {
                    return null;
                }
            default:
                return null;
        }
    }
    
    private boolean evalRexCallBool(RexCall call, Object[] row) {
        // Evaluate the operands
        List<Object> operands = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            operands.add(evalRexNode(operand, row));
        }
        Object a = operands.get(0);
        Object b = operands.get(1);
        // Evaluate the call
        switch (call.getKind()) {
            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
                return a.equals(b);
            case GREATER_THAN:
                return ((Number) a).doubleValue() > ((Number) b).doubleValue();
            case LESS_THAN:
                return ((Number) a).doubleValue() < ((Number) b).doubleValue();
            case AND:
                return (Boolean) a && (Boolean) b;
            case OR:
                return (Boolean) a || (Boolean) b;
            case NOT:
            case IS_FALSE:
            case IS_NOT_TRUE:
                return !(Boolean) a;
            case IS_NULL:
            case IS_UNKNOWN:
                return a == null;
            case IS_NOT_NULL:
                return a != null;
            case IS_TRUE:
            case IS_NOT_FALSE:
                return (Boolean) a;
            case IS_DISTINCT_FROM:
                return !a.equals(b);
            default:
                return false;
        }
    }
    
    private Object evalRexCall(RexCall call, Object[] row) {
        if (call.getKind() == SqlKind.PLUS || call.getKind() == SqlKind.MINUS || call.getKind() == SqlKind.TIMES || call.getKind() == SqlKind.DIVIDE || call.getKind() == SqlKind.MOD) {
            return evalRexCallArithmetic(call, row);
        } else if (call.getKind() == SqlKind.EQUALS || call.getKind() == SqlKind.GREATER_THAN || call.getKind() == SqlKind.LESS_THAN || call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR || call.getKind() == SqlKind.NOT){
            return evalRexCallBool(call, row);
        }
        return null;
    }
    

    // Evaluate a RexNode
    private Object evalRexNode(RexNode node, Object[] row) {
        if (node instanceof RexCall) {
            return evalRexCall((RexCall) node, row);
        } else if (node instanceof RexLiteral) {
            return ((RexLiteral) node).getValue();
        } else if (node instanceof RexInputRef) {
            return row[((RexInputRef) node).getIndex()];
        } else {
            return null;
        }
    }
    
    private Object[] currentRow = null;

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PFilter has next");
        /* Write your code here */
        if (this.input instanceof PRel) {
            for (Object[] row = ((PRel) this.input).next(); ((PRel) this.input).hasNext(); row = ((PRel) this.input).next()) {
                // Evaluate the condition
                Object result = evalRexNode(condition, row);
                // Check the result
                if (result instanceof Boolean && (Boolean) result) {
                    // The condition is true, return the row
                    currentRow = row;
                    return true;
                }
            }
        }
        currentRow = null;
        return false;
    }

    // returns the next row
    // Hint: Try looking at different possible filter conditions
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PFilter");
        /* Write your code here */
        if (this.input instanceof PRel) {
            return currentRow;
        }
        return null;
    }
}
