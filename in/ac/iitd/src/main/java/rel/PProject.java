package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;

import convention.PConvention;

import java.util.ArrayList;
import java.util.List;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {

    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                            List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        /* Write your code here */
        if (this.input instanceof PRel) {
            return ((PRel) this.input).open();
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
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
                    return null;
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
                return !(Boolean) a;
            case IS_NULL:
                return a == null;
            case IS_NOT_NULL:
                return a != null;
            case IS_TRUE:
                return (Boolean) a;
            case IS_FALSE:
                return !(Boolean) a;
            case IS_NOT_TRUE:
                return !(Boolean) a;
            case IS_NOT_FALSE:
                return (Boolean) a;
            case IS_UNKNOWN:
                return a == null;
            case IS_DISTINCT_FROM:
                return !a.equals(b);
            case IS_NOT_DISTINCT_FROM:
                return a.equals(b);
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

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
        if (this.input instanceof PRel) {
            return ((PRel) this.input).hasNext();
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        /* Write your code here */
        if (this.input instanceof PRel) {
            Object[] row = ((PRel) this.input).next();
            List<Object> newRow = new ArrayList<>();
            for (RexNode project : this.exps) {
                newRow.add(evalRexNode(project, row));
            }
            return newRow.toArray();
        }

        return null;
    }
}
