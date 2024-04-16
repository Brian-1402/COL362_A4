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

import java.math.BigDecimal;
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


    private Object evalRexCallArithmetic(RexCall call, Object[] row, String type) {
        // Evaluate the operands
        List<Object> operands = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            operands.add(evalRexNode(operand, row, type));
        }
        Object a = operands.get(0);
        Object b = operands.get(1);
        String operandType = call.getOperands().get(0).getType().getSqlTypeName().getName();
        if (type == "DECIMAL") {
            operandType = "DOUBLE";
        }
        // System.out.println("type of a" + a.getClass().getName());
        // Evaluate the call
        // considers different datatypes too, such as integers, float, etc.
        // Assumes both operands are of the same type
        switch (call.getKind()) {
            
            case PLUS:
                if (operandType.equals("INTEGER")) {
                    return ((Number) a).intValue() + ((Number) b).intValue();
                } else if (operandType.equals("DOUBLE")) {
                    return ((Number) a).doubleValue() + ((Number) b).doubleValue();
                } else if (operandType.equals("FLOAT")) {
                    return ((Number) a).floatValue() + ((Number) b).floatValue();
                } else if (operandType.equals("LONG")) {
                    return ((Number) a).longValue() + ((Number) b).longValue();
                } else if (operandType.equals("VARCHAR") && operandType.equals("VARCHAR")) {
                    return (String) a + (String) b;
                } else {
                    return null;
                }
            case MINUS:
                if (operandType.equals("INTEGER")) {
                    return ((Number) a).intValue() - ((Number) b).intValue();
                } else if (operandType.equals("DOUBLE")) {
                    return ((Number) a).doubleValue() - ((Number) b).doubleValue();
                } else if (operandType.equals("FLOAT")) {
                    return ((Number) a).floatValue() - ((Number) b).floatValue();
                } else if (operandType.equals("LONG")) {
                    return ((Number) a).longValue() - ((Number) b).longValue();
                } else {
                    return null;
                }
            case TIMES:
                if (operandType.equals("INTEGER")) {
                    return ((Number) a).intValue() * ((Number) b).intValue();
                } else if (operandType.equals("DOUBLE")) {
                    return ((Number) a).doubleValue() * ((Number) b).doubleValue();
                } else if (operandType.equals("FLOAT")) {
                    return ((Number) a).floatValue() * ((Number) b).floatValue();
                } else if (operandType.equals("LONG")) {
                    return ((Number) a).longValue() * ((Number) b).longValue();
                } else {
                    throw new RuntimeException("Unsupported type" + a.getClass().getName() + " for TIMES operation");
                    // return null;
                }
            case DIVIDE:
                if (operandType.equals("INTEGER")) {
                    return ((Number) a).intValue() / ((Number) b).intValue();
                } else if (operandType.equals("DOUBLE")) {
                    return ((Number) a).doubleValue() / ((Number) b).doubleValue();
                } else if (operandType.equals("FLOAT")) {
                    return ((Number) a).floatValue() / ((Number) b).floatValue();
                } else if (operandType.equals("LONG")) {
                    return ((Number) a).longValue() / ((Number) b).longValue();
                } else {
                    return null;
                }
            case MOD:
                if (operandType.equals("INTEGER")) {
                    return ((Number) a).intValue() % ((Number) b).intValue();
                } else if (operandType.equals("DOUBLE")) {
                    return ((Number) a).doubleValue() % ((Number) b).doubleValue();
                } else if (operandType.equals("FLOAT")) {
                    return ((Number) a).floatValue() % ((Number) b).floatValue();
                } else if (operandType.equals("LONG")) {
                    return ((Number) a).longValue() % ((Number) b).longValue();
                } else {
                    return null;
                }
            default:
                return null;
        }
    }

    private boolean compareResult(SqlKind kind, int result) {
        switch (kind) {
            case EQUALS:
                return result == 0;
            case NOT_EQUALS:
                return result != 0;
            case GREATER_THAN:
                return result > 0;
            case GREATER_THAN_OR_EQUAL:
                return result >= 0;
            case LESS_THAN:
                return result < 0;
            case LESS_THAN_OR_EQUAL:
                return result <= 0;
            default:
                return false;
        }
    }

    private boolean evalRexCallBool(RexCall call, Object[] row, String type) {
        // Evaluate the operands
        List<Object> operands = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            operands.add(evalRexNode(operand, row, type));
        }
        Object a = operands.get(0);
        Object b = operands.get(1);
        String operandType = call.getOperands().get(0).getType().getSqlTypeName().getName();
        if (type == "DECIMAL") {
            operandType = "DECIMAL";
        }
        // Evaluate the call
        switch (call.getKind()) {

            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                if (operandType.equals("INTEGER") || operandType.equals("DECIMAL") || operandType.equals("DOUBLE") || operandType.equals("FLOAT") || operandType.equals("LONG") ){
                    return compareResult(call.getKind(), Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()));
                } else if (operandType.equals("VARCHAR")){
                    return compareResult(call.getKind(), ((String) a).compareTo((String) b));
                }

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
            case IS_UNKNOWN:
                return a == null;
            case IS_TRUE:
                return (Boolean) a;
            case IS_FALSE:
                return !(Boolean) a;
            case IS_NOT_TRUE:
                return !(Boolean) a;
            case IS_NOT_FALSE:
                return (Boolean) a;
            case IS_DISTINCT_FROM:
                return !a.equals(b);
            case IS_NOT_DISTINCT_FROM:
                return a.equals(b);
            default:
                return false;
        }
    }
    
    private Object evalRexCall(RexCall call, Object[] row, String type) {
        if (call.getKind() == SqlKind.PLUS || call.getKind() == SqlKind.MINUS || call.getKind() == SqlKind.TIMES || call.getKind() == SqlKind.DIVIDE || call.getKind() == SqlKind.MOD) {
            return evalRexCallArithmetic(call, row, type);
        } else if (call.getKind() == SqlKind.EQUALS || call.getKind() == SqlKind.GREATER_THAN || call.getKind() == SqlKind.LESS_THAN || call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR || call.getKind() == SqlKind.NOT){
            return evalRexCallBool(call, row, type);
        }
        return null;
    }
    

    // Evaluate a RexNode
    private Object evalRexNode(RexNode node, Object[] row, String type) {
        if (node instanceof RexCall) {
            return evalRexCall((RexCall) node, row, type);
        } else if (node instanceof RexLiteral) {
            if (type.equals("DECIMAL")) {
                return ((RexLiteral) node).getValueAs(BigDecimal.class); // returns a BigDecimal
            } else {
                return ((RexLiteral) node).getValue();
            }
        } else if (node instanceof RexInputRef) {
            if (type.equals("DECIMAL")) {
                Object num = row[((RexInputRef) node).getIndex()];
                if (num instanceof Number) {
                    return BigDecimal.valueOf(((Number) num).doubleValue());
                }
            }
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
                Object result = evalRexNode(condition, row, "BOOLEAN");
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
