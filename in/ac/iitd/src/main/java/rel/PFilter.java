package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;

import convention.PConvention;

import java.math.BigDecimal;
import org.apache.calcite.rex.RexLiteral;



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
        PRel input = (PRel) this.input;
        return input.open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        PRel input = (PRel) this.input;
        input.close();
    }

    private Object BooleanOperation(RexCall call, Object[] inputRow, String type) {
        logger.trace("Handling boolean operation in PProject");
    
        Object operand1 = handleExpression(call.getOperands().get(0), inputRow, type);
        Object operand2 = handleExpression(call.getOperands().get(1), inputRow, type);
        
        String operandType = call.getOperands().get(0).getType().getSqlTypeName().getName();
        if (type == "DECIMAL") {
            operandType = "DECIMAL";
        }

        switch (call.getKind()) {
            case AND:
                return (Boolean) operand1 && (Boolean) operand2;
            case OR:
                return (Boolean) operand1 || (Boolean) operand2;
            case NOT:
                return !(Boolean) operand1;
            case EQUALS:
                return operand1.equals(operand2);
            case NOT_EQUALS:
                return !operand1.equals(operand2);
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return compareOperands(call.getKind(), operandType, operand1, operand2);
            default:
                throw new UnsupportedOperationException("Unsupported operation in PProject.BooleanOperation");
        }
    }
    
    private boolean compareOperands(SqlKind kind, String operandType, Object operand1, Object operand2) {
        // If operandType is BIGDECIMAL then convert it to DOUBLE
        if (operandType.equals("DECIMAL")) {
            operand1 = ((Number) operand1).doubleValue();
            operand2 = ((Number) operand2).doubleValue();
            operandType = "DOUBLE";
        }

        switch (operandType) {
            case "VARCHAR":
                int result = ((String) operand1).compareTo((String) operand2);
                return compareResult(kind, result);
            case "INTEGER":
                return compareResult(kind, Integer.compare(((Number) operand1).intValue(),((Number) operand2).intValue()));
            case "FLOAT":
                return compareResult(kind, Float.compare(((Number) operand1).floatValue(), ((Number) operand2).floatValue()));
            case "DOUBLE":
                return compareResult(kind, Double.compare(((Number) operand1).doubleValue(), ((Number) operand2).doubleValue()));
            default:
                throw new UnsupportedOperationException("Unsupported data type in PProject.compareOperands");
        }
    }
    
    private boolean compareResult(SqlKind kind, int result) {
        switch (kind) {
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

    private Object ArithmeticOperation(RexCall call, Object[] inputRow, String type) {
        logger.trace("Handling arithmetic operation in PProject");
    
        Object operand1 = handleExpression(call.getOperands().get(0), inputRow, type);
        Object operand2 = handleExpression(call.getOperands().get(1), inputRow, type);

        String operandType = call.getOperands().get(0).getType().getSqlTypeName().getName();
        if (type == "DECIMAL") {
            operandType = "DECIMAL";
        }

        switch (call.getKind()) {
            case PLUS:
            case MINUS:
            case TIMES:
            case DIVIDE:
            case MOD:
                return performArithmeticOperation(call.getKind(), operandType, operand1, operand2);
            default:
                throw new UnsupportedOperationException("Unsupported operation in PProject.ArithmeticOperation");
        }
    }
    
    private Object performArithmeticOperation(SqlKind kind, String operandType, Object operand1, Object operand2) {
        // If operandType is BIGDECIMAL then convert it to DOUBLE
        if (operandType.equals("DECIMAL")) {
            operand1 = ((Number) operand1).doubleValue();
            operand2 = ((Number) operand2).doubleValue();
            operandType = "DOUBLE";
        }

        switch (operandType) {
            case "INTEGER":
                return performIntegerOperation(kind, ((Number) operand1).intValue(), ((Number) operand2).intValue());
            case "FLOAT":
                return performFloatOperation(kind, ((Number) operand1).floatValue(), ((Number) operand2).floatValue());
            case "DOUBLE":
                return performDoubleOperation(kind, ((Number) operand1).doubleValue(), ((Number) operand2).doubleValue());
            case "VARCHAR":
                if (kind == SqlKind.PLUS) {
                    return (String) operand1 + (String) operand2;
                } else {
                    throw new UnsupportedOperationException("Unsupported operation for VARCHAR in PProject.performArithmeticOperation");
                }
            default:
                throw new UnsupportedOperationException("Unsupported data type in PProject.performArithmeticOperation");
        }
    }

    
    private Integer performIntegerOperation(SqlKind kind, Integer operand1, Integer operand2) {
        switch (kind) {
            case PLUS:
                return operand1 + operand2;
            case MINUS:
                return operand1 - operand2;
            case TIMES:
                return operand1 * operand2;
            case DIVIDE:
                return operand1 / operand2;
            case MOD:
                return operand1 % operand2;
            default:
                return null;
        }
    }
    
    private Float performFloatOperation(SqlKind kind, Float operand1, Float operand2) {
        switch (kind) {
            case PLUS:
                return operand1 + operand2;
            case MINUS:
                return operand1 - operand2;
            case TIMES:
                return operand1 * operand2;
            case DIVIDE:
                return operand1 / operand2;
            case MOD:
                return operand1 % operand2;
            default:
                return null;
        }
    }
    
    private Double performDoubleOperation(SqlKind kind, Double operand1, Double operand2) {
        switch (kind) {
            case PLUS:
                return operand1 + operand2;
            case MINUS:
                return operand1 - operand2;
            case TIMES:
                return operand1 * operand2;
            case DIVIDE:
                return operand1 / operand2;
            case MOD:
                return operand1 % operand2;
            default:
                return null;
        }
    }


    private Object Operation(RexCall call, Object[] inputRow, String type){
        logger.trace("Handling arithmetic operation in PProject");
        // Hint: Use the inputRow to evaluate the expression
        // Operation - Logical or Arithmetic

        switch (call.getKind()) {
            case AND:
            case OR:
            case NOT:
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return BooleanOperation(call, inputRow, type);
            case PLUS:
            case MINUS:
            case TIMES:
            case DIVIDE:
            case MOD:
                return ArithmeticOperation(call, inputRow, type);
            default:
                throw new UnsupportedOperationException("Unsupported operation in PProject.Operation");
        }
    }
    // Handle expression cases modularly
    private Object handleExpression(RexNode expr, Object[] inputRow, String type){
        logger.trace("Handling expression in PProject");
        // The expression expr could be a Rexcall or a RexInputRef
        // If it is a RexCall, it could be an arithmetic operation or a function call not necessarily always between 2 columns (eg. could be A+B+C*D, etc.)
        // Hint: Use the inputRow to evaluate the expression
        // If it is a RexInputRef, it is a column reference
        
        if (expr instanceof RexCall) {
            return Operation((RexCall) expr, inputRow, type);
        } 
        else if (expr instanceof RexInputRef){
            if (type.equals("DECIMAL")) {
                // Type cast whatever value it is to BigDecimal if it is of type DECIMAL
                Object value = inputRow[((RexInputRef) expr).getIndex()];
                if (value instanceof Number) {
                    return BigDecimal.valueOf(((Number) value).doubleValue());
                } else {
                    throw new UnsupportedOperationException("Unsupported value type in PProject.handleExpression for DECIMAL");
                }
            }
            return inputRow[((RexInputRef) expr).getIndex()];
        }
        else if (expr instanceof RexLiteral){
            if (type.equals("DECIMAL")) {
                return ((RexLiteral) expr).getValueAs(BigDecimal.class); // returns a BigDecimal
            }
            return ((RexLiteral) expr).getValue(); // returns a String
        }
        else {
            throw new UnsupportedOperationException("Unsupported expression type in PProject.handleExpression");
        }
    }

    private Object[] returnRow = null;

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PFilter has next");
        // Set the return row while checking if hasNext is true
        // Hint: Use the input row to evaluate the filter condition

        PRel input = (PRel) this.input;
        // While checking hasnext itself we'll assingn the returnrow variable and return it in next
        for (Object[] row = input.next(); input.hasNext(); row = input.next()){
            Object condition = handleExpression(this.condition, row, "BOOLEAN");
            if ((Boolean) condition){
                returnRow = row;
                return true;
            }
        }
        return false;
    }

    // returns the next row
    // Hint: Try looking at different possible filter conditions
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PFilter");
        return returnRow;
    }
}
