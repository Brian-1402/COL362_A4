package rel;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;


// import org.apache.calcite.rex.RexCall;
// import org.apache.calcite.rex.RexInputRef;
// import org.apache.calcite.sql.SqlKind;

import convention.PConvention;

// import java.math.BigDecimal;
import org.apache.calcite.rex.RexLiteral;



public class PSort extends Sort implements PRel{
    
    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints, // List of hints -> Hints are used to provide additional information to the planner to make better decisions
            RelNode child, // Input relational expression
            RelCollation collation, // Relational expression's collation -> Collation is the ordering of rows in a relational expression
            RexNode offset, // RexNode offset -> Offset is the number of rows to skip before returning a row
            RexNode fetch // RexNode fetch -> Fetch is the number of rows to return after the offset
            ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }

    private List<Object[]> sortedRows = new ArrayList<>();
    private int int_offset = (Integer) ((RexLiteral) this.offset).getValue();
    private int int_fetch = (Integer) ((RexLiteral) this.fetch).getValue();

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        PRel child = (PRel) this.getInput();
        if (child.open()) {
            while (child.hasNext()) {
                sortedRows.add(child.next());
            }
            sortedRows.sort((a, b) -> {
                for (int i = 0; i < this.collation.getFieldCollations().size(); i++) {
                    int index = this.collation.getFieldCollations().get(i).getFieldIndex();
                    Object val1 = a[index]; //! Mi ght need to evaluate the expression here
                    Object val2 = b[index]; //! Might need to evaluate the expression here
                    int compare_val = 0;

                    if (val1 instanceof Integer) {
                        compare_val = ((Integer) val1).compareTo((Integer) val2); // compareTo() would return -1 if val1 < val2, 0 if val1 == val2, 1 if val1 > val2
                    } else if (val1 instanceof String) {
                        compare_val = ((String) val1).compareTo((String) val2);
                    } else if (val1 instanceof Double) {
                        compare_val = ((Double) val1).compareTo((Double) val2);
                    } else if (val1 instanceof Float) {
                        compare_val = ((Float) val1).compareTo((Float) val2);
                    }
                    
                    if (this.collation.getFieldCollations().get(i).direction.isDescending()) {
                        compare_val = -compare_val;
                    }

                    if (compare_val != 0) {
                        return compare_val;
                    }
                }
                return 0;
            });
            sortedRows = sortedRows.subList(int_offset, Math.min(int_offset + int_fetch, sortedRows.size()));
            return true;
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        PRel child = (PRel) this.getInput();
        child.close();
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        if (sortedRows.size() > 0) {
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        if (sortedRows.size() > 0) {
            return sortedRows.remove(0);
        }
        return null;
    }

}
