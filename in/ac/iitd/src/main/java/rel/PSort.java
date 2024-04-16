package rel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

public class PSort extends Sort implements PRel{
    
    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset, // number of rows to skip
            RexNode fetch // number of rows being returned in total
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
    private List<Object[]> sortedData = new ArrayList<>();
    private int int_offset = (Integer) ((RexLiteral) this.offset).getValue();
    private int int_fetch = (Integer) ((RexLiteral) this.fetch).getValue();

    // returns true if successfully opened, false otherwise
    // In a loop, read all inputs using the input's next() method and store them in sortedData
    // Keep reading until hasNext() returns false
    // Then sort the data in sortedData
    // Remove the top offset rows from the sorted data
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        /* Write your code here */
        if (this.input instanceof PRel) {
            ((PRel)this.input).open();

            // Read all inputs using the input's next() method and store them in sortedData
            for (int i = 0; ((PRel) this.input).hasNext(); i++) {
                sortedData.add(((PRel) this.input).next());
            }

            final RelCollation sort_collation = this.collation;
            sortedData.sort(new Comparator<Object[]>() {
                @Override
                public int compare(Object[] row1, Object[] row2) {
                    for (RelFieldCollation fieldCollation : sort_collation.getFieldCollations()) {
                        int fieldIndex = fieldCollation.getFieldIndex();
                        Comparable value1 = (Comparable) row1[fieldIndex];
                        Comparable value2 = (Comparable) row2[fieldIndex];
                        int comparison = value1.compareTo(value2);
                        if (comparison != 0) {
                            return fieldCollation.getDirection() == RelFieldCollation.Direction.ASCENDING ? comparison : -comparison;
                        }
                    }
                    return 0;
                }
            });
            sortedData = sortedData.subList(int_offset, int_offset + int_fetch);
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        /* Write your code here */
        ((PRel)this.input).close();
        return;
    }

    // returns true if there is a next row, false otherwise
    // Check if there are entries remaining in sortedData
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        /* Write your code here */
        if (sortedData.size() > 0) {
            return true;
        }
        return false;
    }

    // returns the next row
    // pops the first element from sortedData and returns it
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        /* Write your code here */
        if (sortedData.size() > 0) {
            return sortedData.remove(0);
        }

        return null;
    }

}
