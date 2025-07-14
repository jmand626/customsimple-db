package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    // So obviously we need at least these fields from the constructor
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    // But of course there is a message in the javadoc of the constructor! Based on afield, use either type of
    // aggregator from hell we previously did. Since both implement Aggregator, we can just have that
    private Aggregator agg;

    // You would probably expect this at some point, and we use it here for the tupleDesc methods
    private TupleDesc td;

    // Finally we need another OpIterator, but its not obvious why! The reason is that because we get tuples from child
    // so sending tuples back there makes no sense, and by keeping the aggregation and tuple outputting seperate, we
    // have less nasty bugs that might warrant self-defenestration
    private OpIterator outputIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        // Choose which aggregator to extend to based on what we need
        // First want to see if we even need gtype
        Type gtype = null;
        if (gfield != Aggregator.NO_GROUPING) {
            gtype = child.getTupleDesc().getFieldType(gfield);
        }

        // We call getFieldType, not getFieldName here!
        Type atype = child.getTupleDesc().getFieldType(afield);

        if (atype == Type.INT_TYPE) {
            // Remember, atype is the type of the aggregate field!
            this.agg = new IntegerAggregator(gfield, gtype, afield, aop);
        } else {
            this.agg = new StringAggregator(gfield, gtype, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        if (gfield == Aggregator.NO_GROUPING) {
            return Aggregator.NO_GROUPING;
            // Both cases are super easy to do, just lok at comment above
        } else {
            return gfield;
        }
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (gfield == Aggregator.NO_GROUPING) {
            return null;
        }
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
        // We need to dig back all the way back to TupleDesc to find a way to get a field name from a field/index
        // for some stupid reason
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        while (child.hasNext()) {
            // Depending on our aggregate, merge the next thing we get
            agg.mergeTupleIntoGroup(child.next());
        }
        outputIterator = agg.iterator();
        // Whole point of outputIterator, so agg does not have to iterate itself!!

        // Continue on for next tuples
        outputIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (outputIterator.hasNext()) {
            // Guess who clearly thought about and wrote outputIterator.next but just assume I did not need to
            // do that for hasNext :|

            return outputIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        outputIterator.rewind();
        // Nothing else needs to be rewinded because it does not make sense that if we want to go over a list of tuples
        // again that we would have to delete everything else
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (gfield != Aggregator.NO_GROUPING) {
            // Its been a very long since we have seen stuff like this, but type and name arrays!!!!!!!!!
            Type[] typeArray = new Type[]{child.getTupleDesc().getFieldType(gfield), Type.INT_TYPE};
            // Similar to all of our hellish aggregators, the grouping name and then the type together in this order

            String[] nameArray = new String[]{child.getTupleDesc().getFieldName(gfield), nameOfAggregatorOp(aop)
                    + "(" + child.getTupleDesc().getFieldName(afield) + ")"};
            // Quite similar to below, but with the added bonus that we simply attach the group field name like so:
            // group name, op (target column name)

            td = new TupleDesc(typeArray, nameArray);
        } else {
            Type[] typeArray = new Type[]{Type.INT_TYPE};
            // Remember that these tuples only have one kind of value

            String[] nameArray = new String[]{nameOfAggregatorOp(aop) + "(" + child.getTupleDesc().getFieldName(afield) + ")"};
            // op (target column name)
            td = new TupleDesc(typeArray, nameArray);
        }
        return td;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        outputIterator.close();
        // I really hope that this order does not matter :|
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children != null) {
            this.child = children[0];
        }
    }

}
