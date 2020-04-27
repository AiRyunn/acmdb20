package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final DbIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;

    private final boolean noGrouping;
    private final Aggregator aggregator;
    private DbIterator aggIter;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        this.noGrouping = gfield == Aggregator.NO_GROUPING;
        Type afieldtype = child.getTupleDesc().getFieldType(afield);
        Type gfieldtype = noGrouping ? null : child.getTupleDesc().getFieldType(gfield);

        switch (afieldtype) {
            case INT_TYPE:
                this.aggregator = new IntegerAggregator(gfield, gfieldtype, afield, aop);
                break;
            case STRING_TYPE:
                this.aggregator = new StringAggregator(gfield, gfieldtype, afield, aop);
                break;
            default:
                this.aggregator = null;
                assert false;
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        // some code goes here
        if (this.noGrouping) {
            return null;
        } else {
            return this.child.getTupleDesc().getFieldName(this.gfield);
        }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // some code goes here
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // some code goes here
        return this.child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        for (; this.child.hasNext();) {
            Tuple t = this.child.next();
            this.aggregator.mergeTupleIntoGroup(t);
        }
        this.child.close();

        this.aggIter = this.aggregator.iterator();
        this.aggIter.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!this.aggIter.hasNext()) {
            return null;
        }
        return this.aggIter.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.aggIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.aggregator.getTupleDesc();
    }

    public void close() {
        // some code goes here
        this.aggIter.close();
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.aggIter };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.aggIter = children[0];
    }

}
