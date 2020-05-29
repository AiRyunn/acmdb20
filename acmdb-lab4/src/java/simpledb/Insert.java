package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private DbIterator child;
    private final int tableId;

    private boolean inserted;
    private final TupleDesc td;

    /**
     * Constructor.
     *
     * @param tid
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId tid, DbIterator child, int tableId) throws DbException {
        // some code goes here
        this.tid = tid;
        this.child = child;
        this.tableId = tableId;

        String[] names = new String[] { "inserted" };
        Type[] types = new Type[] { Type.INT_TYPE };
        this.td = new TupleDesc(types, names);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        super.open();

        this.inserted = false;
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.inserted) {
            return null;
        }
        this.inserted = true;

        int count = 0;
        for (; this.child.hasNext();) {
            count++;
            Tuple t = this.child.next();
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, t);
            } catch (Exception e) {
                throw new DbException("Exception on insertion");
            }
        }

        Tuple resultTuple = new Tuple(this.td);
        resultTuple.setField(0, new IntField(count));

        return resultTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
