package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private DbIterator child;

    private boolean deleted;
    private final TupleDesc td;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param tid
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId tid, DbIterator child) {
        // some code goes here
        this.tid = tid;
        this.child = child;

        String[] names = new String[] { "deleted" };
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.deleted) {
            return null;
        }
        this.deleted = true;

        int count = 0;
        for (; this.child.hasNext();) {
            count++;
            Tuple t = this.child.next();
            try {
                Database.getBufferPool().deleteTuple(this.tid, t);
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
