package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    // Pretty much just copy the fields from insert, except for tableid
    private TransactionId t;
    private OpIterator child;
    private TupleDesc td;
    private int count;
    private boolean ran;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        // Pretty much copied from insert
        this.t = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.count = 0;
        this.ran = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        this.count = 0;
        this.ran = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.count = 0;
        this.ran = false;
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
        // Yep just copied from insert

        if (ran) return null;
        ran = true;
        // "A 1-field tuple containing the number of inserted records, or null if called more than once."

        Tuple toReturn = new Tuple(this.td);

        try {
            while (child.hasNext()) {
                Tuple nextTuple = child.next();

                /// For some truly bizarre reason, we need to try/catch to get the i/o exception  here
                try {
                    Database.getBufferPool().deleteTuple(this.t, nextTuple);
                    count++;
                } catch (TransactionAbortedException e) {
                    throw e;
                } catch (IOException e) {
                    throw new DbException("I/O error when deleting tuple");
                }
            }
        } catch (TransactionAbortedException e) {
            throw e;
        }


        toReturn.setField(0, new IntField(count));
        // "It returns a one field tuple containing the number of inserted records."
        return toReturn;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

}
