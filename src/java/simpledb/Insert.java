package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    // At a base level, we of course need the three fields from the constructor
    private TransactionId t;
    private OpIterator child;
    private int tableId;

    // On top of those, we also need a TupleDesc for the getter method, and an int that counts the number of tuples
    // that we effect for either insert of delete, as explicitly called out in the readme/instructions
    private TupleDesc td;
    private int count;

    // Final field!!!!!!! In the javadoc for fetch next in the iterator, we see the following statement:
    // "A 1-field tuple containing the number of inserted records, or null if called more than once."
    // This means that we should FORCE that this operator only runs ONCE
    private boolean ran;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableId = tableId;


        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        // we need a schema for this operator, and because we are solely just affecting a straight number of tuples
        // we just make it an int.

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
        if (ran) return null;
        ran = true;
        // "A 1-field tuple containing the number of inserted records, or null if called more than once."

        Tuple toReturn = new Tuple(this.td);

        try {
            while (child.hasNext()) {
                Tuple nextTuple = child.next();

                /// For some truly bizarre reason, we need to try/catch to get the i/o exception  here
                try {
                    Database.getBufferPool().insertTuple(this.t, this.tableId, nextTuple);
                    count++;
                } catch (TransactionAbortedException e) {
                    throw e;
                } catch (IOException e) {
                    throw new DbException("I/O error when inserting tuple");
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
        // Like in project.java
    }
}
