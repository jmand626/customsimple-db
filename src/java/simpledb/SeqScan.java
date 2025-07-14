package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    // Our first three fields come from the constructor
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator it;
    // We need to somehow iterate over the files, and therefore pages, and therefore tuples....

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        // remember that we can always access the static database instance, and the catalog is easy to get
        // from there
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        // Basically like an copy constructor from c++
    }

    // Secondary constructor if needed
    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    // Open is an iterator method, so we open that here
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        DbFile f = Database.getCatalog().getDatabaseFile(tableid);

        // Remember, it is our iterator from above (its a field)
        it = f.iterator(tid);
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        // This code should look familiar from TupleDesc:

        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);

        // it have been a while, but remember that we need these two arrays for the desc constructor
        Type[] typeAr = new Type[td.numFields()];
        String[] fieldAr = new String[td.numFields()];

        for (int i = 0; i < td.numFields(); i++) {
            typeAr[i] = td.getFieldType(i);

            // Right since we want to return the same tupledesc back, but attach the table name to the front
            fieldAr[i] = tableAlias + "." + td.getFieldName(i);
        }

        TupleDesc toReturn = new TupleDesc(typeAr, fieldAr);
        return toReturn;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it == null) return false;
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (it == null || !it.hasNext()) {
            throw new NoSuchElementException("There is no next element");
        }
        // With this and hasNext previously, at our point where this iterator is an iterator
        // of iterators (waytoodank), we just let the smaller iterator do its job
        return it.next();
    }

    public void close() {
        // some code goes here
        if (it !=null) {
            // Unforunatetely, you do have check that you did not already close the iterator before you close it

            it.close();
            it = null;
        }
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (it != null) {
            it.rewind();
            // Similar to next and hasNext, let the smaller iterator do its job
        }
    }
}
