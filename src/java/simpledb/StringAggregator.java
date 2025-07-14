package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // Fields as defined by constructor below
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    // However we also need additional fields beyond what was given in the constructor for merge and iterator
    // respectively -> We need some way to store a count for each group, as well as a TupleDesc for the tuples returned
    // by the iterator
    private HashMap<Field, Integer> count;
    private TupleDesc td;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Can only use the count aggregate on a String Iterator");
        }

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        // The extra fields we have
        this.count = new HashMap<>();

        if (gbfield == NO_GROUPING)  { // or check if gbfieldtype == null
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
            // To represent just holding an int with not grouping used
        } else {
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            // As we know according to the javadoc for iterator, the group type comes first
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = null;
        if (gbfield != NO_GROUPING) {
            key = tup.getField(gbfield);
        }

        // Increment if it exists, or initialize and then increment as before
        count.put(key, count.getOrDefault(key, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        // The typical design for how I made these iterators
        return new OpIterator() {
            private ArrayList<Tuple> list;
            // We create a list of tuples to iterate over in
            private int index;
            // Where we are currently

            @Override
            public void open() throws DbException, TransactionAbortedException {
                list = new ArrayList<>();

                if (gbfield == NO_GROUPING) { // Similar conditional idea to above
                    // So now create a tuple and feed it into our list, no need to get anything from our map because
                    // as the javadoc states, there is just one value, which means we dont need to loop through to get
                    // a key/value pair (plus the key is null anyways)
                    Tuple temp = new Tuple(td);
                    temp.setField(0, new IntField(count.get(null)));
                    // Because its the only field present, so we know what index to set it at. We also use count.get(null)
                    // to get our value from the hashmap, since we should only get one because this is not grouping
                    list.add(temp);
                } else {
                    for (Field f : count.keySet()){
                        Tuple temp = new Tuple(td);

                        // Now we have to set both fields because we grouped by
                        temp.setField(0, f);
                        temp.setField(1, new IntField(count.get(f)));
                        // Remember since fields were our keys

                        list.add(temp);
                    }
                }
                this.index = 0;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return index < list.size();
                // Since index is just a regular old for loop style counter that goes through our list
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    // Can actually just use hasNext instead of manually checking!
                    throw new NoSuchElementException("We are already at the end of the list");
                } else {
                    index++;
                    return list.get(index - 1);
                    // Which will nicely increment index as well
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                index = 0;
                // It makes sense that this is the only piece of rewind as opposed to close
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
                // The td defined in this implementation of iterator, not the other one!!!!
            }

            @Override
            public void close() {
                list = null;
                index = 0;
            }
        };
    }

}
