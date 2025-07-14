package simpledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // Create fields as defined for the constructor below
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    // I did StringAggregator first so look there for more info on my thoughts, but, we need a TupleDesc, and two hashmaps,
    // one for storing counts for each group for each agg besides AVG, and one for avg that maps field to sum AND count

    private TupleDesc td;
    private HashMap<Field, Integer> count;
    private HashMap<Field, int[]> avgMap;
    // So in order to calculate the average, we need the sum AND the count, which is why we put them in an array

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.count = new HashMap<>();
        this.avgMap = new HashMap<>();

        if (gbfield == NO_GROUPING) { // or check if gbfieldtype == null
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
            // To represent just holding an int with not grouping used
        } else {
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            // As we know according to the javadoc for iterator, the group type comes first
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        // So why do we need to check this again here too, didnt we already check this in the constructor?
        // Well yes, but that was just a general pre-processing step to represent a tuple that would come out of this
        // aggregate, but we need to check it again for each merged tuple
        Field temp;
        if (gbfield == NO_GROUPING) {
            // the gbfield from the top of this class
            temp = null;
        } else {
            temp = tup.getField(gbfield);
        }

        // Before we figure out what aggregate operator we have, first bring out the actual aggregate values
        // because we want/need to use them in our switch statements!
        IntField aggField = (IntField) tup.getField(afield);
        // Remember that afield and all of those params above store the values of the 'other' tuple!!
        int value = aggField.getValue();

        switch (this.what) {
            // For every case, check whether or not the value exists in the hashmap, if not add it, with some details
            // added in for each one
            case MIN:
                if (count.containsKey(temp)) {
                    count.put(temp, Math.min(count.get(temp), value));
                    // Max vs min
                } else {
                    count.put(temp, value);
                }
                break;

            case MAX:
                if (count.containsKey(temp)) {
                    count.put(temp, Math.max(count.get(temp), value));
                    // Min vs max
                } else {
                    count.put(temp, value);
                }
                break;

            case SUM:
                if (count.containsKey(temp)) {
                    count.put(temp, count.get(temp) + value);
                    // Rolling sum
                } else {
                    count.put(temp, value);
                }
                break;

            case AVG:
                if (avgMap.containsKey(temp)) {
                    int[] old = avgMap.get(temp);
                    avgMap.put(temp, new int[]{old[0] + value, old[1] + 1});
                    // Update two values distinctly
                } else {
                    avgMap.put(temp, new int[]{value, 1});
                }
                break;

            case COUNT:
                if (count.containsKey(temp)) {
                    count.put(temp, count.get(temp) + 1);
                    // Update through incrementation
                } else {
                    count.put(temp, 1);
                }
                break;
        } // Seems like alot, and was not easy to come up with, but not too bad!
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private ArrayList<Tuple> list;
            // We create a list of tuples to iterate over in
            private int index;
            // Where we are currently

            @Override
            public void open() throws DbException, TransactionAbortedException {
                list = new ArrayList<>();
                // First divide by grouping, then divide by AVG
                Tuple cur = new Tuple(td);

                if (gbfield == NO_GROUPING) {
                    // Then split with avg
                    if (what == Op.AVG) {
                        cur = new Tuple(td);
                        int avg = avgMap.get(null)[0] / avgMap.get(null)[1];
                        // This is why we had a separate map!!

                        cur.setField(0, new IntField(avg));
                        // Since it needs a field!
                    } else {
                        cur = new Tuple(td);
                        int mapValue = count.get(null);
                        cur.setField(0, new IntField(mapValue));
                        // Can set the value directly
                    }
                    list.add(cur);
                } else {
                    // HOLY SHIT BEFORE MY FOR LOOP SURROUNDED BOTH LOOPS SO IN THE AVG CASE IT WOULD USE KEYS FROM
                    // COUNT.KEYSET() HOLY SHIT I WAS GOING TO DEFENSTRATE MYSELF
                    if (what == Op.AVG) {
                        for (Field f : avgMap.keySet()) {
                            // DISTINCT LOOPSSSSSSSSSSSSSSSSSSSSSSS
                            cur = new Tuple(td);
                            int[] sumCount = avgMap.get(f);
                            int avg = sumCount[0] / sumCount[1];

                            // Sent new fields properly since both group and val
                            cur.setField(0, f);
                            cur.setField(1, new IntField(avg));
                            list.add(cur);
                        }
                    } else {
                        for (Field f : count.keySet()) {
                            // REMEMBER, distinct loops!!!!!!!!!!!!!!!!!!!!!
                            cur = new Tuple(td);
                            cur.setField(0, f);
                            cur.setField(1, new IntField(count.get(f)));
                            list.add(cur);
                        }
                    }
                }
                this.index = 0;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return index < list.size();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    // Can actually just use hasNext instead of manually checking!
                    throw new NoSuchElementException("We are already at the end of the list");
                } else {
                    // return list.get(index++);
                    // Which will nicely increment index as well
                    index++;
                    return list.get(index - 1);
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                index = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                list = null;
                index = 0;
            }
        };
    }

}