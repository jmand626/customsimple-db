package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
    // So first off, we figure out what fields we want. Obviously we want the table id and iocostperpage as mentioned
    // below because of the constructor, but we will also, since our int and string histograms clearly need the min,
    // max, amount of tuples and array of buckets, it makes sense that we would also need an array of mins, an array
    // of maxes, an Object array for our two histograms (because they are distinct objects), and the number of tuples.
    // We also just happen to need the tupleDesc to get the DBFile so we can scan it, and we also keep the amount of
    // pages to see how long to scan for
    private int tableId;
    private int ioCostPerPage;
    private int[] mins;
    private int[] maxes;
    private int total;
    private Object[] histograms;
    private TupleDesc td;
    private int numPages;
    private int fieldCount;

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;

        // Now get the DBFile and scan
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        this.numPages = ((HeapFile) f).numPages();
        this.td = f.getTupleDesc();
        this.fieldCount = td.numFields();

        // Initialize min/max arrays and histograms arrays
        this.mins = new int[fieldCount];
        this.maxes = new int[fieldCount];
        this.histograms = new Object[fieldCount];

        // Default both mins/maxes
        for (int i = 0; i < fieldCount; i++) {
            if (this.td.getFieldType(i) == Type.INT_TYPE) {
                mins[i] = Integer.MAX_VALUE;
                maxes[i] = Integer.MIN_VALUE;
            }
            // It would be so much better if we could default our histograms here too, but they take mins and maxes
            // for the integer histogram, and java is pass by reference :(
        }


        // Two passes now - scan first and build first stats, and then populate histograms
        // Remember what the SeqScan wants -> A transaction, a table, and an alias. We dont really need the last part
        Transaction temp = new Transaction();
        temp.start();

        // So instead we use "" for the alias, someone mentioned this in an office hours (and throughout other files)
        SeqScan scan = new SeqScan(temp.getId(), this.tableId, "");

        this.total = 0;
        try {
            scan.open();

            while (scan.hasNext()) {
                Tuple curr = scan.next();
                this.total++;

                // If you haven't figured out by now, the first scan is for mins and maxes
                for (int j = 0; j < fieldCount; j++) {
                    if (this.td.getFieldType(j) == Type.INT_TYPE) {
                        // Remember the type class :) ! ... and the aggregators :(
                        int val = ((IntField) curr.getField(j)).getValue();
                        if (val < mins[j]) {
                            mins[j] = val;
                        } else if (val > maxes[j]) {
                            maxes[j] = val;
                        }
                    }
                }
            }
            scan.close();
            temp.commit();
        } catch (Exception e) {
            // Mentioned, once again, by other students to try catch so I will just in case
            e.printStackTrace();
        }

        // So now default histograms since we fixed mins and maxes
        for (int i = 0; i < fieldCount; i++) {
            if (this.td.getFieldType(i) == Type.INT_TYPE) {
                histograms[i] = new IntHistogram(NUM_HIST_BINS, mins[i], maxes[i]);
                // "  /**
                //     * Number of bins for the histogram. Feel free to increase this value over
                //     * 100, though our tests assume that you have at least 100 bins in your
                //     * histograms.
                //     */ "
            } else if (this.td.getFieldType(i) == Type.STRING_TYPE) {
                histograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }

        // Now do the transaction thing again!!
        Transaction temp2 = new Transaction();
        temp2.start();

        // So instead we use "" for the alias, someone mentioned this in an office hours (and throughout other files)
        SeqScan scan2 = new SeqScan(temp.getId(), this.tableId, "");

        this.total = 0;
        try {
            scan2.open();

            while (scan2.hasNext()) {
                Tuple curr = scan2.next();
                this.total++;

                // If you haven't figured out by now, the first scan is for mins and maxes
                for (int j = 0; j < fieldCount; j++) {
                    // This is different from other loops here though, since we get the field from the current tuple,
                    // not from the TupleDesc
                    Field currF = curr.getField(j);

                    if (currF.getType() == Type.INT_TYPE) {
                        ((IntHistogram) histograms[j]).addValue(((IntField) currF).getValue());
                    } else if (currF.getType() == Type.STRING_TYPE) {
                        ((StringHistogram) histograms[j]).addValue(((StringField)currF).getValue());
                    }
                }
            }
            scan2.close();
            temp2.commit();
        } catch (Exception e) {
            // Mentioned, once again, by other students to try catch so I will just in case
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ioCostPerPage * numPages;
        // And thats why we have numPages
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(this.total * selectivityFactor);
        // Well thank god (or curse him D:) cuz we already did all ofthis and can just use the formula
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
        // Didnt implement it in IntHistogram, so I wont here
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        // Plan: Quick sanity check, and then split into string, int, and neither
        if (histograms[field] == null) return 1.0;

        if (constant.getType() == Type.STRING_TYPE) {
            // String
            return ((StringHistogram)histograms[field]).estimateSelectivity(op, ((StringField) constant).getValue());
        } else if (constant.getType() == Type.INT_TYPE){
            // Int
            return ((IntHistogram) histograms[field]).estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            // Anything else
            return 1.0;
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.total;
    }

}
