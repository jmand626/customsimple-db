package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    // We obviously want the buckets, the min, and the max as described below, but considering the diagrams from the
    // spec its obvious that we need the width of our buckets as selection predicates will more often than not cut into
    // our buckets as opposed to sticking to their boundaries. We will also keep the total amount of values for
    // obvious reasons

    private int[] buckets;
    private int min;
    private int max;
    private int width;
    private int total;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.total = 0;

        // Now we have to calculate the width of the bucket. Since its obviously dependent on other values, we have
        // to do some actual math monkaS to see it. But first! -> special case
        double range = (double) (this.max - this.min + 1);
        if (this.min == this.max) {
            this.width = 1;
        } else{
            // We use the simple approach of an fixed number of buckets and a fixed range for each bucket for the
            // numbers that end up in each bucket
            this.width = (int) Math.ceil(range/buckets);
        }

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = indexLookup(v);
        if (index < buckets.length && index >= 0) {
            total++;

            // Increment both our total count and bucket array
            buckets[index]++;
        }
    }

    // Helper function to get indices based on values
    /**
     * Retrieves the bucket index in buckets for this value
     *
     * @param v Value to lookup
     */
    public int indexLookup (int v) {
        if (v > max || v < min) return -1;
        // Although ryan said we don't have to handle this case, comment it out if it causes issues

        return (v - this.min)/width;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        // First quick sanity check
        if (total == 0) {
            return 0.0;
        }

        switch (op) {
            // Break into cases and call functions that we will write later. I could put them in each cell in line here,
            // clearly the function already mentioned points us to do it like this
            case EQUALS:
                return equalsSelectivity(v);
            case NOT_EQUALS:
                // not out our equals selectivity
                return 1.0 - equalsSelectivity(v);
            case GREATER_THAN:
                return greaterthanSelectivity(v);
            case GREATER_THAN_OR_EQ:
                return equalsSelectivity(v) + greaterthanSelectivity(v);
                // No reason to deal with greater or than separately, just do this
            case LESS_THAN:
                return lessthanSelectivity(v);
            case LESS_THAN_OR_EQ:
                return equalsSelectivity(v) + lessthanSelectivity(v);
            default:
                return 0.0;
        }
    }

    /**
     * Estimate the selectivity of an equals predicate with our value v
     *
     * @param v  Value
     * @return Predicted selectivity of running equals through our table with v
     */
    private double equalsSelectivity(int v) {
        // If value is outside range, selectivity is 0
        if (v < min || v > max) {
            return 0.0;
        }
        int height = buckets[indexLookup(v)];
        // Clearly we should assume uniform distribution (mentioned many times in the readme and code)
        // Then, assuming values are uniformly distributed throughout the bucket, the selectivity of the expression is
        // roughly (h / w) / ntups , since (h/w) represents the expected number of tuples in the bin with value const.
        return ((double) height / this.width) / this.total;
    }

    /**
     * Estimate the selectivity of greater than predicate with our value v. Note that this does not deal with
     * greater than or equals
     *
     * @param v Value
     * @return Predicted selectivity of running a greater than predicate through our table with v
     */
    private double greaterthanSelectivity(int v) {
        // If value is outside range, selectivity is 0
        if (v < min){
            return 1.0;
        } else if (v >= max) {
            return 0.0;
        }
        // Quick and easy cases first
        // "Make sure you also consider outliers. For example, if f=const has a const outside the domain, the selectivity
        // should be 0. If f>const or f<const results in selecting all tuples, then you can simply return 1."

        // Now we follow the explanation given in 2.2.3
        int index = indexLookup(v);
        int height = buckets[index];

        // We want to figure out the fraction of tuples in this bucket that we want to take, as well as the buckets
        // to the left of this cut one in entire. First we look for the fractional component
        // To calculate the selectivity of all of b itself
        double bFrac = (double) height / total;

        // Now calculate b_right. Obviously we start at min, and it makes sense that if you multiply the index by
        // the fixed width, you get to the start of an bucket. We add 1 to index to get to the bucket after ours,
        // multiply this value by the width, and then subtract 1, because as just mentioned, multiplying gets
        // to the start of the bucket one after ours, so we can just subtract one number to get our end.
        int right = min + (index+1) * width - 1;
        if (right > max) right = this.max;
        // Quick sanity check

        double bPart = (double) (right - v) / width;
        double selectivity = bFrac * bPart;
        //  bucket b contributes (b_f x b_part)

        //  "In addition, buckets b+1...NumB-1 contribute all of their selectivity (which can be computed using a formula
        //  similar to b_f above). Summing the selectivity contributions of all the buckets will yield the overall
        //  selectivity of the expression."
        for (int i = index + 1; i < buckets.length; i++) {
            selectivity += (double) buckets[i] / total;
        }

        return selectivity;
    }

    /**
     * Estimate the selectivity of an equals predicate with our value v
     *
     * @param v Value
     * @return Predicted selectivity of running equals through our table with v
     */
    private double lessthanSelectivity(int v) {
        // If value is outside range, selectivity is 0
        if (v <= min) {
            return 0.0;
        } else if (v > max) {
            return 1.0;
        }
        // Quick and easy cases first
        // "Make sure you also consider outliers. For example, if f=const has a const outside the domain, the selectivity
        // should be 0. If f>const or f<const results in selecting all tuples, then you can simply return 1."

        // Now we follow the explanation given in 2.2.3
        int index = indexLookup(v);
        int height = buckets[index];

        // We want to figure out the fraction of tuples in this bucket that we want to take, as well as the buckets
        // to the left of this cut one in entire. First we look for the fractional component
        // To calculate the selectivity of all of b itself
        double bFrac = (double) height / total;

        // Now calculate b_left: we dont have to any weird thing where we go one past and then subtract back
        int left = min + index * width;


        double bPart = (double) (v-left) / width;
        double selectivity = bFrac * bPart;
        //  bucket b contributes (b_f x b_part)

        //  "We also have to take an range of buckets, but this time we take the complete/total buckets before the
        // one that we partitioned"
        for (int i = 0; i < index; i++) {
            selectivity += (double) buckets[i] / total;
        }

        return selectivity;
    }


    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // We will also just skip this method sorry. Finals week is ugh
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        /*String s = "IntHistogram(min =" + min + ", max =" + max + ", width =" + width + ", " +
                "total =" + total + ", buckets = [";
        // Beginning properties/fields to place first

        for (int j =0; j < buckets.length; j++) {
            s += buckets[j];
            if (j < buckets.length -1) {
                s += ", ";
                // Add buckets with ,
            }
        }
        // Fence post
        s += " ]) ";
        return s;*/
        return "";
    }
}
