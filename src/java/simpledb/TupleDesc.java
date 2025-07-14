package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }


    // ADDED the main data structure that will make up the tuple desc here because we
    // need to use it in the iterator too.
    public List<TDItem> tdList;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here

        // When it says "Create a new TupleDesc with typeAr.length fields", this
        // is just a confusing way of saying the length of the tupledesc obj IS the length
        // of the typeAr. Nice :(
        tdList = new ArrayList<TDItem>(typeAr.length);

        // Remember the tditem helper class created above. It was made so that the list we
        // have can easily store an item that contains both the type from typeAr and the field
        // from fieldAr
        TDItem temp;
        for (int i = 0; i < typeAr.length; i++) {
            temp = new TDItem(typeAr[i], fieldAr[i]);
            tdList.add(temp);
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tdList = new ArrayList<TDItem>(typeAr.length);

        // Basically the exact same but now each tditem will no longer have a specific name
        // i.e. that means it will be null
        TDItem temp;
        for (int i = 0; i < typeAr.length; i++) {
            temp = new TDItem(typeAr[i], null);
            tdList.add(temp);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here

        if (i < 0 || i >= tdList.size()){
            throw new NoSuchElementException("Invalid Index");
        }

        return tdList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= tdList.size()) {
            throw new NoSuchElementException("Invalid Index");
        }

        return tdList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) throw new NoSuchElementException("Input is null");

        for (int i = 0; i < numFields(); i++) {
            if (name.equals(getFieldName(i))) {
                return i;
            }
        }

        // If we exited the loop without returning an index, we didnt find a match
        throw new NoSuchElementException("No field with a matching name found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here

        // This obj is obviously built up of smaller objects, so we add their lengths
        // up since each individual piece is and always has been a byte
        int toReturn = 0;

        for (int i = 0; i < numFields(); i++) {
            // Of course we add the lengths of the types, not the names
            toReturn += getFieldType(i).getLen();
        }
        return toReturn;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here

        TupleDesc toReturn;
        // Since the constructor requires the field array and the name array, we need
        // to build them ourself, making new TDitems on the fly

        // We will use the general "ADT" Type var here
        Type[] typeArr = new Type[td1.numFields() + td2.numFields()];
        String[] nameArr = new String[td1.numFields() + td2.numFields()];

        mergeHelper(td1, typeArr, nameArr, 0, td1.numFields());
        mergeHelper(td2, typeArr, nameArr, td1.numFields(), td2.numFields());
        // Clearly, we want to place td1 right after td1, so we start at its index since arraylists
        // are 0-indexed. We dont need to make end td1.numFields() + td2.numFields() since the indexer
        // in the helper function uses a completely unrelated variable i


        toReturn = new TupleDesc(typeArr, nameArr);
        return toReturn;
    }

    /**
     * Helper function for merge above, since there is repetition with a loop going
     * over td1 and td2 DISTINCTLTY. This will fill up either the first or second half
     * off typeArr and nameArr
     *
     * @param td
     *          The tupledesc we want to put either into the first half or second half of our new TupleDesc
     * @param typeArr
     *          The type-of-field array to fill out
     * @param nameArr
     *          The name-of-field array to fill out
     * @param indexInNewObj
     *          The index in the new tupledesc. It is simple for td1, but for td2, it goes from td1.numFields()
     *          to td1.numFields() + td2.numFields()
     * @param end
     *          How long the loop should go in mergeHelper. It is only here to go for a specific amount of iterations.
     *          It does not serve as an index to typeArr or nameArr, but it does to our methods on td
     */
    public static void mergeHelper (TupleDesc td, Type[] typeArr, String[] nameArr, int indexInNewObj, int end) {
        for (int i = 0; i < end; i++) {
            typeArr[indexInNewObj] = td.getFieldType(i);
            nameArr[indexInNewObj] = td.getFieldName(i);

            indexInNewObj++;
        }
    }


    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here

        // Same number of items check
        if ( o == null || !(o instanceof TupleDesc) || (((TupleDesc)o).numFields() != this.numFields()) ) {
            return false;
        }

        // i-th type check
        for (int i = 0; i < this.numFields(); i++) {
            if (this.getFieldType(i) != ((TupleDesc)o).getFieldType(i) ) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String toReturn = "";

        // Fenceposting here
        for (int i = 0; i < numFields() - 1; i++) {
            toReturn += getFieldType(i) + "[" + i + "](" + getFieldName(i) + "[" + i + "]),";
        }
        // No comma for the last in the list obviously
        toReturn += getFieldType(numFields() - 1) + "[" + (numFields() - 1)
                + "](" + getFieldName(numFields() - 1) + "[" + (numFields() - 1) + "])";

        return toReturn;
    }
}
