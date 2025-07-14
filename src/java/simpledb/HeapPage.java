package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    private LinkedHashMap<TransactionId, Boolean> dirtyMap;
    // Added in lab 2/3, to keep track on if a txn modifies our page when outside of disk, and track that txn (dirty)

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tfuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();

        // Create dirty hashmap
        this.dirtyMap = new LinkedHashMap<>();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        // some code goes here
        // I found the how to get of alot of variables by looking at the java doc in the begining of the
        // class. One of our fields is a TupleDesc, so I just needed to figure out how to get its size
        return (int) Math.floor((double) (BufferPool.getPageSize() * 8) / (td.getSize() * 8 + 1));
        // Math.floor returns a double so we have to cast it, we cast the numerator to a double to
        // get the right type of divison. I FORGOT TO MULTIPLY PAGESIZE BY 8 FOR LIKE 3 HOURS IM GOING
        // TO FLIPPING DEFENESTRATE MY SELF
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        // We use our typical formula for this, and similar to how this formula used the result from the first one
        // the 'tupsperpage' variable we want to use here is getNumTuples/
        return (int) Math.ceil(this.getNumTuples() /8.0);
    }

    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        RecordId rid = t.getRecordId();
        if (rid == null || !rid.getPageId().equals(this.pid)) {
            // The current pid field!! Its easy to forget the this and then pretty difficult to remember what file
            // we are in
            throw new DbException("Tuple is not on this page!");
        }

        int tno = rid.getTupleNumber();

        if (!isSlotUsed(tno)) {
            throw new DbException("Tuple slot is already empty");
            // Second main error
        }

        PageId pid = rid.getPageId();

        markSlotUsed(tno, false);
        // So we dont have to deal with that bullshit bit mapping right now


        // Super easy to forget about this because we didnt write these fields but we have an array for ease of access
        // to
        tuples[tno] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        // Once again order checking in same order as doc
        if (getNumEmptySlots() == 0) {
            throw new DbException("No empty slots in this page");
        } else if (!t.getTupleDesc().equals(this.td)) {
            // I almost did == for a second, remember what language we are in!!
            throw new DbException("The tupledescs expected do not match");
        }

        for (int i = 0;i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                t.setRecordId(new RecordId(this.pid, i));
                markSlotUsed(i, true);

                // It was a bit difficult to remember all of this stuff, but its alot of saved info to help save us from
                // headaches later
                tuples[i] = t;
                break;
                // We only add one tuple per call
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	    // not necessary for lab1
        if (dirty) {
            this.dirtyMap.put(tid, true);
        } else {
            this.dirtyMap.remove(tid);
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	    // Not necessary for lab1

        // Loop through dirty map and keep updating until we get to the last one
        TransactionId lastUpdate = null;

        for (TransactionId t : dirtyMap.keySet()) {
            if (dirtyMap.get(t)) {
                lastUpdate = t;
            }
        }
        return lastUpdate;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        // Even though IsSlotUsed is right below this method, it is used here and it 2 other spots, so we
        // write this method lik
        int toReturn = 0;

        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) toReturn++;
        }

        return toReturn;
    }


    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        // First quick check
        if (i < 0 || i >= numSlots) return false;

        // The first pieces of this are easy, the last piece is not. The bitmap is a set of bytes that are built
        // up for us to more efficiently (idk about more easily) check if a slot is used later in the file. So,
        // we want to find the byte and therefore bit in the header of our i.

        // Calculate which byte and bit we want to use. This should be self explanatory, as i is a bit
        // value, and there are 8 bits in a byte, so we need to divide by8 to get out byte, and mod to get
        // our offset into that byte.
        int byteI = i / 8;
        int offset = i % 8;

        // this will shift the byte to align such that we can put our desired bit at the right end for an easy bitmask
        int bit = header[byteI] >> offset;

        // If the value is 1 at the right end, 1 & 1 will result 1 which is equal to 1, so this works
        return (bit & 1) == 1;
    }


    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1

        // Now that we are in lab 2, we can use getNumEmptySlots and isSlotUsed to fill this helper for some of our
        // functions above!! Similar to isSlotUsed above, we must first get our byte and bit offset
        int byteI = i / 8;
        int offset = i % 8;

        byte b = header[byteI];

        if (value) {
            // So if we want to sent something to 1, we take the byte, and then we OR it with a singular 1 left shifted
            // by offset. We calculated offset by seeing how far our value is in its byte, and by ORing the exact point
            // at offset, we guruantee to change a 0 to 1 and keep a 1 as 1. Mask: 0000001000 -> or with this
            header[byteI] = (byte) (b | ((byte)(1 << offset)));
        } else {
            // Look at my comments for the other case to get that, we want to have the same thing, but where we force
            // this value to turn to 0 no matter what and keep the others the same, and we just flip all the bits from
            // the previous mask, as 1 & 1 is 1, and 0 & 1 is 0 to maintain all other bits, while for our specific value,
            // it will be ANDd with 0 which forces it to turn from 1 to 0. Mask: 1111110111 -> and with this
            header[ byteI] = (byte) (b & ~((byte) (1 << offset)));
        }
    }


    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new Iterator<Tuple>() {
            // and thus, the super weird syntax of iterators in java begin
            private int i = 0;

            @Override
            public boolean hasNext() {
                while (i < numSlots && !isSlotUsed(i)) {
                    // We also have the check for slot use because as the doc makes clear, tuples in empty spots
                    // are never treated as real tuples either.
                    i++;
                }

                return i < numSlots;
            }

            @Override
            public Tuple next() {
                if (!hasNext()) throw new NoSuchElementException("No further elements");
                // might as well as use the above method we wrote

                // After first checking to make sure we can move forward, we then just move forward...
                return tuples[i++];
            }

            @Override
            public void remove() {
                // According to the javadoc comment above
                throw new UnsupportedOperationException("Removal is not supported");
            }
        };
    }

}

