package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    // We create our fields according to the constructor below
    private File f;
    private TupleDesc td;

    // Originally i had a hashmap from pageid to page, turns out that really did not work

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
        // I love easy methods, just looked at the javadoc for this
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // We use a random access file method as answered in Section 2 for me. I had to do try-catch
        // in order for me to get warnings instead of errors
        try {
            RandomAccessFile r = new RandomAccessFile(this.f, "r");
            int pageSize = BufferPool.getPageSize();
            // Remember the project-wide variables we can use from database.java. We need this for the offset

            byte[] bytes = new byte[pageSize];
            // Obviously we want to read a page, and the read function will read bytes.length (so page size)
            // amount of bytes into the this exact array.

            // Since we are reading full pages, obviously we want to get to some multiple of page size
            // and we can remember we can get our page number with heappageid's getter for the pgNo field
            // Also, we use longs because the seek function expects longs, not ints
            int pgNum = pid.getPageNumber();
            long pos = pgNum * (long) pageSize;

            // Now our classical seek, read, and close. The creation of the r object was the 'open call'
            r.seek(pos);
            r.read(bytes);
            r.close();

            // Construct the heappage that we will put into our hashmap and then return
            HeapPage p = new HeapPage((HeapPageId) pid, bytes);

            //pageIdToPage.put(pid, p);
            return p;
        } catch (IOException e) {
            throw new IllegalArgumentException("Reading error", e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        // Looking at the javadoc, we use page.getId().pageno() for the offset. HOWEVER, the page numbers do not take
        // into account the actual page sizes themselves, so we must also multiply by the actual size. Don't ask me how
        // i figured this out, too much pain :(
        // To write into the file, we look back
        // just one method above to see that we used randomaccessfile, and we will use that
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();

        RandomAccessFile r = new RandomAccessFile(this.f, "rw");
        // I would hope to just get a write mode, but only rw exists so we go with that

        r.seek((long) offset);
        r.write(page.getPageData());
        // Remember we have a parameter page to write. Looking at the page class, we see that getPageData returns an
        // byte array, which is actually what the RandomAccessFile needs to write/read something

        r.close();
        // Now surely insertTuple and deleteTuple are just as simple right? TrollDespair
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // Our formula: dividing file size by page size (how many pages fit into our file) works
        // because we only add space as we need it
        int pageSize = BufferPool.getPageSize();
        return (int) (f.length() / pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // I did deleteTuple first, so go there if you want to see how I approached getting stuff
        // Ignoring anything to do with locks for a second, we first want to make sure that we can add this tuple,
        // which means that we check if the schema/td of the tuple matches with the schema/td of the file because
        // otherwise it does not make sense to put this td into this file
        TupleDesc td = t.getTupleDesc();
        if (!td.equals(this.td)) {
            throw new DbException("This tuple cannot be added");
        }

        ArrayList<Page> list = new ArrayList<>();
        // Cant do list = new ArrayList :(

        // Now we want to "find a page with an empty slot" if that exists, or "create a new page and append it to the
        // physical file on disk"
        for (int i = 0; i < numPages(); i++) {
            // Read/write a page to see if there is space. But before we can call that method from BufferPool, we need
            // a PageId for the PAGE, not our TUPLE T, so we cant call t.getRid.getPid
            HeapPageId pid = new HeapPageId(this.getId(), i);

            // Edited this to start with a read only lock first until it finds an non empty slot
            // We need a tableid/fileid and a page number. Well we have an id and we are iterating so
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);

            // and know HOPEFULLY there is enough room
            if (page.getNumEmptySlots() > 0) {
                // only release and upgrade if we found an actual empty slot
                Database.getBufferPool().releasePage(tid, pid);

                // upgrade lock
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

                page.insertTuple(t);
                // Our method payed off there!
                list.add(page);
                return list;
                // Short circuit and get out!
            } else {
                // With the additional logic of starting with the read lock, we need an else case
                Database.getBufferPool().releasePage(tid, pid);
            }
        }
        // Clearly thats not 2pl, but according to the last bullet point in 2.5, thats ok

        // We need to add a new page. First, we need to make a new pid, and we can just sneak it onto the end
        HeapPageId pid = new HeapPageId(this.getId(), numPages());

        // Next, if you remember, from HeapPage the constructor wants our pid and a byte array of data, because
        // the byte[] corresponds to reads and writes. There is also a method in that class to initialize this properly!
        byte[] data = HeapPage.createEmptyPageData();

        // Now we try/catch to get this ioexception
        try{
            RandomAccessFile r = new RandomAccessFile(this.f, "rw");

            // Numpages * size gets all the way to the end, and we want to be here to add our new page
            r.seek(numPages() * BufferPool.getPageSize());
            r.write(data);
            r.close();

        } catch(IOException e) {
            throw new IOException("the needed file can't be read/written");
        }


        // Now get this page properly back from the bufferpool, actually created since the bufferpool will create the
        // obj once it reads it back from disk.
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        // Insert this tuple

        list.add(page);
        return list;
    }


    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // We throw a DB exception if the "tuple cannot be deleted" or if its "not a member of this file", so we start
        // with that. We ignore anything to do with locking until lab 3.
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();

        // We cannot delete a tuple if it has no recordId, which means that something bad happened. Or, the pageid
        // we get from this tuple does not match this files id
        if (rid == null) {
            throw new DbException("This tuple does not exist and cannot be deleted");
        } else if (pid.getTableId() != this.getId()) {
            throw new DbException("This tuple does not exist on this file");
        }

        // Now we must get our page from bufferpool in exactly the way as mentioned in the readme. There is a simple
        // getPage method that we can use that requires an Transactionid, a pid, and permissions. We can then simply
        // look at the permissions class and see that we can either take read only or read write, and we obviously go
        // for that. I had completely forgotten but we had done this in the iterator before so look at that!
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        // And now we can use the the method we previously wrote!

        // And now we return an arraylist! (which for some reason cannot be a list = arraylist thing but ok i guess)
        ArrayList<Page> list = new ArrayList<>();
        list.add(page);
        return list;
    }


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        // To be clear, unlike for pages, we need to fill 5 method: open, hasNext, next, rewind, close
        return new DbFileIterator() {
            // keep track of the page we are in, and then our iterator through the page. If we have to go
            // to next page, we will make a new iterator for the next page
            private int pageCount;
            private Iterator<Tuple> tupleIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if(numPages() > 0) {
                    pageCount = 0;
                    // We start with the first page
                    HeapPageId pid = new HeapPageId(getId(), 0);
                    // getId() is the only way to get a pid btw

                    HeapPage page = (HeapPage) Database.getBufferPool().
                            getPage(tid, pid, Permissions.READ_ONLY);
                    // We looked at the permissions class, and since we are never writing anything here,
                    // I just made it read only.

                    tupleIterator = page.iterator();

                    // We have to stop one early because we already looked at the first page
                    for (int i = 0; i < numPages() - 1 && !tupleIterator.hasNext(); i++) {
                        // We also check if !tupleIterator.hasNext() because when we started, our page
                        // did not have tuples

                        pageCount++;

                        // Doing pretty much exactly what we did before for the first page, but for whatever
                        // page we are now on.
                        pid = new HeapPageId(getId(), pageCount);
                        page = (HeapPage) Database.getBufferPool().
                                getPage(tid, pid, Permissions.READ_ONLY);

                        // Get iterator for this page's tuples
                        tupleIterator = page.iterator();
                    }
                }
            }


            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (tupleIterator == null) {
                    return false;
                    // Two quick cases to get out of the way, we don't actually have an iterator, or ...
                }else if (tupleIterator.hasNext()) {
                    return true;
                    // ... we simply can just move our tuple iterator forward
                }

                // Loop through the rest of the file to see if we can iterate over something
                for (int i = pageCount+1; i < numPages(); i++) {
                    // Create PageId for the page we're checking
                    HeapPageId pid = new HeapPageId(getId(), i);

                    // got the page through buffer pool
                    HeapPage page = (HeapPage) Database.getBufferPool().
                            getPage(tid, pid, Permissions.READ_ONLY);
                    // We looked at the permissions class, and since we are never writing anything here,
                    // i just made it read only.

                    Iterator<Tuple> next = page.iterator();
                    if (next.hasNext()) {
                        // update our page count to where we are currently, to represent the idea that
                        // we have indeeed found more pages to explore
                        pageCount = i;
                        tupleIterator = next;
                        return true;
                    }
                }

                // If we get here, no pages had any tuples left
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                // If iterator not initialized or no next element, throw exception
                if (tupleIterator == null || !hasNext()) throw new NoSuchElementException();
                return tupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                pageCount = 0;
                tupleIterator = null;
            }
        };
    }

}

