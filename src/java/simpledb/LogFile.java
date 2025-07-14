
package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;
import java.lang.reflect.*;

/**
LogFile implements the recovery subsystem of SimpleDb.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>
*/

/**
<p> The format of the log file is as follows:

<ul>

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

<li> Each log record ends with a long integer file offset representing
the position in the log file where the record began.

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>

*/

public class LogFile {

    final File logFile;
    private RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;
    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    long currentOffset = -1;//protected by this
//    int pageSize;
    int totalRecords = 0; // for PatchTest //protected by this

    HashMap<Long,Long> tidToFirstLogRecord = new HashMap<Long,Long>();

    /** Constructor.
        Initialize and back the log file with the specified file.
        We're not sure yet whether the caller is creating a brand new DB,
        in which case we should ignore the log file, or whether the caller
        will eventually want to recover (after populating the Catalog).
        So we make this decision lazily: if someone calls recover(), then
        do it, while if someone starts adding log file entries, then first
        throw out the initial log file contents.

        @param f The log file's name
    */
    public LogFile(File f) throws IOException {
	this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
                // public void run() { shutdown(); }
            // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    // we're about to append a log record. if we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
    void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
    }
    
    /** Write an abort record to the log for the specified tid, force
        the log to disk, and perform a rollback
        @param tid The aborting transaction.
    */
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getBufferPool()) {

            synchronized(this) {
                preAppend();
                //Debug.log("ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                rollback(tid);

                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    /** Write a commit record to disk for the specified tid,
        and force the log to disk.

        @param tid The committing transaction.
    */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log("COMMIT " + tid.getId());
        //should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /** Write an UPDATE record to disk for the specified tid and page
        (with provided         before and after images.)
        @param tid The transaction performing the write
        @param before The before image of the page
        @param after The after image of the page

        @see simpledb.Page#getBeforeImage
    */
    public  synchronized void logWrite(TransactionId tid, Page before,
                                       Page after)
        throws IOException  {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int pageInfo[] = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int i = 0; i < pageInfo.length; i++) {
            raf.writeInt(pageInfo[i]);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object idArgs[] = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = new Integer(raf.readInt());
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException e){
            e.printStackTrace();
            throw new IOException();
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning

    */
    public synchronized  void logXactionBegin(TransactionId tid)
        throws IOException {
        Debug.log("BEGIN");
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.printf("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    //Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    /** Truncate any unneeded portion of the log to reduce its space
        consumption */
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();

                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                case UPDATE_RECORD:
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);

                    writePageData(logNew, before);
                    writePageData(logNew, after);
                    break;
                case CHECKPOINT_RECORD:
                    int numXactions = raf.readInt();
                    logNew.writeInt(numXactions);
                    while (numXactions-- > 0) {
                        long xid = raf.readLong();
                        long xoffset = raf.readLong();
                        logNew.writeLong(xid);
                        logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                    }
                    break;
                case BEGIN_RECORD:
                    tidToFirstLogRecord.put(record_tid,newStart);
                    break;
                }

                //all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e) {
                break;
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        //print();
    }


    /** Rollback the specified transaction, setting the state of any
        of pages it updated to their pre-updated state.  To preserve
        transaction semantics, this should not be called on
        transactions that have already committed (though this may not
        be enforced by this method.)

        @param tid The transaction to rollback
    */
    public void rollback(TransactionId tid)
        throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                preAppend();
                // some code goes here
                // We make two passes to first collect all pages to undo, and then we apply it. I originally
                // tried to do this with only one pass, and I am sure that that works, but I quickly thought that
                // it would be easier and cleaner to just do this.

                // We start by using the map given at the top of this file to figure out where to start reading
                // for this transaction
                Long first = tidToFirstLogRecord.get(tid.getId());
                Long currTid = tid.getId();
                // I guess this is why we made some fields longs across this project
                // Escape out if there is no such first element
                if (first ==null) {
                    throw new NoSuchElementException("No elements found for this TXN: " + currTid);
                    // This makes sense since this the header already had this exception
                }

                List<Page> list = new ArrayList<>();
                // Since we obviously have to go in kind of reverse order of how the log builds up, so from the
                // end to the start of our TXN.

                // We use the class-wide Random Access File to read to our start.
                raf.seek(first);

                // while true loop instead of a for loop because we are using an actual File, so it doesnt seem
                // reasonable to be build a for loop when its unclear what that actually means here.

                long pos = 0;
                while (true) {
                    // We read the position above for the first piece, so clearly we need to do that here
                    pos = raf.getFilePointer();
                    // This method is used in many places in this file to get the "current offset", so it makes
                    // sense given its name

                    // The way we can get pass not having a for loop
                    if (pos >= raf.length()) break;
                    // An regretful amount of pain came from > instead of >= because of how raf was written :(

                    // Break into whether or not a current log write is even related to our current TXN,
                    // if they are not we want to just ignore them

                    // "Each log record begins with an integer type and a long integer transaction id."
                    int intType = raf.readInt();
                    long txnID = raf.readLong();
                    if (txnID == tid.getId()) {
                        // Clearly the only thing that matters here is just saving all the updates, any other
                        // actions dont matter
                        if (intType != UPDATE_RECORD) {
                            // So we just keep reading and reading to get past this uninteresting stuff
                            raf.readLong();
                        } else {
                            // Now we can use the readPageData mentioned to read the before and after images
                            // mentioned in the spec. Note that we have no need to actually use the after image,
                            // but we just read it so that the actual pointer is moved forward
                            Page before = readPageData(raf);

                            // Just read to move past
                            Page after = readPageData(raf);

                            list.add(before);
                            // For our second pass later

                            // "Each log record ends with a long integer file offset representing the position in
                            // the log file where the record began." We want to skip that
                            raf.readLong();
                        }
                    } else {
                        // We want to skip all of this data. Note that there are 5 different types of records,
                        // and update and commit are different from the other ones. For begin, abort, and commit
                        // we can just readLong as we did above, but for update we have the before/after image
                        // and the final record start pointer, while for checkpoint, we have the TXN count, tid,
                        // and record offset/pointer, and the actual final record start pointer
                        if (intType == CHECKPOINT_RECORD) {
                            int txnCount = 0;

                            txnCount = raf.readInt();
                            int i = 0;
                            // Now read all of the transactions that we counted previously
                            while (i < txnCount) {
                                raf.readLong();
                                raf.readLong();
                                i++;
                            }
                            raf.readLong();
                        } else if (intType == UPDATE_RECORD) {
                            // As mentioned above, there are three things to skip: before-image/after-image, and
                            // finally the final record start pointer/offset
                            readPageData(raf);
                            readPageData(raf);

                            raf.readLong();
                        } else {
                            // Since the last three transactions should all be uniform and simple
                            raf.readLong();
                        }
                    }
                }

                // Now we finally get out of that while loop AND NOW WE ARE AT THE SECOND LOOP TO ACTUALLy UNDO
                // STUFF WOOOOOOOOOOO
                for (int i = list.size() - 1; i >= 0; i--) {
                    // "extract the before-image from each"
                    Page before = list.get(i);
                    PageId bpid = before.getId();
                    int tbid = bpid.getTableId();
                    Page curr = null;

                    // We get the corresponding page from buffer pool, if we can find it.
                    try {
                        curr = Database.getBufferPool().getPage(tid, bpid, Permissions.READ_WRITE);
                        // We must try catch this cuz of the exceptions getPage throws
                    } catch (Exception e) {}


                    // Use the writePage we have used throughout bufferpool
                    DbFile dbf = Database.getCatalog().getDatabaseFile(tbid);


                    // Write and then discard
                    dbf.writePage(before);
                    Database.getBufferPool().discardPage(bpid);
                }
            }
        }
    }

    /** Shutdown the logging system, writing out whatever state
        is necessary so that start up can happen quickly (without
        extensive recovery.)
    */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                // some code goes here

                // Remember the crucial point: there are three points to our recovery: collecting info,
                // redo, and undo.

                raf.seek(0);
                // Seek the beginning and then read the long integer that represents the offset into the first
                // checkpoint written into the file
                long checkOff = raf.readLong();
                if (raf.length() == 8) return;
                // Quickly check to see if that if this is the only thing in the file, delete it because then
                // there is nothing else that happened in the final

                List<Long> updateIDs = new ArrayList<>();
                List<Long> updateFilePos = new ArrayList<>();
                // Set<Long> updateFilePos = new ArrayList<>(); -> Originally I did this, and its fine, but
                // we dont need the exclusivity to apply to this since each filePos connects to an id/tid, and
                // that is unique

                Set<Long> uncommitted = new HashSet<>();
                // If you remember how ARIES recovery works, we redo everything, and then undo what shouldn't
                // have been redone in the first place

                // Read in the tids and types for each element together. The type is an int and the tid is a long
                // and by the info given at the top of the file, the int is 4 bytes while the long is 8 bytes.
                while (raf.getFilePointer() + 12 <= raf.length()) {
                    // We recenter our file pointer, and then read out type and tid
                    long start = raf.getFilePointer();

                    int currType = raf.readInt();
                    long tid = raf.readLong();
                    if (currType == 0) break;

                    switch (currType) {
                        case ABORT_RECORD:
                            // If something has aborted, it's no longer considered uncommitted, so if it
                            // was added there we just take it out and move past the final pointer/offset, which
                            // we will do in every case here
                            raf.readLong();
                            break;
                        // Unlike the last time that I did switch statements, which were the aggregators, here
                        // we break instead of returning since a TXN can obviously have many actions, and
                        // we must go over each one one-by-one

                        case COMMIT_RECORD:
                            // Obviously take it out of uncommitted if it exists there, then same thing
                            uncommitted.remove(tid);
                            raf.readLong();
                            break;

                        case UPDATE_RECORD:
                            updateIDs.add(tid);
                            // Remember that we have our before and after images here, which pretty much are the
                            // only new things here
                            updateFilePos.add(start);

                            readPageData(raf);
                            readPageData(raf);
                            raf.readLong();
                            break;

                        case BEGIN_RECORD:
                            uncommitted.add(tid);
                            raf.readLong();
                            break;

                        case CHECKPOINT_RECORD:
                            // Read past the dummy tid field.
                            raf.readLong();

                            int numActive = raf.readInt(); // Read the number of active transactions.
                            for (int i = 0; i < numActive; i++) {
                                long activeTid = raf.readLong();
                                raf.readLong(); // Read and discard first log record offset.
                                uncommitted.add(activeTid); // Add active transactions from checkpoint.
                            }
                            raf.readLong(); // Skip trailing pointer.
                            break;
                    }
                }

                // Clearly, we move onto redo. Notice how these two loops seem to go in opposite directions.
                for (int i = 0; i < updateFilePos.size(); i++) {
                    raf.seek(updateFilePos.get(i));

                    // Classic type/tid pair
                    int type = raf.readInt();
                    long tid = raf.readLong();
                    if (type != UPDATE_RECORD) throw new IOException("Requires UPDATE_RECORD to update");

                    // Now look to the images
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);
                    raf.readLong();

                    // Write to the database
                    int tableID = after.getId().getTableId();

                    DbFile dbf = Database.getCatalog().getDatabaseFile(tableID);
                    dbf.writePage(after);

                    // Flush and get rid of dirtiness
                    Database.getBufferPool().discardPage(after.getId());
                }

                // Now go in the reverse direction for undo, bottom up as discussed
                for (int j = updateFilePos.size() - 1; j >= 0; j--) {
                    long pointer = updateFilePos.get(j); // FIXED: Using file position instead of transaction ID
                    raf.seek(pointer);

                    // Remember that the redo in ARIES (I've said this many times but to be clear) redoes uncommitted
                    // TXNs as well, even though that is not what redo does individually. Undo must only undo
                    // actions made by the UNCOMMITTED TXNs. Luckily, we have a set.
                    if (uncommitted.contains(updateIDs.get(j))) {
                        // Classic type/tid pair
                        int type = raf.readInt();
                        long tid = raf.readLong();
                        if (type != UPDATE_RECORD) throw new IOException("Requires UPDATE_RECORD to update");

                        // Now look to the images
                        Page before = readPageData(raf);
                        Page after = readPageData(raf);
                        raf.readLong();

                        // Ensure we do not undo an update that has been overwritten by a committed transaction.
                        boolean later = false;
                        PageId p = after.getId();
                        for (int k = j + 1; k < updateFilePos.size(); k++) { // FIXED: Corrected loop condition
                            long curr = updateIDs.get(k);

                            // Check for committed transactions that hold log entries on this page.
                            if (!uncommitted.contains(curr)) {
                                raf.seek(updateFilePos.get(k));
                                int typeNext = raf.readInt();
                                long tidNext = raf.readLong();
                                if (typeNext != UPDATE_RECORD)
                                    throw new IOException("Requires UPDATE_RECORD to update");

                                PageId pnext = readPageData(raf).getId();
                                // Compare the page modified by a later txn when it comes to our current page.
                                raf.readLong();

                                if (pnext.equals(p)) {
                                    later = true;
                                    break;
                                }
                            }
                        }
                        if (!later) {
                            // Write back our original before image because here we finally undo.
                            DbFile dbf = Database.getCatalog().getDatabaseFile(p.getTableId());
                            dbf.writePage(before);

                            // Flush and get rid of dirtiness.
                            Database.getBufferPool().discardPage(p);
                        }
                    }
                }
            }
        }
    }


    /** Print out a human readable represenation of the log */
    public void print() throws IOException {
        // some code goes here
    }

    public  synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }

}
