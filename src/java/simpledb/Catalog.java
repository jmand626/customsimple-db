package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {

    // The reason why I decided to create a inner class for the information in the table is similar
    // to why the project created the Field interface or the TDItem inner class. Its far easier to
    // just group them together and use them in the bigger structures we use in the overall Catalog
    // class
    private class TableInformation {
        // Looking at the main addTable methods below, clearly we need a DbFile, a table's name, and
        // a primary key field, so since this class is here to store table info, those are our fields
        private DbFile file;
        private String name;
        private String pkeyField;

        public TableInformation (DbFile file, String name, String pkeyField) {
            this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }
    }

    // Now we need data structures for our inner tableinformation classes. Look at the methods after the
    // adds: getTableId (the one we can get from DbFile, look at that interface) takes the name of a table,
    // while every other method with a parameter takes in the tableid for a varied array of info. So:

    // HOWEVER, concurrent hash maps are already imported, which heavily implies we should use them over
    // regular hashmaps. I did ask on ed about them, and although the TA said not to worry about concurrency,
    // but we will just use them here.
    private Map<String, Integer> nameToId;
    private Map<Integer, TableInformation> idToInfo;




    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        nameToId = new ConcurrentHashMap<>();
        idToInfo = new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        // "May not be null"
        if (name == null) throw new IllegalArgumentException("Provide a non-null name");

        // Firstly, we deal with the name conflict. The "last table" is the one we are adding right
        // now, so we delete whatever table current holds that name
        if (nameToId.containsKey(name)) {
            idToInfo.remove(nameToId.get(name));
            nameToId.remove(name);
        }

        // Otherwise we can just add it. We can get the id (and the tupledesc) from the file, look at
        // that interface for the methods we need
        TableInformation ti = new TableInformation(file, name, pkeyField);

        // Now add to our hashmaps
        nameToId.put(name, file.getId());
        idToInfo.put(file.getId(), ti);
    }

    // The below overloaded addtables are just special cases of the above one we need to implement
    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null ||!nameToId.containsKey(name)) {
            throw new NoSuchElementException("Cannot find Table");
        }
        // Easy returns like this are why we used maps!
        return nameToId.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!idToInfo.containsKey(tableid)) {
            throw new NoSuchElementException("Cannot find Table id");
        }
        // We want to use the getTupleDesc method in the DbFile class, so we need to get the databasefile
        // Luckily enough for us, thats the next method (so this method was implemented second)
        return getDatabaseFile(tableid).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!idToInfo.containsKey(tableid)) {
            throw new NoSuchElementException("Cannot find Table id");
        }

        return idToInfo.get(tableid).file;
    }

    // And now its just super simple!!!

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if (!idToInfo.containsKey(tableid)) {
            throw new NoSuchElementException("Cannot find Table id");
        }

        return idToInfo.get(tableid).pkeyField;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here

        // Here we want to iterate over ids, we have a hashmap with ids as the key, and its super easy
        // to get the keyset. The answer is obvious
        return idToInfo.keySet().iterator();
    }

    // Who wrote the iterator method in between the getters :|

    public String getTableName(int id) {
        // some code goes here
        // WHY IS IT CALLED ID HERE INSTEAD
        if (!idToInfo.containsKey(id)) {
            throw new NoSuchElementException("Cannot find Table id");
        }

        return idToInfo.get(id).name;
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        // some code goes here
        nameToId.clear();
        idToInfo.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

