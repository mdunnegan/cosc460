package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.TupleDesc.TDItem;

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

	private Map<String, DbFile> tableHash; // {name: table}
	private Map<String, String> keyHash; // {name: keys as a string, weird}
	
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        tableHash = new HashMap<String, DbFile>();
        keyHash = new HashMap<String, String>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     *                  conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField){
    	if (file == null){
    		throw new RuntimeException("File is null");
    	}
    	if (name == null){
    		throw new RuntimeException("Name is null");
    	}
    	if (pkeyField == null){
    		throw new RuntimeException("pKeyField is null");
    	}	
    	tableHash.put(name, file);	
    }

    public void addTable(DbFile file, String name){
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
    public void addTable(DbFile file){
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // search the catalog for a DbFile whose name == name. return the id of this table
    	if (!tableHash.containsKey(name)){
    		throw new NoSuchElementException("Table does not exist");
    	}
        return tableHash.get(name).getId();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    
    // Don't know why this isn't working
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	for (DbFile file : tableHash.values()){
    		if (file.getId() == tableid){
    			return file.getTupleDesc();
    		}
    	}
    	return null;	
        //throw new NoSuchElementException("No such element");
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    	for (DbFile file : tableHash.values()){
    		if (file.getId() == tableid){
    			return file;
    		}
    	}
        return null;
    }

    public String getPrimaryKey(int tableid) {
    	for (String tableName : tableHash.keySet()){
    		if (tableHash.get(tableName).getId() == tableid){
    			return keyHash.get(tableName);
    		}
    	}
    	return null;
    }

    public Iterator<Integer> tableIdIterator() {
    	
    	// wont work
    	//List<Integer> ids = Arrays.asList(tableHash.values().getId()); // iterates through the values, this should be close, it's very similar to the TupleDesc Iterator
    	List ids = new ArrayList(tableHash.size());
    	
    	// iterate through these
    	for (DbFile dbfile : tableHash.values()){
    		ids.add(dbfile.getId());
    	}
    	
    	return ids.iterator();     
    }

    public String getTableName(int id) {
    	for (String name : tableHash.keySet()){
    		if (tableHash.get(name).getId() == id){
    			return name;
    		}
    	}
        return null;
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        tableHash.clear();
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
                System.out.println("Added table : " + name + " with schema " + t + (primaryKey.equals("")? "":(" key is " + primaryKey)));
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

