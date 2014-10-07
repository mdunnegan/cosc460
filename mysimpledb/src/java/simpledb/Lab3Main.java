package simpledb;

import java.io.IOException;
import java.util.ArrayList;

import simpledb.Predicate.Op;

public class Lab3Main {

    public static void main(String[] argv) throws DbException, TransactionAbortedException, IOException {

        System.out.println("Loading schema from file:");
        // file named college.schema must be in mysimpledb directory
        Database.getCatalog().loadSchema("college.schema");

        // SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
        // algebra translation: select_{name="alice"}( Students )
        
        // query plan: a tree with the following structure
        // - a Filter operator is the root; filter keeps only those w/ name=Alice
        // - a SeqScan operator on Students at the child of root
        
        /**
        SELECT * 
        FROM Students
        WHERE name="alice"
        **/
        
//        TransactionId tid = new TransactionId();
//        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("students"));
//        StringField alice = new StringField("alice", Type.STRING_LEN);
//        Predicate p = new Predicate(1, Predicate.Op.EQUALS, alice);
//        Filter filterStudents = new Filter(p, scanStudents);
//        
//        // query execution: we open the iterator of the root and iterate through results
//        
//        // Error here...
//        System.out.println("Query results:");
//        
//        filterStudents.open();
//        
//        System.out.println("Opened!");
//        while (filterStudents.hasNext()) {
//            Tuple tup = filterStudents.next();
//            System.out.println("\t"+tup);
//        }
//        filterStudents.close();
//        Database.getBufferPool().transactionComplete(tid);
//        
        /**
        SELECT * 
        FROM Courses, Profs
        WHERE cid = favoriteCourse
        **/
//        
//        TransactionId tid1 = new TransactionId();
//        SeqScan scanCourses = new SeqScan(tid1, Database.getCatalog().getTableId("courses"));
//        SeqScan scanProfs = new SeqScan(tid1, Database.getCatalog().getTableId("profs"));
//        
//        JoinPredicate jp = new JoinPredicate(scanCourses.getTupleDesc().fieldNameToIndex("courses.cid"),
//        									Predicate.Op.EQUALS,
//        									scanProfs.getTupleDesc().fieldNameToIndex("profs.favoriteCourse"));
//        
//        Join favorites = new Join(jp, scanCourses, scanProfs);
//        
//        /**Query Execution**/
//        
//        System.out.println("Query results for Profs favorite courses:");
//        
//        
//        // Not opening
//        scanCourses.open();
//        scanProfs.open();
//        favorites.open();
//        
//        while (favorites.hasNext()){
//        	System.out.println("Join had next");
//        	Tuple t = favorites.next();
//        	System.out.println(t.toString());
//        }
//        favorites.close();
        
        /**
        SELECT * 
        FROM Students S, Takes T
        WHERE S.sid = T.tid
        */
        
//        TransactionId tid2 = new TransactionId();
//        SeqScan scanStudents2 = new SeqScan(tid2, Database.getCatalog().getTableId("students"));
//        SeqScan scanTakes = new SeqScan(tid2, Database.getCatalog().getTableId("takes"));
//        
//        JoinPredicate jp2 = new JoinPredicate(scanStudents2.getTupleDesc().fieldNameToIndex("students.sid"), 
//        									Predicate.Op.GREATER_THAN,
//        									scanTakes.getTupleDesc().fieldNameToIndex("takes.sid"));
//        
//        Join takes = new Join(jp2, scanStudents2, scanTakes);
//        
//        System.out.println("Query results for students taking courses:");
//        scanStudents2.open();
//        scanTakes.open();
//        takes.open();
//        
//        while (takes.hasNext()){
//        	Tuple t = takes.next();
//        	System.out.println(t.toString());
//        }
//        takes.close();
        
        /**
         * SELECT S.name
		   FROM Students S, Takes T, Profs P
		   WHERE S.sid = T.cid AND
	       T.cid = P.favoriteCourse AND
	       P.name = "hay"
         */
        
        TransactionId tid2 = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid2, Database.getCatalog().getTableId("students"));
        SeqScan scanTakes = new SeqScan(tid2, Database.getCatalog().getTableId("takes"));
        SeqScan scanProfs = new SeqScan(tid2, Database.getCatalog().getTableId("profs"));
        
        JoinPredicate jp1 = new JoinPredicate(scanStudents.getTupleDesc().fieldNameToIndex("students.sid"), 
      							Predicate.Op.EQUALS,
      							scanTakes.getTupleDesc().fieldNameToIndex("takes.cid"));
        
        JoinPredicate jp2 = new JoinPredicate(scanTakes.getTupleDesc().fieldNameToIndex("takes.cid"), 
								Predicate.Op.EQUALS,
								scanProfs.getTupleDesc().fieldNameToIndex("profs.favoriteCourse"));
        
        StringField hay = new StringField("hay", Type.STRING_LEN);  
        
        Predicate p = new Predicate(1, Op.EQUALS, hay); // avoiding long lines of code
        Filter hayFilter = new Filter(p, scanProfs);
        
        Join j1 = new Join(jp1, scanTakes, hayFilter);
        Join j2 = new Join(jp2, scanStudents, j1);
        
        ArrayList<Integer> projectList = new ArrayList<Integer>();
        ArrayList<Type> typeList = new ArrayList<Type>();
        
        projectList.add(1);
        typeList.add(Type.STRING_TYPE);
        
        Project sName = new Project(projectList, typeList, j2);
        
        System.out.println("Query results:");
        
        sName.open();
        while (sName.hasNext()){
        	Tuple t = sName.next();
        	System.out.println(t.toString());
        }

    }

}