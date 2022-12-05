/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.lang.Math;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */
public class Retail {

   // reference to physical database connection.
   private Connection _connection = null;

   // handling the keyboard inputs through a BufferedReader
   // This variable can be global for convenience.
   static BufferedReader in = new BufferedReader(
                                new InputStreamReader(System.in));

   private enum ACCESS_LEVEL {
      NONE(0),
      CUSTOMER(1),
      MANAGER(2),
      ADMIN(3);

      public final int val;

      private ACCESS_LEVEL(int _val) {
         this.val = _val;
      }
   }

   private ACCESS_LEVEL access_level = ACCESS_LEVEL.NONE;
   private String userId = "";

   /**
    * Creates a new instance of Retail shop
    *
    * @param hostname the MySQL or PostgreSQL server hostname
    * @param database the name of the database
    * @param username the user name used to login to the database
    * @param password the user login password
    * @throws java.sql.SQLException when failed to make a connection.
    */
   public Retail(String dbname, String dbport, String user, String passwd) throws SQLException {

      this.access_level = ACCESS_LEVEL.NONE;

      System.out.print("Connecting to database...");
      try{
         // constructs the connection URL
         String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
         System.out.println ("Connection URL: " + url + "\n");

         // obtain a physical connection
         this._connection = DriverManager.getConnection(url, user, passwd);
         System.out.println("Done");
      }catch (Exception e){
         System.err.println("Error - Unable to Connect to Database: " + e.getMessage() );
         System.out.println("Make sure you started postgres on this machine");
         System.exit(-1);
      }//end catch
   }//end Retail

   // Method to calculate euclidean distance between two latitude, longitude pairs.
   public double calculateDistance (double lat1, double long1, double lat2, double long2){
      double t1 = (lat1 - lat2) * (lat1 - lat2);
      double t2 = (long1 - long2) * (long1 - long2);
      return Math.sqrt(t1 + t2);
   }
   /**
    * Method to execute an update SQL statement.  Update SQL instructions
    * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
    *
    * @param sql the input SQL string
    * @throws java.sql.SQLException when update failed
    */
   public void executeUpdate (String sql) throws SQLException {
      // creates a statement object
      Statement stmt = this._connection.createStatement ();

      // issues the update instruction
      stmt.executeUpdate (sql);

      // close the instruction
      stmt.close ();
   }//end executeUpdate

   /**
    * Method to execute an input query SQL instruction (i.e. SELECT).  This
    * method issues the query to the DBMS and outputs the results to
    * standard out.
    *
    * @param query the input query string
    * @return the number of rows returned
    * @throws java.sql.SQLException when failed to execute the query
    */
   public int executeQueryAndPrintResult (String query) throws SQLException {
      // creates a statement object
      Statement stmt = this._connection.createStatement ();

      // issues the query instruction
      ResultSet rs = stmt.executeQuery (query);

      /*
       ** obtains the metadata object for the returned result set.  The metadata
       ** contains row and column info.
       */
      ResultSetMetaData rsmd = rs.getMetaData ();
      int numCol = rsmd.getColumnCount ();
      int rowCount = 0;

      // iterates through the result set and output them to standard out.
      boolean outputHeader = true;
      while (rs.next()){
		 if(outputHeader){
			for(int i = 1; i <= numCol; i++){
			System.out.print(rsmd.getColumnName(i) + "\t");
			}
			System.out.println();
			outputHeader = false;
		 }
         for (int i=1; i<=numCol; ++i)
            System.out.print (rs.getString (i) + "\t");
         System.out.println ();
         ++rowCount;
      }//end while
      stmt.close ();
      return rowCount;
   }//end executeQuery

   /**
    * Method to execute an input query SQL instruction (i.e. SELECT).  This
    * method issues the query to the DBMS and returns the results as
    * a list of records. Each record in turn is a list of attribute values
    *
    * @param query the input query string
    * @return the query result as a list of records
    * @throws java.sql.SQLException when failed to execute the query
    */
   public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException {
      // creates a statement object
      Statement stmt = this._connection.createStatement ();

      // issues the query instruction
      ResultSet rs = stmt.executeQuery (query);

      /*
       ** obtains the metadata object for the returned result set.  The metadata
       ** contains row and column info.
       */
      ResultSetMetaData rsmd = rs.getMetaData ();
      int numCol = rsmd.getColumnCount ();
      int rowCount = 0;

      // iterates through the result set and saves the data returned by the query.
      boolean outputHeader = false;
      List<List<String>> result  = new ArrayList<List<String>>();
      while (rs.next()){
        List<String> record = new ArrayList<String>();
		for (int i=1; i<=numCol; ++i)
			record.add(rs.getString (i));
        result.add(record);
      }//end while
      stmt.close ();
      return result;
   }//end executeQueryAndReturnResult

   /**
    * Method to execute an input query SQL instruction (i.e. SELECT).  This
    * method issues the query to the DBMS and returns the number of results
    *
    * @param query the input query string
    * @return the number of rows returned
    * @throws java.sql.SQLException when failed to execute the query
    */
   public int executeQuery (String query) throws SQLException {
       // creates a statement object
       Statement stmt = this._connection.createStatement ();

       // issues the query instruction
       ResultSet rs = stmt.executeQuery (query);

       int rowCount = 0;

       // iterates through the result set and count nuber of results.
       while (rs.next()){
          rowCount++;
       }//end while
       stmt.close ();
       return rowCount;
   }

   /**
    * Method to fetch the last value from sequence. This
    * method issues the query to the DBMS and returns the current
    * value of sequence used for autogenerated keys
    *
    * @param sequence name of the DB sequence
    * @return current value of a sequence
    * @throws java.sql.SQLException when failed to execute the query
    */
   public int getCurrSeqVal(String sequence) throws SQLException {
	Statement stmt = this._connection.createStatement ();

	ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
	if (rs.next())
		return rs.getInt(1);
	return -1;
   }

   /**
    * Method to close the physical connection if it is open.
    */
   public void cleanup(){
      try{
         if (this._connection != null){
            this._connection.close ();
         }//end if
      }catch (SQLException e){
         // ignored.
      }//end try
   }//end cleanup

   /**
    * The main execution method
    *
    * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
    */
   public static void main (String[] args) {
      if (args.length != 3) {
         System.err.println (
            "Usage: " +
            "java [-classpath <classpath>] " +
            Retail.class.getName () +
            " <dbname> <port> <user>");
         return;
      }//end if

      Greeting();
      Retail esql = null;
      try{
         // use postgres JDBC driver.
         Class.forName ("org.postgresql.Driver").newInstance ();
         // instantiate the Retail object and creates a physical
         // connection.
         String dbname = args[0];
         String dbport = args[1];
         String user = args[2];
         esql = new Retail (dbname, dbport, user, "");

         boolean keepon = true;
         while(keepon) {
            // These are sample SQL statements
            System.out.println("MAIN MENU");
            System.out.println("---------");
            System.out.println("1. Create user");
            System.out.println("2. Log in");
            System.out.println("9. < EXIT");
            String authorisedUser = null;
            switch (readChoice()){
               case 1: CreateUser(esql); break;
               case 2: authorisedUser = LogIn(esql); break;
               case 9: keepon = false; break;
               default : System.out.println("Unrecognized choice!"); break;
            }//end switch
            if (authorisedUser != null) {
              boolean usermenu = true;
              while(usermenu) {
                System.out.println("MAIN MENU");
                System.out.println("---------");
                System.out.println("1. View Stores within 30 miles");
                System.out.println("2. View Product List");
                System.out.println("3. Place a Order");
                System.out.println("4. View 5 recent orders");

                //the following functionalities basically used by managers
                System.out.println("5. Update Product");
                System.out.println("6. View 5 recent Product Updates Info");
                System.out.println("7. View 5 Popular Items");
                System.out.println("8. View 5 Popular Customers");
                System.out.println("9. Place Product Supply Request to Warehouse");

                System.out.println(".........................");
                System.out.println("20. Log out");
                switch (readChoice()){
                   case 1: viewStores(esql); break;
                   case 2: viewProducts(esql); break;
                   case 3: placeOrder(esql); break;
                   case 4: viewRecentOrders(esql); break;
                   case 5: updateProduct(esql); break;
                   case 6: viewRecentUpdates(esql); break;
                   case 7: viewPopularProducts(esql); break;
                   case 8: viewPopularCustomers(esql); break;
                   case 9: placeProductSupplyRequests(esql); break;

                   case 20:
                     // Reset access level on logout
                     esql.access_level = ACCESS_LEVEL.NONE;
                     esql.userId = "";
                     usermenu = false;
                     break;
                   default : System.out.println("Unrecognized choice!"); break;
                }
              }
            }
         }//end while
      }catch(Exception e) {
         System.err.println (e.getMessage ());
      }finally{
         // make sure to cleanup the created table and close the connection.
         try{
            if(esql != null) {
               System.out.print("Disconnecting from database...");
               esql.cleanup ();
               System.out.println("Done\n\nBye !");
            }//end if
         }catch (Exception e) {
            // ignored.
         }//end try
      }//end try
   }//end main

   public static void Greeting(){
      System.out.println(
         "\n\n" +
         "*******************************************************\n" +
         "                     User Interface                     \n" +
         "*******************************************************\n");
   }//end Greeting

   /*
    * Reads the users choice given from the keyboard
    * @int
    **/
   public static int readChoice() {
      int input;
      // returns only if a correct value is given.
      do {
         System.out.print("Please make your choice: ");
         try { // read the integer, parse it and break.
            input = Integer.parseInt(in.readLine());
            break;
         }catch (Exception e) {
            System.out.println("Your input is invalid!");
            continue;
         }//end try
      }while (true);
      return input;
   }//end readChoice

   /*
    * Creates a new user
    **/
   public static void CreateUser(Retail esql){
      try{
         System.out.print("\tEnter name: ");
         String name = in.readLine();
         System.out.print("\tEnter password: ");
         String password = in.readLine();
         System.out.print("\tEnter latitude: ");
         String latitude = in.readLine();       //enter lat value between [0.0, 100.0]
         System.out.print("\tEnter longitude: ");  //enter long value between [0.0, 100.0]
         String longitude = in.readLine();

         String type="Customer";

			String query = String.format("INSERT INTO USERS (name, password, latitude, longitude, type) VALUES ('%s','%s', %s, %s,'%s')", name, password, latitude, longitude, type);

         esql.executeUpdate(query);
         System.out.println ("User successfully created!");
      }catch(Exception e){
         System.err.println (e.getMessage ());
      }
   }//end CreateUser


   /*
    * Check log in credentials for an existing user
    * @return User login or null is the user does not exist
    **/
   public static String LogIn(Retail esql){
      try{
         String name = "";
         String password = "";
         System.out.print("\tEnter name: ");
         name = in.readLine();
         System.out.print("\tEnter password: ");
         password = in.readLine();

         String query = String.format("SELECT * FROM USERS WHERE name = '%s' AND password = '%s'", name, password);

         // We want to extract user type from query results
         int userNum = esql.executeQueryAndPrintResult(query); // for debugging
         List<List<String>> qResults = esql.executeQueryAndReturnResult(query);
         if (!qResults.isEmpty()) {
            // Check user type and adjust access level
            String LoginType = qResults.get(0).get(5).trim();
            esql.userId = qResults.get(0).get(1).trim();
            switch (LoginType) {
               case "customer":
                  esql.access_level = ACCESS_LEVEL.CUSTOMER;
                  break;
               case "manager":
                  esql.access_level = ACCESS_LEVEL.MANAGER;
                  break;
               case "admin":
                  esql.access_level = ACCESS_LEVEL.ADMIN;
                  break;
               default:
                  throw new Exception("Unknown access type: " + LoginType);
                  //break;
            }

            return name;
         }
         return null;
      }catch(Exception e){
         System.err.println (e.getMessage ());
         return null;
      }
   }//end

// Rest of the functions definition go in here

  //print stores within 30 miles of user
   public static void viewStores(Retail esql) {
    if (esql.access_level.val == 0) { System.out.println("Error: FORBIDDEN"); return; }

     try{
       String query = String.format("SELECT name, latitude, longitude FROM USERS WHERE name = '%s'", esql.userId);
       List<List<String>> result = esql.executeQueryAndReturnResult(query);
       double lat1 = Double.parseDouble(result.get(0).get(1).trim());
       double long1 = Double.parseDouble(result.get(0).get(2).trim());


        query = String.format("SELECT name, latitude, longitude FROM Store");
        result = esql.executeQueryAndReturnResult(query);


        System.out.println("");
        System.out.println("Stores Within 30 Miles:");
        for (int i = 0; i < result.size(); i++){
          double lat2 = Double.parseDouble(result.get(i).get(1));
          double long2 = Double.parseDouble(result.get(i).get(2));

          if (esql.calculateDistance(lat1, long1, lat2, long2) <= 30){
            System.out.println(result.get(i).get(0));
          }

       }
       System.out.println("");
       return;

     } catch(Exception e){
        System.err.println (e.getMessage ());
        return;
     }
   }

   //print all products given a store ID
   public static void viewProducts(Retail esql) {
      if (esql.access_level.val == 0) { System.out.println("Error: FORBIDDEN"); return; }

      try{
        String id = "";
        System.out.print("Enter store ID: ");
        id = in.readLine();

        String query = String.format("SELECT productName, numberOfUnits, pricePerUnit FROM Product WHERE storeID = '%s'", id);
        int result = esql.executeQueryAndPrintResult(query);

        if (result == 0){
          System.out.println("No matching store ID");
        }
        System.out.println("");
        return;

      } catch(Exception e){
         System.err.println (e.getMessage ());
         return;
      }


   }

   //make an order
   public static void placeOrder(Retail esql) {
      if (esql.access_level.val == 0) { System.out.println("Error: FORBIDDEN"); return; }


      try{
        String storeID = "";
        String prodName = "";
        String temp = "";
        int numUnits = 0;
        System.out.print("Enter store ID: ");
        storeID = in.readLine();
        System.out.print("Enter store product name: ");
        prodName = in.readLine();
        System.out.print("Enter store number of units: ");
        temp = in.readLine();
        numUnits = Integer.parseInt(temp);


        //get user info
        String query = String.format("SELECT name, latitude, longitude, userId FROM USERS WHERE name = '%s'", esql.userId);
        List<List<String>> result = esql.executeQueryAndReturnResult(query);
        double lat1 = Double.parseDouble(result.get(0).get(1).trim());
        double long1 = Double.parseDouble(result.get(0).get(2).trim());
        int userIDnum = Integer.parseInt(result.get(0).get(3).trim());

        //get store info
        //query = String.format("SELECT storeID, latitude, longitude FROM Store");
        query = String.format("SELECT storeID, latitude, longitude FROM Store WHERE storeID = '%s'", storeID);
        result = esql.executeQueryAndReturnResult(query);

        //check if store exists
        if (result.size() == 0){
         System.out.println("Error: store number " + storeID + " does not exist");
         System.out.println("");
         return;
        }

        double lat2 = Double.parseDouble(result.get(0).get(1));
        double long2 = Double.parseDouble(result.get(0).get(2));

        //check id store speccified is within 30 miles, if not return
        if (esql.calculateDistance(lat1, long1, lat2, long2) > 30){
         System.out.println("Error: store too far away");
         System.out.println("");
         return;
        }

        //validate product EXISTS
        query = String.format("SELECT numberOfUnits FROM Product WHERE storeID = '%s' AND productName = '%s'", storeID, prodName);
        result = esql.executeQueryAndReturnResult(query);
        if (result.size() == 0){
         System.out.println("Error: product name " + prodName + " does not exist at this store");
         System.out.println("");
         return;
        }

        //validate quantity doesnt exceed
        if (Integer.parseInt(result.get(0).get(0)) > numUnits || numUnits < 1){
         System.out.println("Error: invalid quantity");
         System.out.println("");
         return;
        }

        Timestamp ts = Timestamp.from(Instant.now());

        //update another table I think

        query = "INSERT INTO Orders (customerID, storeID, productName, unitsOrdered, orderTime)" +
          "VALUES ( '" + userIDnum + "', '" + storeID + "', '" + prodName + "', '"  + numUnits + "', '" + ts.toString() + "' );";
        System.out.println(query);
        esql.executeQuery(query);

        System.out.println("");
        return;

      } catch(Exception e){
         System.err.println (e.getMessage ());
         return;
      }



   }

   public static void viewRecentOrders(Retail esql) {
      if (esql.access_level.val == 0) { System.out.println("Error: FORBIDDEN"); return; }

      String query = "";

      switch (esql.access_level) {
         case CUSTOMER:
            System.out.println("***** Last 5 Orders *****");
            query = String.format("SELECT S.storeID, S.name, O.productName, O.unitsOrdered, O.orderTime " +
                                  "FROM STORES S, ORDERS O " +
                                  "WHERE S.storeID = O.storeID AND customerID = '%s' " +
                                  "ORDER BY O.orderTime DESC " +
                                  "LIMIT 5"
                                   , esql.userId);
            break;
         case MANAGER:
         case ADMIN:
            System.out.println("***** Orders *****");
            query = String.format("SELECT O.customerID, U.name, O.storeID, O.productName, O.orderTime " +
                                  "FROM USERS U, STORES S, ORDERS O " +
                                  "WHERE S.managerID = '%s' AND S.storeID = O.storeID AND U.userID = O.customerID" +
                                  "ORDER BY O.orderTime DESC"
                                   ,esql.userId);
            break;
         default:
            System.out.println("Unknown Access Level: " + esql.access_level.val);
            break;
      }

      int ResponseLength = 0;
      try {
         ResponseLength = esql.executeQueryAndPrintResult(query);
      } catch(Exception e){
         System.err.println (e.getMessage());
      }
      System.out.println(String.format("[%s Results]", ResponseLength));
   }

   public static void updateProduct(Retail esql) {
      if (esql.access_level.val < ACCESS_LEVEL.MANAGER.val) { System.out.println("Error: FORBIDDEN"); return; }

      try{
        String storeID = "";
        String prodName = "";
        String temp = "";
        int numUnits = -1;
        double ppu = -1;

        System.out.print("Enter store ID: ");
        storeID = in.readLine();

        String query = String.format("SELECT managerID FROM Store WHERE storeID = '%s'", storeID);
        List<List<String>> result = esql.executeQueryAndReturnResult(query);

        //check if store exists
        if (result.size() == 0){
         System.out.println("Error: store number " + storeID + " does not exist");
         System.out.println("");
         return;
        }

        //check if manager is current user if not admin
        if (Integer.parseInt(result.get(0).get(0).trim()) != esql.userId && esql.access_level.val != ACCESS_LEVEL.ADMIN.val){
         System.out.println("Error: you are not the manager of store " + storeID);
         System.out.println("");
         return;
        }

        System.out.print("Enter store product name: ");
        prodName = in.readLine();

        //validate product EXISTS
        query = String.format("SELECT numberOfUnits FROM Product WHERE storeID = '%s' AND productName = '%s'", storeID, prodName);
        result = esql.executeQueryAndReturnResult(query);
        if (result.size() == 0){
         System.out.println("Error: product name " + prodName + " does not exist at this store");
         System.out.println("");
         return;
        }

        //choose field(s) to update
        boolean miniMenu = true;
        boolean miniMenu2 = true;
        while (miniMenu){
          miniMenu2 = true;
          System.out.println("Choose a field to update: ");
          System.out.println("1. number of units");
          System.out.println("2. price per unit");
          System.out.println("3. exit");
          System.out.println("");
          switch (readChoice()){
             case 1:
                while (miniMenu2){
                  System.out.println("Enter new quantity of units");
                  temp = in.readLine();
                  if (Integer.parseInt(temp) < 0){
                    System.out.println("Unrecognized choice!");
                  }
                  else {
                    numUnits = Integer.parseInt(temp);
                    miniMenu2 = false;
                  }
                }
                break;
             case 2:
               while (miniMenu2){
                 System.out.println("Enter new price per unit");
                 temp = in.readLine();
                 if (Double.parseDouble(temp) < 0){
                   System.out.println("Unrecognized choice!");
                 }
                 else {
                   ppu = Double.parseDouble(temp);
                   miniMenu2 = false;
                 }
               }
               break;
             case 3:
               miniMenu = false;
               break;
             default : System.out.println("Unrecognized choice!"); break;
          }
        }


        //update query
        //Product and ProductUpdates tables will need to be updated accordingly if any updates take place
        if (numUnits == -1 && ppu == -1){
          return;
        }
        if (numUnits > -1){
          query = String.format("UPDATE Product SET numberOfUnits = %s WHERE storeID = '%s' AND productName = '%s'", numUnits, storeID, prodName);
          result = esql.executeQuery(query);
        }
        if (ppu > -1){
          query = String.format("UPDATE Product SET pricePerUnit = %s WHERE storeID = '%s' AND productName = '%s'", ppu, storeID, prodName);
          result = esql.executeQuery(query);
        }
        Timestamp ts = Timestamp.from(Instant.now());
        query = uery = String.format("INSERT INTO ProductUpdates (managerID, storeID, productName, updatedOn) VALUES ('%s','%s', '%s', %s)", esql.userId, storeID, prodName, ts.toString());
        result = esql.executeQuery(query)


      } catch(Exception e){
         System.err.println (e.getMessage ());
         return;
      }

   }
   public static void viewRecentUpdates(Retail esql) {
      if (esql.access_level.val < ACCESS_LEVEL.MANAGER.val) { System.out.println("Error: FORBIDDEN"); return; }

   }
   public static void viewPopularProducts(Retail esql) {
      if (esql.access_level.val < ACCESS_LEVEL.MANAGER.val) { System.out.println("Error: FORBIDDEN"); return; }

      System.out.println("***** Top 5 Popular Products *****");
      String query = String.format("SELECT O.productName " +
                            "FROM " +
                            "(SELECT O.productName, SUM(O.unitsOrdered) " +
                            "FROM ORDERS O, STORES S " +
                            "WHERE S.managerID = '%s' AND S.storeID = O.storeID " +
                            "ORDER BY SUM(O.unitsOrdered) DESC" +
                            ") " +
                            "LIMIT 5"
                            , esql.userId);

      int ResponseLength = 0;
      try {
         ResponseLength = esql.executeQueryAndPrintResult(query);
      } catch(Exception e){
         System.err.println (e.getMessage());
      }
      System.out.println(String.format("[%s Results]", ResponseLength));
   }
   public static void viewPopularCustomers(Retail esql) {
      if (esql.access_level.val < ACCESS_LEVEL.MANAGER.val) { System.out.println("Error: FORBIDDEN"); return; }

      System.out.println("***** Top 5 Customers *****");
      String query = String.format("SELECT U.name " +
                            "FROM " +
                            "(SELECT U.name, SUM(O.customerID) " +
                            "FROM USERS U, ORDERS O, STORES S " +
                            "WHERE S.managerID = '%s' AND S.storeID = O.storeID AND U.userID = O.customerID " +
                            "ORDER BY SUM(O.customerID) DESC" +
                            ") " +
                            "LIMIT 5"
                            , esql.userId);

      int ResponseLength = 0;
      try {
         ResponseLength = esql.executeQueryAndPrintResult(query);
      } catch(Exception e){
         System.err.println (e.getMessage());
      }
      System.out.println(String.format("[%s Results]", ResponseLength));

   }
   public static void placeProductSupplyRequests(Retail esql) {
      if (esql.access_level.val < ACCESS_LEVEL.MANAGER.val) { System.out.println("Error: FORBIDDEN"); return; }

   }

}//end Retail
