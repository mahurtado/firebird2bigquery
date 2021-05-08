package com.manolo.firebase2bq;

import java.sql.*;

public class HelloFirebird {

  public static final String DEFAULT_FIREBIRD_PORT = "3050";

  public static void main(String[] args) throws Exception {

    String firebirdAddress = System.getProperty("firebirdAddress");
    int firebirdPort = Integer.parseInt(System.getProperty("firebirdPort", DEFAULT_FIREBIRD_PORT));
    String firebirdDatabase = System.getProperty("firebirdDatabase");
    String firebirdUser = System.getProperty("firebirdUser");
    String firebirdPassword = System.getProperty("firebirdPassword");

    String query = System.getProperty("query");

    String connUrl = "jdbc:firebirdsql://" + firebirdAddress + ":" + firebirdPort + "/" + firebirdDatabase;

    System.out.println("Connecting to " + connUrl);
    System.out.println("with " + firebirdUser + "/" + firebirdPassword);

    Class.forName("org.firebirdsql.jdbc.FBDriver"); 
    /*Connection connection = DriverManager.getConnection(
        "jdbc:firebirdsql://35.195.27.120:3050/MELT_SHOP",
        "SYSDBA", "jefe$$66"); */
    Connection connection = DriverManager.getConnection(connUrl, firebirdUser, firebirdPassword); 

    Statement stmt = connection.createStatement(); 

    /*String sql = "SELECT FIRST 1 MEAS_LOC_ID, MEAS_LOC_SITE, GMT_EVENT,  NUMBER_OF_SAMPLES , ASSOC_RPM_IN_HZ FROM SP_TIME_DATA";
    ResultSet rs = stmt.executeQuery(sql);
    while(rs.next()){
       System.out.println("MEAS_LOC_ID: " + rs.getInt("MEAS_LOC_ID"));
       System.out.println("MEAS_LOC_SITE: " + rs.getInt("MEAS_LOC_SITE"));
       System.out.println("GMT_EVENT: " + rs.getString("GMT_EVENT"));
       System.out.println("NUMBER_OF_SAMPLES: " + rs.getInt("NUMBER_OF_SAMPLES"));
       System.out.println("ASSOC_RPM_IN_HZ: " + rs.getDouble("ASSOC_RPM_IN_HZ"));
    }*/

    System.out.println("Running query: " + query);    
    ResultSet rs = stmt.executeQuery(query);
    ResultSetMetaData metadata = rs.getMetaData();
    int columnCount = metadata.getColumnCount();
    while(rs.next()){
      System.out.println("Row -------------------------");
      for (int i = 1; i <= columnCount; i++) {
        System.out.println(metadata.getColumnName(i) + ": " + rs.getObject(i));          
      }
    }
    rs.close();
  }
}