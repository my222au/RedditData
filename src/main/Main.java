package main;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.sql.*;

public class Main {
    private static long start;
    private static long end;
    private  static  Statement statement;
    private static  Connection connection;
    private static  PreparedStatement preparedStatement;



    public static void main(String[] args) {

     //   saveToDataBase(); // Comment this if database is already created and data is imported
        readFromDataBase();
    }


    /**
     * Method to import data to/create database
     */
    private static void saveToDataBase() {
        readFile();
    }

    /**
     * Method to read data form database
     */
    private static void readFromDataBase() {
        DatabaseHelper db = new DatabaseHelper();
        db.readFromDataBase("SELECT count(body)  FROM Comment Where  author = 'eggnogdog'",1);  // Here is where you select what to read
        // Right now we get number of comments (count(body)), from author 'eggnogdog'.
    }



    /****
     * Reads the JSON file and saves the data to string
     */
    private static void readFile() {
        BufferedReader bufferedReader = null;
        int lineCount = 0;      // int to count what line we are on
        int batchSize = 10000;  // Number of lines to commit at the same time
        DatabaseHelper db = new DatabaseHelper();
        db.createTables();

        try {


            bufferedReader = new BufferedReader(new FileReader("/Users/db/RC_2007_10"));
            start = System.currentTimeMillis(); // Start timer to later calculate time it takes.
            String line;

            while ((line = bufferedReader.readLine())!= null) {
                try {
                    JSONObject jsonObject = new JSONObject(line);   // Creates JSON Object

                    // Calls method with all tuple-key-names
                    try {
                        db.insert(jsonObject.getString("id"),
                                jsonObject.getString("parent_id"),
                                jsonObject.getString("link_id"),
                                jsonObject.getString("name"),
                                jsonObject.getString("author"),
                                jsonObject.getString("body"),
                                jsonObject.getString("subreddit_id"),
                                jsonObject.getString("subreddit"),
                                jsonObject.getInt("score"),
                                jsonObject.getString("created_utc")
                        );

                    } catch (JSONException e1) {
                        e1.printStackTrace();
                    }

                } catch (JSONException e1) {
                    e1.printStackTrace();
                }

                if(++lineCount % batchSize ==  0 ) {    // Executes when lineCount reaches batchSize
                   db.execute();

                }
                db.execute();

            }
               db.connectionCommit();

            end = (System.currentTimeMillis() - start); // Ends timer
            System.out.println(end);

            db.closeConnection();   // Closes connection

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void execute() {
        try {
            preparedStatement.executeBatch();
            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
//    public static void executeBatch(int batchSize, int lineCount) {
//        if(lineCount % batchSize == 0)
//            try {
//                System.out.println("excutetd Batch ");
//                int [] resluts  =    preparedStatement.executeBatch();
//                connection.commit();
//
//                System.out.println(resluts);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//
//
//    }


//    public static void readFromDataBase(String SQLstatment, String coloumName){
//
//        ResultSet resultSet;
//        try {
//            resultSet = statement.executeQuery(SQLstatment);
//            while (resultSet.next()) {
//                // read the result set
//                System.out.println(resultSet.getString(coloumName));
//
//            }
//
//
//
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//
//


}



