package main;

import com.sun.tools.doclets.formats.html.SourceToHTMLConverter;
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

    private static DatabaseHelper db = new DatabaseHelper();



    public static void main(String[] args) {

     //   saveToDataBase(); // Comment this if database is already created and data is imported

    //    printNumCommentsSpecificUser("Captain-Obvious");
    //    printNumLolComments();
    //    printNumCommentsSpecificSubredditPerDay("politics");

        printSubrettidsOfSpecificLinkID("t3_5yll6");

    }

    /**
     * 4. Users that commented on a specific link has also posted to which subreddits?
     * @param link_id
     */
    private static void printSubrettidsOfSpecificLinkID(String link_id) {
        ResultSet rs = db.getResultSet("SELECT author FROM Comment Where link_id = '" + link_id + "'");

        try {
            while(rs.next()) {

                int i=1;
                int j=1;
                ResultSet subreddit_ids = db.getResultSet("SELECT subreddit_id FROM Comment Where author = '" + rs.getString(i) + "'");

                while(subreddit_ids.next()) {
                    System.out.println(subreddit_ids.getString(1));
             //       j++;
                }

                System.out.println("\n\n");

                i++;

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
    private static void printNumCommentsSpecificUser(String user) {
        db.readFromDataBase("SELECT count(body) FROM Comment Where  author = '" + user + "'",1);  // Here is where you select what to read
        // Right now we get number of comments (count(body)), from author 'eggnogdog'.
    }

    private static void printNumCommentsSpecificSubredditPerDay(String subreddit) {
        int startTimer = 0;
        int postCounter = 0;
        final int secondsPerDay = 86400;
        int days = 0;
        double average = 0;

        ResultSet rsSub = db.getResultSet("Select subreddit_id FROM Sub Where subreddit = '" + subreddit + "'");
        String subreddit_id = null;
        try {
            subreddit_id = rsSub.getString(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        ResultSet rs = db.getResultSet("SELECT created_utc FROM Comment Where subreddit_id = '" + subreddit_id + "'");

        try {
            startTimer = rs.getInt(1);
            while (rs.next()) {

                postCounter++;

                if(rs.getInt(1) - startTimer >= secondsPerDay) {
                    startTimer = rs.getInt(1);
                    days++;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if(days != 0) {
                average = postCounter/days;
        }
        System.out.println("Total posts: " + postCounter + ", Over " + days + " days.");
        System.out.println(average + " is posted on average per day on this subreddit");

    }

    private static void printNumLolComments() {
        db.readFromDataBase("SELECT count(body) FROM Comment WHERE body LIKE '%lol%'",1);
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



