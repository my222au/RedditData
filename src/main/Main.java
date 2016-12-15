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

//        saveToDataBase(); // Comment this if database is already created and data is imported

    //    printNumCommentsSpecificUser("Captain-Obvious");
    //    printNumLolComments();
//        printNumCommentsSpecificSubredditPerDay("politics");
//        printSubrettidsOfSpecificLinkID("t3_5ykb7");

        printMaxAndMinScores();

    }

    private static void printMaxAndMinScores() {
//       ResultSet rs = db.getResultSet("SELECT (SELECT MAX(score) FROM Comment), (SELECT MIN(score) FROM Comment)");
        ResultSet rs = db.getResultSet("SELECT author, MIN(sum_score) FROM( SELECT author, SUM(score) AS sum_score FROM Comment GROUP BY author)" +
                "UNION SELECT author, MAX(sum_score) FROM( SELECT author, SUM(score) AS sum_score FROM Comment GROUP BY author)");

        try {
            while(rs.next()) {
                System.out.println(rs.getString(1) + "\tSum: " + rs.getString(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 4. Users that commented on a specific link has also posted to which subreddits?
     * @param link_id
     */
    private static void printSubrettidsOfSpecificLinkID(String link_id) {

//       ResultSet rs = db.getResultSet("SELECT DISTINCT subreddit FROM Comment WHERE author IN (SELECT DISTINCT author FROM Comment WHERE link_id = '" + link_id + "')");
//         ResultSet rs = db.getResultSet("SELECT subreddit from Sub (SELECT DISTINCT author FROM Comment where link_id = 't3_5zcbl'), (SELECT DISTINCT count(author) FROM Comment where link_id = 't3_5zcbl')");

       ResultSet rs = db.getResultSet("SELECT * From (SELECT distinct subreddit FROM Comment WHERE author = (SELECT DISTINCT author FROM Comment  where link_id = 't3_5zcbl'))");

        try {

            String users[] = new String[rs.getInt(2)];
            int i=0;


            while(rs.next()) {
                System.out.println(rs.getString(1));

                users[i] = rs.getString(1);
                i++;
            }

            for(int j=0; j<users.length; j++) {
                ResultSet rs2 = db.getResultSet("SELECT DISTINCT subreddit FROM Comment where author = '" + users[j] +"'");

                while(rs2.next()) {
                    System.out.println(rs2.getString(1));
                }
                System.out.println("----------------------------------------------");
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
        // Right now we get number of comments (count(body)), from author 'user'.
    }

    private static void printNumCommentsSpecificSubredditPerDay(String subreddit) {
        int startTimer = 0;
        int postCounter = 0;
        final int secondsPerDay = 86400;
        int days = 0;
        double average = 0;


        ResultSet rs = db.getResultSet("SELECT created_utc FROM Comment Where subreddit = '" + subreddit + "'");

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
}



