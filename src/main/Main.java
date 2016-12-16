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

    private static String file1 = "/Users/db/RC_2007_10";
    private static String file2 = "/Users/db/RC_2011-07";
    private static String file3 = "/Users/db/RC_2012-12";

    private static DatabaseHelper db = new DatabaseHelper();



    public static void main(String[] args) {

//        saveToDataBase(); // Comment this if database is already created and data is imported

//        printNumCommentsSpecificUser("Captain-Obvious");
//        printNumLolComments();
//        printNumCommentsSpecificSubredditPerDay("politics");
//        printSubrettidsOfSpecificLinkID("t3_5ykb7");
//        printMaxAndMinUserScores();
//        printMaxAndMinSubredditScores();
//        printUsersWhoPostedOnOnlyOneSubreddit();

        printUsersWhoInteractedWith("ejcross");
    }

    /**
     * 1. How many comments have a specific user posted?
     */
    private static void printNumCommentsSpecificUser(String user) {
        db.readFromDataBase("SELECT count(body) FROM Comment Where  author = '" + user + "'",1);
        // Right now we get number of comments (count(body)), from author 'user'.
    }

    /**
     * 2. How many comments does a specific subreddit get per day?
     */
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

    /**
     * 3. How many comments include the word ‘lol’?
     */
    private static void printNumLolComments() {
        db.readFromDataBase("SELECT count(body) FROM Comment WHERE body LIKE '%lol%'",1);
    }

    /**
     * 4. Users that commented on a specific link has also posted to which subreddits?
     * @param link_id
     */
    private static void printSubrettidsOfSpecificLinkID(String link_id) {

        ResultSet rs = db.getResultSet("SELECT * From (SELECT distinct subreddit FROM Comment WHERE author = (SELECT DISTINCT author FROM Comment  where link_id = '"+link_id+"'))");

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
     * 5. Which users have the highest and lowest combined scores? (combined as the sum of all scores)
     */
    private static void printMaxAndMinUserScores() {

        ResultSet rs = db.getResultSet("SELECT author, MIN(sum_score) FROM( SELECT author, SUM(score) AS sum_score FROM Comment WHERE author IS NOT '[deleted]' GROUP BY author)" +
                "UNION SELECT author, MAX(sum_score) FROM( SELECT author, SUM(score) AS sum_score FROM Comment WHERE author IS NOT '[deleted]' GROUP BY author)");

        try {
            while(rs.next()) {
                System.out.println(rs.getString(1) + "\tSum: " + rs.getString(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 6. Which subreddits have the highest and lowest scored comments?
     */
    private static void printMaxAndMinSubredditScores() {

        // reddit.com does not count as a subreddit
        ResultSet rs = db.getResultSet("SELECT subreddit, MAX(max_score) FROM( SELECT subreddit, SUM(score) AS max_score FROM Comment GROUP BY subreddit)" +
                "UNION SELECT subreddit, MIN(min_score) FROM( SELECT subreddit, SUM(score) AS min_score FROM Comment GROUP BY subreddit)");

        try {
            while(rs.next()) {
                System.out.println(rs.getString(1) + "\tScore: " + rs.getString(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 7. Given a specific user, list all the users he or she has potentially interacted with (i.e., everyone who as
     *    commented on a link that the specific user has commented on).
     */
    private static void printUsersWhoInteractedWith(String user) {

       ResultSet rs = db.getResultSet("SELECT DISTINCT author FROM Comment WHERE author IS NOT '"+user+"' AND link_id IN" +
                " (SELECT DISTINCT link_id FROM Comment WHERE author = '"+user+"' ORDER BY author)");

        try {
            while(rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 8. Which users has only posted to a single subreddit?
     */
    private static void printUsersWhoPostedOnOnlyOneSubreddit() {
        ResultSet rs = db.getResultSet("SELECT * FROM ( SELECT DISTINCT author, subreddit FROM Comment  GROUP BY subreddit HAVING count(subreddit) == 1)");
//        WHERE subreddit IS NOT 'reddit.com'
        try {
            while(rs.next()) {
                System.out.println(rs.getString(1) + "\t Subreddit: " + rs.getString(2));
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


            bufferedReader = new BufferedReader(new FileReader(file1));
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



