package main;

import java.sql.*;

public class Main {

    private static String file1 = "/Users/db/RC_2007_10";
    private static String file2 = "/Users/db/RC_2011-07";
    private static String file3 = "/Users/db/RC_2012-12";

    private static DatabaseHelper db = new DatabaseHelper();



    public static void main(String[] args) {

//        db.saveToDataBase(file1); // Comment this if database is already created and data is imported

        printNumCommentsSpecificUser("rahhh");      // 1
//        printNumLolComments();                                // 2
//        printNumCommentsSpecificSubredditPerDay("politics");  // 3
//        printSubrettidsOfSpecificLinkID("t3_5ykb7");  // 4
//        printMaxAndMinUserScores();               // 5
//        printMaxAndMinSubredditScores();          // 6
//        printUsersWhoInteractedWith("ejcross");   // 7
//        printUsersWhoPostedOnOnlyOneSubreddit();  // 8

    }

    /**
     * 1. How many comments have a specific user posted?
     */
    private static void printNumCommentsSpecificUser(String user) {
        db.printFromDataBase("SELECT count(body) FROM Comment Where  author = '" + user + "'",1);
        // Right now we get number of comments (count(body)), from author 'user'.
    }

    /**
     * 2. How many comments does a specific subreddit get per day?
     */
    private static void printNumCommentsSpecificSubredditPerDay(String subreddit) {
        int startTimer;
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
        db.printFromDataBase("SELECT count(body) FROM Comment WHERE body LIKE '%lol%'",1);
    }

    /**
     * 4. Users that commented on a specific link has also posted to which subreddits?
     */
    private static void printSubrettidsOfSpecificLinkID(String link_id) {

        ResultSet rs = db.getResultSet("SELECT * FROM (SELECT DISTINCT subreddit FROM Comment WHERE author = (SELECT author FROM Comment  where link_id = '"+link_id+"'))");

        try {
            while(rs.next()) {
                System.out.println(rs.getString(1));
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
}



