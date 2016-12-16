package main;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;


public class DatabaseHelper {
    private Connection connection;
    private Statement statement;
    //   private PreparedStatement psNameTable;
    private PreparedStatement psSubTable;
    private PreparedStatement psCommentTable;

    private long start;


    public DatabaseHelper() {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:Reddit.db");  // Sets up connection
            statement = connection.createStatement();
            connection.setAutoCommit(false);    // Manual commit

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method where we create our tables
     */
    public void createTables() {

        try {
            // WITHOUT CONSTRAINTS
//            statement.execute("CREATE TABLE Sub (subreddit TEXT, subreddit_id TEXT, UNIQUE(subreddit, subreddit_id))");
//            statement.execute("CREATE TABLE Comment (id TEXT, parent_id TEXT, link_id TEXT, name TEXT, " +
//                      "author TEXT, body TEXT, subreddit TEXT, score INTEGER, created_utc TEXT)");

            // WITH CONSTRAINTS
            statement.execute("CREATE TABLE Sub (subreddit TEXT PRIMARY KEY, subreddit_id TEXT UNIQUE, UNIQUE(subreddit, subreddit_id))");
            statement.execute("CREATE TABLE Comment (id TEXT PRIMARY KEY, parent_id TEXT, link_id TEXT, name TEXT CHECK(name LIKE 't1_%'), " +
                    "author TEXT NOT NULL, body TEXT NOT NULL, subreddit TEXT NOT NULL, score INTEGER NOT NULL, created_utc TEXT NOT NULL, FOREIGN KEY(subreddit) REFERENCES Sub(subreddit) )");

        } catch (SQLException e) {
            System.out.println("Failed while creating the table ");
            e.printStackTrace();
        }
    }


    // test to save the  read
    public void printFromDataBase(String SQLstatement, int num) {

        start = System.currentTimeMillis(); // Start timer to later calculate time it takes.

        ResultSet rs = null;
        try {
            rs = statement.executeQuery(SQLstatement);
            while (rs.next()) {
                System.out.println(rs.getString(num));

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("\n\nTime: " + (System.currentTimeMillis() - start) + "ms\n\n"); // Ends timer
    }

    public ResultSet getResultSet(String SQLStatement) {

        start = System.currentTimeMillis(); // Start timer to later calculate time it takes.

        ResultSet rs = null;
        try {
            rs = statement.executeQuery(SQLStatement);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("\n\nTime: " + (System.currentTimeMillis() - start) + "ms\n\n"); // Ends timer

        return rs;
    }


    public void closeConnection() {
        if (connection != null) {
            try {

                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public void saveToDataBase(String path) {

        BufferedReader bufferedReader = null;
        int lineCount = 0;      // int to count what line we are on
        int batchSize = 10000;  // Number of lines to commit at the same time
        long start;
        long end;
        createTables();

        try {
            bufferedReader = new BufferedReader(new FileReader(path));
            start = System.currentTimeMillis(); // Start timer to later calculate time it takes.
            String line;
            JSONObject jsonObject;

            // Creates are statements where '?' will be our values WITHOUT Constraints
            psSubTable = connection.prepareStatement("INSERT OR IGNORE INTO Sub (subreddit, subreddit_id) VALUES (?,?)");
            psCommentTable = connection.prepareStatement("INSERT INTO Comment (id, parent_id, link_id, name, author, body, subreddit, score, created_utc) VALUES (?,?,?,?,?,?,?,?,?)");

            while ((line = bufferedReader.readLine()) != null) {

                        jsonObject = new JSONObject(line);   // Creates JSON Object

                        psSubTable.setString(1, jsonObject.getString("subreddit"));
                        psSubTable.setString(2, jsonObject.getString("subreddit_id"));

                        psCommentTable.setString(1, jsonObject.getString("id"));
                        psCommentTable.setString(2, jsonObject.getString("parent_id"));
                        psCommentTable.setString(3, jsonObject.getString("link_id"));
                        psCommentTable.setString(4, jsonObject.getString("name"));
                        psCommentTable.setString(5, jsonObject.getString("author"));
                        psCommentTable.setString(6, jsonObject.getString("body"));
                        psCommentTable.setString(7, jsonObject.getString("subreddit"));
                        psCommentTable.setInt(8, jsonObject.getInt("score"));
                        psCommentTable.setString(9, jsonObject.getString("created_utc"));

                        psSubTable.addBatch();
                        psCommentTable.addBatch();

                    if (++lineCount % batchSize == 0) {    // Executes when lineCount reaches batchSize
                        psCommentTable.executeBatch();
                        psSubTable.executeBatch();
                        connection.commit();
                    }
                }


            end = (System.currentTimeMillis() - start); // Ends timer
            System.out.println(end);

            closeConnection();   // Closes connection

        }  catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }  finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}


