package main;
import java.sql.*;

/**
 * .
 * TODO 1  Import and parse  the  json file
 * TODO 2  set up att database and then covert the json file to
 * TODO 3 Create a E/R MODEL ff
 }
  */





public class Main {
    public static void main(String[] args) {

        Connection c = null;
        try {
            c = DriverManager.getConnection("jdbc:sqlite:Reddit.db");
            Statement s = c.createStatement();
            s.execute("CREATE TABLE Sub(subreddit_id TEXT, subreddit TEXT)");
            // create the other tables
            s.execute("CREATE TABLE Name(Id TEXT, name TEXT)");
            s.execute("CREATE TABLE Comment(id TEXT, parent_id TEXT, link_id TEXT, author TEXT, body TEXT, subreddit_id TEXT, score INTEGER, created_utc TEXT)");

        }
        catch(SQLException e) {
            System.err.println(e.getMessage());
        }
        finally {
            try {
                if(c != null)
                    c.close();
            }
            catch(SQLException e) {
                System.err.println(e);
            }
        }


    }
}
