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
            c = DriverManager.getConnection("jdbc:sqlite:Riddet.db");
            Statement s = c.createStatement();
            s.execute("CREATE TABEL SUB(subreddit_id TEXT, subreddit TEXT");
            // create the other tables

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
 // create a tabe l




}
