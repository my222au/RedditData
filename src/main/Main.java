package main;


import java.sql.*;

/**
 * .
 * TODO 1  Import and parse  the  json file
 * TODO 2  set up att database and then covert the json file to
 * TODO 3 Create a E/R MODEL
 }
  */





public class Main {
    public static void main(String[] args) {

        Connection c = null;
        try {
            c = DriverManager.getConnection("jdbc:sqlite:2dv513.db");
            Statement s = c.createStatement();
            s.setQueryTimeout(30);  // set timeout to 30 sec.
            ResultSet rs = s.executeQuery("select * from subs limit 10");
            while(rs.next()) {
                System.out.println("name = " + rs.getString("name"));
                System.out.println("id = " + rs.getInt("id"));
            }
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
