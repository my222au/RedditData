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

            ResultSet rs = s.executeQuery("SELECT * FROM subs LIMIT 10");
            while(rs.next()) {
                System.out.println("id = " + rs.getInt("id"));
                System.out.println("name = " + rs.getString("name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
