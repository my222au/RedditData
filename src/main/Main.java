package main;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * .
 * TODO 1  Import and parse  the  json file
 * TODO 2  set up att database and then covert the json file to
 * TODO 3 Create a E/R MODEL ff
 }
  */





public class Main {



        public static void main(String[] args)
        {
            Connection connection = null;
            try
            {
                // create a database connection
                connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
                Statement statement = connection.createStatement();
                statement.setQueryTimeout(30);  // set timeout to 30 sec.

                statement.executeUpdate("drop table if exists person");
                statement.executeUpdate("create table person (id integer, name string)");
                statement.executeUpdate("insert into person values(1, 'leo')");
                statement.executeUpdate("insert into person values(2, 'yui')");
                ResultSet rs = statement.executeQuery("select * from person");
                while(rs.next())
                {
                    // read the result set
                    System.out.println("name = " + rs.getString("name"));
                    System.out.println("id = " + rs.getInt("id"));
                }
            }
            catch(SQLException e)
            {
                // if the error message is "out of memory",
                // it probably means no database file is found
                System.err.println(e.getMessage());
            }
            finally
            {
                try
                {
                    if(connection != null)
                        connection.close();
                }
                catch(SQLException e)
                {
                    // connection close failed.
                    System.err.println(e);
                }
            }
        }
    }


