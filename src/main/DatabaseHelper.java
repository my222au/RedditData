package main;

import java.io.File;
import java.sql.*;

/**
 * Created by MohamedOsman on 2016-12-08.
 */
public class DatabaseHelper  {
    private Connection  connection;
    private Statement  statement;
    File file;


    public Statement connectToDatabase(){
       file = new File("Reddit.db");

            try {
                connection  = DriverManager.getConnection("jdbc:sqlite:Reddit.db");
                statement  = connection.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
            }




        return  statement;
    }


    public  void createTables() {

        try {


                statement.execute("CREATE TABLE  IF NOT EXISTS Sub (subreddit_id TEXT, subreddit TEXT)");
                statement.execute("CREATE TABLE IF NOT EXISTS Name (id TEXT, name TEXT)");
                statement.execute("CREATE TABLE IF NOT EXISTS Comment (id TEXT, parent_id TEXT, link_id TEXT, author TEXT, body TEXT, subreddit_id TEXT, score INTEGER, created_utc TEXT)");


            }catch(SQLException e){
                System.out.println("Failed while creating the tabel ");
                e.printStackTrace();

            }

        }



    public void insert(String id, String name, String subreddit_id, String subreddit) {
        // testing to inserting Id
        try {

                statement.execute("INSERT INTO Name VALUES (" + "\'" + id + "\'," + "\'" + name + "\'" + " )");
                statement.execute("INSERT INTO Name VALUES (" + "\'" + subreddit_id + "\'," + "\'" + subreddit + "\'" + " )");


        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

// test to save the  read
    public  void readFromDataBase(){

        ResultSet rs = null;
        try {
            if(statement!=null) {
                rs = statement.executeQuery("SELECT * FROM name");

                while (rs.next()) {
                    // read the result set

                    System.out.println("id:" + rs.getString("id") + " name:" + rs.getString("name"));

                }
            }
                rs = statement.executeQuery("SELECT * FROM Sub");
                while (rs.next()){
                    System.out.println( rs.getString("subreddit_id") + " " + rs.getString("subreddit"));
                }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    public void close() {
        if(connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
