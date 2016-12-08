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
        if(!file.exists()){
            try {
                connection  = DriverManager.getConnection("jdbc:sqlite:Reddit.db");
                statement  = connection.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            // db esixts  do nothing

        }

        return  statement;
    }


    public  void createTables() {

        try {

            if (statement != null) {
                statement.execute("CREATE TABLE  IF NOT EXISTS Sub (subreddit_id TEXT, subreddit TEXT)");
                statement.execute("CREATE TABLE IF NOT EXISTS Name (id TEXT, name TEXT)");
                statement.execute("CREATE TABLE IF NOT EXISTS Comment (id TEXT, parent_id TEXT, link_id TEXT, author TEXT, body TEXT, subreddit_id TEXT, score INTEGER, created_utc TEXT)");

            }
            }catch(SQLException e){
                System.out.println("Failed while creating the tabels ");
                e.printStackTrace();

            }

        }



    public void insert(String id, String name) {
        // testing to inserting Id
        try {
            if(statement!=null) {
                statement.execute("INSERT INTO Name VALUES (" + "\'" + id + "\'," + "\'" + name + "\'" + " )");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

// test to save the  read
    public  void readFromDataBase(){

        ResultSet rs = null;
        try {
            if(statement!=null) {
                rs = statement.executeQuery("select * from name");

                while (rs.next()) {
                    // read the result set
                    System.out.println(rs.getString("id"));

                }
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
