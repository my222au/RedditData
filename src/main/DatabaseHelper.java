package main;

import java.io.File;
import java.sql.*;

/**
 * Created by MohamedOsman on 2016-12-08.
 */
public class DatabaseHelper  {
    private Connection  connection;
    private  Statement  statement;
    PreparedStatement preparedStatement;
    File file;


    public Statement connectToDatabase(){


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



    public void insert(String id,String parent_id, String link_id,
                       String name, String author, String body,
                       String subreddit_id, String subreddit, int score, String created_utc ) {


            String sqlinser = "INSERT INTO Name (id, name) VALUES (?,?)";
            String sqlStatement2 = "INSERT INTO Sub VALUES (" + "\'" + subreddit_id + "\'," + "\'" + subreddit + "\'" + " )";
            String sqlStatement3 ="INSERT INTO Comment  VALUES  ("+ "\'" + id + "\'," +"\'" + parent_id+ "\',"+ "\'" + link_id + "\',"
                      + "\'" + author+ "\'," +"\'"
                      + subreddit_id +"\',"
                      + score +
                      ",\'" + subreddit_id +"\'," + "\'" + created_utc +"\'" + ")";


        try {
           preparedStatement = connection.prepareStatement(sqlinser);
            preparedStatement.setString(1,id);
            preparedStatement.setString(2,name);
            preparedStatement.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }



    }



    public void excuteBatch(int batchSize, int lineCount) {
        if(batchSize==lineCount){
            try {
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }



    }
// test to save the  read
    public  void readFromDataBase(String SQLstatment, String coloumName){

        ResultSet rs = null;
        try {
                rs = statement.executeQuery(SQLstatment);
                  while (rs.next()) {
                    // read the result set
                      System.out.println(rs.getString(coloumName));

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
