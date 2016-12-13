package main;

import jdk.nashorn.internal.parser.JSONParser;
import jdk.nashorn.internal.parser.Parser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.sql.*;

public class Main {
    private static long start;
    private static long end;
    private  static  Statement statement;
    private static  Connection connection;
    private static  PreparedStatement preparedStatement;



    public static void main(String[] args) {
//
        setupDatabaser();
        // saveToDataBase();
         readFromDataBase();


    }

    public static  void setupDatabaser() {
        try {
            connection  = DriverManager.getConnection("jdbc:sqlite:Reddit.db");
        statement  = connection.createStatement();
            connection.setAutoCommit(false);
            createTables(statement);

        } catch (SQLException e) {

            e.printStackTrace();
        }


    }


    private static void saveToDataBase() {
        readFile();
    }

    private static void readFromDataBase() {
        ResultSet resultSet;
        try {
            resultSet = statement.executeQuery("SELECT * FROM name WHERE  id = 'c0299an' ");
            while (resultSet.next()) {
                // read the result set
                System.out.println(resultSet.getString("id"));

            }




        } catch (SQLException e) {
            e.printStackTrace();
        }
    }





    /****
     * Reads the  josn file  and saves the data to string
     *
     * @return string data.
     */
    private static void readFile() {
        BufferedReader bufferedReader = null;
        int lineCount =0;
        int batchsize =10000;

        try {

            String sqlinser = "INSERT INTO Name (id, name) VALUES (?,?)";
            bufferedReader = new BufferedReader(new FileReader("/Users/db/RC_2007_10"));
            start = System.currentTimeMillis();
            String line = "";
            while ((line = bufferedReader.readLine())!= null) {


                try {
                    JSONObject jsonObject = new JSONObject(line);
                    try {
                        preparedStatement = connection.prepareStatement(sqlinser);
                        preparedStatement.setString(1,jsonObject.getString("id"));
                        preparedStatement.setString(2,jsonObject.getString("name"));
                        preparedStatement.addBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (JSONException e1) {
                        e1.printStackTrace();
                    }

                } catch (JSONException e1) {
                    e1.printStackTrace();
                }

                lineCount++;


                if( lineCount % batchsize ==  batchsize-1 ) {

                    preparedStatement.executeBatch();
                    connection.commit();

                }
                preparedStatement.executeBatch();
                preparedStatement.close();

            }

            connection.commit();




            end = (System.currentTimeMillis() - start);
            System.out.println(end);
            connection.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void excute() {
        try {
            preparedStatement.executeBatch();
            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void excuteBatch(int batchSize, int lineCount) {
//        if(lineCount % batchSize == 0)
//            try {
//                System.out.println("excutetd Batch ");
//                int [] resluts  =    preparedStatement.executeBatch();
//                connection.commit();
//
//                System.out.println(resluts);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }


    }



    public  static void  createTables(Statement statement) {

        try {


            statement.execute("CREATE TABLE  IF NOT EXISTS Sub (subreddit_id TEXT, subreddit TEXT)");
            statement.execute("CREATE TABLE IF NOT EXISTS Name (id TEXT, name TEXT)");
            statement.execute("CREATE TABLE IF NOT EXISTS Comment (id TEXT, parent_id TEXT, link_id TEXT, author TEXT, body TEXT, subreddit_id TEXT, score INTEGER, created_utc TEXT)");


        }catch(SQLException e){
            System.out.println("Failed while creating the tabel ");
            e.printStackTrace();

        }

    }



    public  static  void insert(String id,String parent_id, String link_id,
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



//    public static void readFromDataBase(String SQLstatment, String coloumName){
//
//        ResultSet resultSet;
//        try {
//            resultSet = statement.executeQuery(SQLstatment);
//            while (resultSet.next()) {
//                // read the result set
//                System.out.println(resultSet.getString(coloumName));
//
//            }
//
//
//
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//
//


    public static void closeConnection() {
        if (connection != null) {
            try {

                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }
}



