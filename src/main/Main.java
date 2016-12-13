package main;

import jdk.nashorn.internal.parser.JSONParser;
import jdk.nashorn.internal.parser.Parser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.xml.crypto.Data;
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

         saveToDataBase();
         readFromDataBase();


    }





    private static void saveToDataBase() {
        readFile();
    }

    private static void readFromDataBase() {
        DatabaseHelper db = new DatabaseHelper();
        db.readFromDataBase("SELECT * from name", "id" );
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
        DatabaseHelper db =   new DatabaseHelper();
        db.createTables();

        try {


            bufferedReader = new BufferedReader(new FileReader("/Users/db/RC_2007_10"));
            start = System.currentTimeMillis();
            String line = "";
            while ((line = bufferedReader.readLine())!= null) {


                try {
                    JSONObject jsonObject = new JSONObject(line);
                    try {
                        db.insert(jsonObject.getString("id"),
                                jsonObject.getString("parent_id"),
                                jsonObject.getString("link_id"),
                                jsonObject.getString("name"),
                                jsonObject.getString("author"),
                                jsonObject.getString("body"),
                                jsonObject.getString("subreddit_id"),
                                jsonObject.getString("subreddit"),
                                jsonObject.getInt("score"),
                                jsonObject.getString("created_utc")
                        );


                    } catch (JSONException e1) {
                        e1.printStackTrace();
                    }

                } catch (JSONException e1) {
                    e1.printStackTrace();
                }



//
                if(++lineCount % batchsize ==  0 ) {

                   db.excute();

                }
                db.excute();


            }
               db.connectionCommmit();





            end = (System.currentTimeMillis() - start);
            System.out.println(end);
            db.closeConnection();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
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


}



