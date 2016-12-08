package main;

import jdk.nashorn.internal.parser.JSONParser;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.sql.*;

public class Main {


    public static void main(String[] args) {
        // craete new database  class
            DatabaseHelper db = new DatabaseHelper();
            db.connectToDatabase();
           db.createTables();
        // read the file;

//            String data = readFile();
//            try {
//                JSONObject jsonObject = new JSONObject(data);
//                db.insert(jsonObject.getString("id"),
//                        jsonObject.getString("name"),
//                        jsonObject.getString("subreddit_id"),
//                        jsonObject.getString("subreddit"));
//            } catch (JSONException e) {
//                e.printStackTrace();
//            }
//
        db.readFromDataBase();
        db.close();

//        readFile();

    }


    /****
     * Reads the  josn file  and saves the data to string
     *
     * @return string data.
     */
    private static String readFile() {
        String data = ""; // save th
        BufferedReader bufferedReader = null;
        //  "/Users/macbookpro/Desktop/RC_2007_10.json"
        DatabaseHelper db = new DatabaseHelper();
        db.connectToDatabase();
        db.createTables();
        try {
            String line = "";
            bufferedReader = new BufferedReader(new FileReader("/Users/macbookpro/Desktop/RC_2007_10.json"));
            while ((line = bufferedReader.readLine()) != null) {
                data += line;



                try {
                    JSONObject jsonObject = new JSONObject(data);
                    System.out.println(jsonObject.toString());
                    db.insert(jsonObject.getString("id"),
                            jsonObject.getString("name"),
                            jsonObject.getString("subreddit_id"),
                            jsonObject.getString("subreddit"));
                } catch (JSONException e) {
                    e.printStackTrace();
                }

            }
            db.readFromDataBase();
            db.close();
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

        return data;
    }


}
