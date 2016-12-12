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



    public static void main(String[] args) {
//
        saveToDataBase();
//        readFromDataBase();


    }

    private static void saveToDataBase() {
        readFile();
    }

    private static void readFromDataBase() {
        DatabaseHelper db = new DatabaseHelper();
        db.connectToDatabase();
        db.readFromDataBase("SELECT * FROM Comment", "author");

        db.close();
    }


    /****
     * Reads the  josn file  and saves the data to string
     *
     * @return string data.
     */
    private static void readFile() {

        BufferedReader bufferedReader = null;
        //  "/Users/macbookpro/Desktop/RC_2007_10.json"
        DatabaseHelper db = new DatabaseHelper();
        db.connectToDatabase();
        db.createTables();
        int lineCount =0;
        StringBuilder sb = new StringBuilder();
        try {
            String line = "";
            bufferedReader = new BufferedReader(new FileReader("/Users/db/RC_2007_10"));
            start = System.currentTimeMillis();
            while ((line = bufferedReader.readLine()) != null) {
                lineCount++;
                try {
                    JSONObject jsonObject = new JSONObject(line);
                    db.insert(jsonObject.getString("id"),
                            jsonObject.getString("parent_id"),
                            jsonObject.getString("link_id"),
                            jsonObject.getString("name"),
                            jsonObject.getString("author"),
                            jsonObject.getString("body"),
                            jsonObject.getString("subreddit_id"),
                            jsonObject.getString("subreddit"),
                            jsonObject.getInt("score"),
                            jsonObject.getString("created_utc"));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                db.excuteBatch(10000,lineCount);
            }

            end = (System.currentTimeMillis() - start);
            System.out.println(end);
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
    }

}
