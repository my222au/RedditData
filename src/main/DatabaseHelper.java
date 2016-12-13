package main;

import java.io.File;
import java.sql.*;


public class DatabaseHelper {
    private Connection connection;
    private Statement statement;
    private PreparedStatement psNameTable;
    private PreparedStatement psSubTable;
    private PreparedStatement psCommentTable;


    public DatabaseHelper() {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:Reddit.db");
            statement = connection.createStatement();
            connection.setAutoCommit(false);

        } catch (SQLException e) {

            e.printStackTrace();
        }
    }


    public void createTables() {

        try {

            statement.execute("CREATE TABLE  IF NOT EXISTS Sub (subreddit_id TEXT, subreddit TEXT)");
            statement.execute("CREATE TABLE IF NOT EXISTS Name(id TEXT, name TEXT)");
            statement.execute("CREATE TABLE IF NOT EXISTS Comment (id TEXT, parent_id TEXT, link_id TEXT, author TEXT, body TEXT, subreddit_id TEXT, score INTEGER, created_utc TEXT)");


        } catch (SQLException e) {
            System.out.println("Failed while creating the table ");
            e.printStackTrace();

        }
    }


    public void insert(String id, String parent_id, String link_id,
                       String name, String author, String body,
                       String subreddit_id, String subreddit, int score, String created_utc) {


        String sqlStatement1 = "INSERT INTO Name (id, name) VALUES (?,?)";
        String sqlStatement2 = "INSERT INTO Sub (subreddit_id, subreddit) VALUES (?,?)";
        String sqlStatement3 = "INSERT INTO Comment (id, parent_id, link_id, author, body, subreddit_id, score, created_utc) VALUES (?,?,?,?,?,?,?,?)";



        try {
            psNameTable = connection.prepareStatement(sqlStatement1);
            psNameTable.setString(1, id);
            psNameTable.setString(2, name);

            psSubTable = connection.prepareStatement(sqlStatement2);
            psSubTable.setString(1,subreddit_id);
            psSubTable.setString(2,subreddit);

            psCommentTable = connection.prepareStatement(sqlStatement3);
            psCommentTable.setString(1, id);
            psCommentTable.setString(2, parent_id);
            psCommentTable.setString(3, link_id);
            psCommentTable.setString(4, author);
            psCommentTable.setString(5, body);
            psCommentTable.setString(6, subreddit_id);
            psCommentTable.setInt(7, score);
            psCommentTable.setString(8, created_utc);


            psNameTable.addBatch();
            psSubTable.addBatch();
            psCommentTable.addBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void execute() {
        try {
            psNameTable.executeBatch();
            psCommentTable.executeBatch();
            psSubTable.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

//    public void executeBatch(int batchSize, int lineCount) {
//        if (lineCount % batchSize == 0)
//            try {
//                System.out.println("excutetd Batch ");
//                int[] resluts = preparedStatement.executeBatch();
//                connection.commit();
//
//                System.out.println(resluts);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }


//    }


    // test to save the  read
    public void readFromDataBase(String SQLstatement,int num){

        ResultSet rs = null;
        try {
            rs = statement.executeQuery(SQLstatement);
            while (rs.next()) {
                // read the result set
                System.out.println(rs.getInt(num));

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void closeConnection() {
        if (connection != null) {
            try {

                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

    public void connectionCommit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}


