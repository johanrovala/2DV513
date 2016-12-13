
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.sql.*;

public class SQLite {

    private static String TABLE_NAME = "POST";

    private static String USER_TABLE = "USERS";
    private static String COMMENT_TABLE = "COMMENTS";
    private static String LINK_TABLE = "LINKS";
    private static String SUBREDDIT_TABLE = "SUBREDDITS";

    private static Connection conn;

    public static void main(String[] args) {
        conn = null;

        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:test.db");
            System.out.println("Connection to DB up..");
            File file = new File("src/main/resources/RC_2007-10");

            DBPerfect dbPerfect = new DBPerfect(conn);
            DBWithoutConstraints dbWithoutConstraints = new DBWithoutConstraints(conn);

            dbPerfect.clearTables();
            dbWithoutConstraints.clearTables();

            dbPerfect.createTables();
            dbPerfect.importData(file);


        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
}