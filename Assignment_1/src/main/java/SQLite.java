
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sqlite.SQLiteConfig;

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
            SQLiteConfig config = new SQLiteConfig();
            config.enforceForeignKeys(true);
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:test.db", config.toProperties());
            System.out.println("Connection to DB up..");
            File file = new File("src/main/resources/RC_2011-07");

            DBPerfect dbPerfect = new DBPerfect(conn);
            DBWithoutConstraints dbWithoutConstraints = new DBWithoutConstraints(conn);

            dbWithoutConstraints.clearTables();
            dbWithoutConstraints.createTables();
            dbWithoutConstraints.importData(file);
            dbWithoutConstraints.clearTables();

            dbPerfect.clearTables();
            dbPerfect.createTables();
            dbPerfect.importData(file);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
}