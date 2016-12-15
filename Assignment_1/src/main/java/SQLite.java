import org.sqlite.SQLiteConfig;

import java.io.*;
import java.sql.*;
import java.util.Calendar;

public class SQLite {

    private static Connection conn;

    public static void main(String[] args) {
        conn = null;

        try {
            SQLiteConfig config = new SQLiteConfig();
            config.enforceForeignKeys(true);
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:test.db", config.toProperties());
            System.out.println("Connection to DB up..");
            File file = new File("src/main/resources/RC_2007-10");

            DBPerfect dbPerfect = new DBPerfect(conn);
            int sec = 1193635850;
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(sec * 1000L);
            System.out.println(c.getTime());
/*

            dbPerfect.clearTables();
            dbPerfect.createTables();
            dbPerfect.importData(file);
 */
            dbPerfect.getCommentsForUser("HiggsBoson");
            dbPerfect.getCommentsPerDayOnSub("programming");




        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
}