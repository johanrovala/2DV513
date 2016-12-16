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



           /* dbPerfect.clearTables();
            dbPerfect.createTables();
            dbPerfect.importData(file);
*/

            long start = System.nanoTime();



            dbPerfect.getCommentsForUser("HiggsBoson");
            dbPerfect.getCommentsPerDayOnSub("nsfw");
            dbPerfect.getAmountOfCommentsWithSpecificWord("lol");
            dbPerfect.usersWhoCommentedOnLinkHasPostedToSubreddits("t3_z9oz");
            dbPerfect.getHighestAndLowestUserScore();
            dbPerfect.highestAndLowestSubreddits();
            dbPerfect.userHasInteractedWith("Danceswithwires");
            dbPerfect.usersWhoPostedToASingleSubOnly();


            System.out.println("Queries in total took: " + (System.nanoTime()-start)/1000000 + "ms");


        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
}