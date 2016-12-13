import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Ludde on 2016-12-13.
 */
public class DBPerfect implements DB {


    String USER_TABLE = "USERS";
    String COMMENT_TABLE = "COMMENTS";
    String LINK_TABLE = "LINKS";
    String SUBREDDIT_TABLE = "SUBREDDITS";

    Connection conn;

    public DBPerfect(Connection conn) {
        this.conn = conn;
    }


    public void createTables() {
        System.out.println("Creating \"perfect\" tables");
        Statement st;
        String sql;

        try {
            st = conn.createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + USER_TABLE +
                    " (author TEXT PRIMARY KEY )";

            st.executeUpdate(sql);
            st.close();

            st = conn.createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + COMMENT_TABLE +
                    " (id TEXT," +
                    "parent_id TEXT," +
                    "link_id TEXT, " +
                    "author TEXT," +
                    "body TEXT," +
                    "score INT," +
                    "create_utc INT)";
            st.executeUpdate(sql);
            st.close();

            st = conn.createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + LINK_TABLE +
                    " (link_id TEXT, " +
                    "subreddit_id TEXT)";
            st.executeUpdate(sql);
            st.close();

            st = conn.createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + SUBREDDIT_TABLE +
                    " (subreddit_id TEXT," +
                    "subreddit TEXT)";
            st.executeUpdate(sql);
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void importData(File file) {
        System.out.println("Importing data \"perfectly\".");
        Long startime = System.nanoTime();

        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        try {
            conn.setAutoCommit(false);

            PreparedStatement userStatement = conn.prepareStatement
                    ("INSERT OR IGNORE INTO " + USER_TABLE + " VALUES(?)");
            PreparedStatement commentStatement = conn.prepareStatement
                    ("INSERT INTO " + COMMENT_TABLE + " VALUES(?,?,?,?,?,?,?)");
            PreparedStatement linkStatement = conn.prepareStatement
                    ("INSERT INTO " + LINK_TABLE + " VALUES(?,?)");
            PreparedStatement subredditStatement = conn.prepareStatement
                    ("INSERT INTO " + SUBREDDIT_TABLE + " VALUES(?,?)");


            int l = 0;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = bufferedReader.readLine();
            while (line != null) {
                PostEntry obj = mapper.readValue(line, PostEntry.class);
                userStatement.setString(1, obj.author);

                commentStatement.setString(1, obj.id);
                commentStatement.setString(2, obj.parent_id);
                commentStatement.setString(3, obj.link_id);
                commentStatement.setString(4, obj.author);
                commentStatement.setString(5, obj.body);
                commentStatement.setInt(6, obj.score);
                commentStatement.setInt(7, obj.created_utc);

                linkStatement.setString(1, obj.link_id);
                linkStatement.setString(2, obj.subreddit_id);

                subredditStatement.setString(1, obj.subreddit_id);
                subredditStatement.setString(2, obj.subreddit);

                userStatement.executeUpdate();
                commentStatement.executeUpdate();
                linkStatement.executeUpdate();
                subredditStatement.executeUpdate();
                l++;
                line = bufferedReader.readLine();//Next line
            }
            conn.commit();
            System.out.println(l + " items inserted, time: " + (System.nanoTime() - startime) / 1000000000);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void clearTables() {
        System.out.println("Clearing,....");
        Statement statement;
        String SQL;

        try {
            statement = conn.createStatement();
            SQL = "DROP TABLE IF EXISTS " + COMMENT_TABLE;
            statement.execute(SQL);
            statement = conn.createStatement();
            SQL = "DROP TABLE IF EXISTS " + LINK_TABLE;
            statement.execute(SQL);
            statement = conn.createStatement();
            SQL = "DROP TABLE IF EXISTS " + USER_TABLE;
            statement.execute(SQL);
            statement = conn.createStatement();
            SQL = "DROP TABLE IF EXISTS " + SUBREDDIT_TABLE;
            statement.execute(SQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
