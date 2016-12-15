import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

class DBPerfect implements DB {


    private String USER_TABLE = "USERS";
    private String LINK_TABLE = "LINKS";
    private String SUBREDDIT_TABLE = "SUBREDDITS";
    private String COMMENT_TABLE = "COMMENTS";

    private Connection conn;

    DBPerfect(Connection conn) {
        this.conn = conn;
    }


    public void createTables() {
        System.out.println("Creating \"perfect\" tables");
        Statement st;
        String sql;

        try {
            st = conn.createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + USER_TABLE +
                    " (author TEXT PRIMARY KEY)";

            st.executeUpdate(sql);
            st.close();

            st = conn.createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + COMMENT_TABLE +
                    " (id TEXT PRIMARY KEY," +
                    "parent_id TEXT NOT NULL ," +
                    "link_id TEXT NOT NULL , " +
                    "author TEXT NOT NULL ," +
                    "body TEXT NOT NULL ," +
                    "score INT NOT NULL ," +
                    "create_utc INT NOT NULL)";
            st.executeUpdate(sql);
            st.close();

            st = conn.createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + LINK_TABLE +
                    " (link_id TEXT PRIMARY KEY, " +
                    "subreddit_id TEXT NOT NULL)";
            st.executeUpdate(sql);
            st.close();

            st = conn.createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + SUBREDDIT_TABLE +
                    " (subreddit_id TEXT PRIMARY KEY," +
                    "subreddit TEXT NOT NULL UNIQUE)";
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
                    ("INSERT OR IGNORE INTO " + COMMENT_TABLE + " VALUES(?,?,?,?,?,?,?)");
            PreparedStatement linkStatement = conn.prepareStatement
                    ("INSERT OR IGNORE INTO " + LINK_TABLE + " VALUES(?,?)");
            PreparedStatement subredditStatement = conn.prepareStatement
                    ("INSERT OR IGNORE INTO " + SUBREDDIT_TABLE + " VALUES(?,?)");


            int l = 0;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = bufferedReader.readLine();
            while (line != null) {
                PostEntry obj = mapper.readValue(line, PostEntry.class);
                userStatement.setString(1, obj.author);

                linkStatement.setString(1, obj.link_id);
                linkStatement.setString(2, obj.subreddit_id);

                commentStatement.setString(1, obj.id);
                commentStatement.setString(2, obj.parent_id);
                commentStatement.setString(3, obj.link_id);
                commentStatement.setString(4, obj.author);
                commentStatement.setString(5, obj.body);
                commentStatement.setInt(6, obj.score);
                commentStatement.setInt(7, obj.created_utc);

                subredditStatement.setString(1, obj.subreddit_id);
                subredditStatement.setString(2, obj.subreddit);

                userStatement.executeUpdate();
                subredditStatement.executeUpdate();
                linkStatement.executeUpdate();
                commentStatement.executeUpdate();
                l++;
                line = bufferedReader.readLine();//Next line
            }
            conn.commit();
            conn.setAutoCommit(true);           //Set back to true so we dont have to commit everything in future
            System.out.println(l + " items inserted, time: " + (System.nanoTime() - startime) / 1000000000);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteUser(String user) throws SQLException {
        conn.createStatement().executeUpdate("DELETE FROM " + USER_TABLE + " WHERE author='" + user + "'");
    }

    public void deleteCommentByUsername(String user) throws SQLException {
        conn.createStatement().executeUpdate("DELETE FROM " + COMMENT_TABLE + " WHERE author='" + user + "'");
    }

    public void getCommentsForUser(String user) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + COMMENT_TABLE + " WHERE author='" + user + "'");
        ResultSetMetaData rsmd = rs.getMetaData();

        int coln = rsmd.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i < coln; i++) {
                String col = rs.getString(i);
                System.out.println(col + " " + rsmd.getColumnName(i));
            }
            System.out.println();
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
