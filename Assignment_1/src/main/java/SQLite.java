
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
            System.out.println("Success");

            createTables();
            System.out.println("Table created successfully");
            clearTable();
            File file = new File("src/main/resources/RC_2007-10");
            importJohanData(file);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }


    private static void tableWithNoConstraints() throws SQLException {
        System.out.println("Creating table with no constraints...");
        Statement st = conn.createStatement();
        String sql = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
                " (id TEXT," +
                "parent_id TEXT," +
                "link_id TEXT, " +
                "name TEXT," +
                "autor TEXT," +
                "body TEXT," +
                "subreddit_id TEXT," +
                "subreddit TEXT," +
                "score INT," +
                "create_utc INT)";
        st.executeUpdate(sql);
        st.close();
    }

    private static void tableWithConstraints() throws SQLException {
        System.out.println("Creating table WITH constraints...");
        Statement st = conn.createStatement();
        String sql = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
                " (id TEXT," +
                "parent_id TEXT," +
                "link_id TEXT, " +
                "name TEXT," +
                "autor TEXT," +
                "body TEXT," +
                "subreddit_id TEXT," +
                "subreddit TEXT," +
                "score INT," +
                "create_utc INT)";
        st.executeUpdate(sql);
        st.close();
    }

    private static void clearTable() throws SQLException {
        System.out.println("Clearing table " + TABLE_NAME + "...");
        Statement statement;
        String SQL;

        statement = conn.createStatement();
        SQL = "DELETE FROM " + COMMENT_TABLE;
        statement.execute(SQL);
        statement = conn.createStatement();
        SQL = "DELETE FROM " + LINK_TABLE;
        statement.execute(SQL);
        statement = conn.createStatement();
        SQL = "DELETE FROM " + USER_TABLE;
        statement.execute(SQL);
        statement = conn.createStatement();
        SQL = "DELETE FROM " + SUBREDDIT_TABLE;
        statement.execute(SQL);



    }


    private static void createTables() throws SQLException {
        System.out.println("Creating johan supadesign tables...");
        Statement st;
        String sql;

        st = conn.createStatement();
        sql = "CREATE TABLE IF NOT EXISTS " + USER_TABLE +
                " (author TEXT)";
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
    }

    private static void importJohanData(File file) throws SQLException, IOException {
        System.out.println("Import johans data");
        Long startime = System.nanoTime();

        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        conn.setAutoCommit(false);

        PreparedStatement userStatement = conn.prepareStatement
                ("INSERT INTO " + USER_TABLE + " VALUES(?)"
                +" SELECT ");
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
        System.out.println(l + " items inserted, time: " + (System.nanoTime()-startime) / 1000000000);
    }

    private static void importData(File file) throws SQLException, IOException {
        System.out.println("Importing data to " + TABLE_NAME + "...");
        Long startTime = System.nanoTime();
        System.out.println("Starting to read + insert..");
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        conn.setAutoCommit(false);
        PreparedStatement stmt = conn.prepareStatement
                ("INSERT INTO " + TABLE_NAME + " VALUES(?,?,?,?,?,?,?,?,?,?)");
        int l = 0;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String line = bufferedReader.readLine();
        while (line != null) {
            PostEntry obj = mapper.readValue(line, PostEntry.class);
            stmt.setString(1, obj.id);
            stmt.setString(2, obj.parent_id);
            stmt.setString(3, obj.link_id);
            stmt.setString(4, obj.name);
            stmt.setString(5, obj.author);
            stmt.setString(6, obj.body);
            stmt.setString(7, obj.subreddit_id);
            stmt.setString(8, obj.subreddit);
            stmt.setInt(9, obj.score);
            stmt.setInt(10, obj.created_utc);
            stmt.executeUpdate();
            l++;
            line = bufferedReader.readLine();//Next line
        }
        conn.commit();
        long time = (System.nanoTime() - startTime) / 1000000000;
        System.out.println("Done inserting " + l + " objects. Time: " + time);
        conn.close();
    }
}