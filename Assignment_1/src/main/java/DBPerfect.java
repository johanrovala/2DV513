import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.javafx.binding.StringFormatter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

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
                    "create_utc INT NOT NULL , " +
                    "FOREIGN KEY (author) REFERENCES " + USER_TABLE + "(author) ON DELETE CASCADE," +
                    "FOREIGN KEY (link_id) REFERENCES " + LINK_TABLE + "(link_id) ON DELETE CASCADE)";
            st.executeUpdate(sql);
            st.close();

            st = conn.createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + LINK_TABLE +
                    " (link_id TEXT PRIMARY KEY, " +
                    "subreddit_id TEXT NOT NULL ," +
                    "FOREIGN KEY (subreddit_id) REFERENCES " + SUBREDDIT_TABLE + "(subreddit_id) ON DELETE CASCADE)";
            st.executeUpdate(sql);
            st.close();

            st = conn.createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + SUBREDDIT_TABLE +
                    " (subreddit_id TEXT PRIMARY KEY," +
                    "subreddit TEXT NOT NULL UNIQUE )";
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
            System.out.println(l + " items inserted, time: " + (System.nanoTime() - startime) / 1000000);
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

    // 1. How many comments have a specific user posted?

    public void getCommentsForUser(String user) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) AS count FROM " + COMMENT_TABLE + " WHERE author='" + user + "'");
        System.out.println(rs.getInt("count"));
    }

    // 2. How many comments does a specific subreddit get per day?

    public void getCommentsPerDayOnSub(String sub) throws SQLException {

        // Select subreddit_id from given subreddit name

        ResultSet subredditResultSet = conn.createStatement().executeQuery("SELECT subreddit_id FROM " + SUBREDDIT_TABLE + " WHERE subreddit='" + sub + "'");
        String subreddit_id = subredditResultSet.getString(1);


        ResultSet linksResultSet = conn.createStatement().executeQuery("SELECT link_id FROM " + LINK_TABLE + " WHERE subreddit_id='" + subreddit_id + "'");

        // Push all links related to the subreddit to the list links
        ArrayList<String> links = new ArrayList<String>();
        while (linksResultSet.next()) {
            links.add(linksResultSet.getString(1));
        }

        // Get created_utc from comments where link_id is equal

        ResultSet commentsResultSet;
        ArrayList<Date> dates = new ArrayList<Date>();
        HashSet<String> uniqueDays = new HashSet<String>();
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
        int amountOfComments = 0;
        for (int i = 0; i < links.size(); i++) {
            commentsResultSet = conn.createStatement().executeQuery("SELECT create_utc FROM " + COMMENT_TABLE + " WHERE link_id='" + links.get(i) + "'");
            while (commentsResultSet.next()) {
                amountOfComments++;
                Date date = new Date(Long.parseLong(commentsResultSet.getString(1)) * 1000);
                String d = sdf.format(date);
                uniqueDays.add(d);
            }
        }

        System.out.println("Average amount of comments per day for subbreddit: " + sub + " is " + amountOfComments / uniqueDays.size());

    }

    // 3. How many comments include the word ‘lol’?

    public void getAmountOfCommentsWithSpecificWord(String word) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) AS count FROM " + COMMENT_TABLE + " WHERE body LIKE '%" + word + "%'");
        System.out.println("Amount of comments with the word " + word + " is " + rs.getInt("count"));
    }

    // 4. your mom
    public void findUsersWhoCommentedOnLinkWhoAlsoPostedToSubReddits(String linkid) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT author FROM " + COMMENT_TABLE + " WHERE link_id='"+ linkid +"'");
        Set links = new HashSet();
        while(rs.next()) {
            String currAuthor = rs.getString(1);
            ResultSet currentAuthorLinks = conn.createStatement().executeQuery("SELECT DISTINCT link_id FROM " + COMMENT_TABLE + " WHERE author='"+ currAuthor +"'");
            while(currentAuthorLinks.next()) {
                links.add(currentAuthorLinks.getString(1));
            }
        }
        ArrayList list = new ArrayList(links);
        System.out.println(links.size());
        Set subs = new HashSet();
        for(int i = 0; i<links.size(); i++) {
            ResultSet sub = conn.createStatement().executeQuery("SELECT subreddit_id FROM " + LINK_TABLE + " WHERE link_id='"+ list.get(i) +"'");
            while(sub.next()) {
                subs.add(sub.getString(1));
            }
        }
        System.out.println(subs.size());
        System.out.println(subs);
        ArrayList sublist = new ArrayList(subs);

        Set subname = new HashSet();
        for(int i = 0; i<sublist.size(); i++) {
            ResultSet subnames = conn.createStatement().executeQuery("SELECT subreddit FROM " + SUBREDDIT_TABLE + " WHERE subreddit_id='"+ sublist.get(i) +"'");
            while (subnames.next()) {
                subname.add(subnames.getString(1));
            }
        }
        System.out.println(subname);
        System.out.println(subname.size());

    }

    // 5. Which users have the highest and lowest combined scores? (combined as the sum of all scores)
    public void getHighestAndLowestUserScore() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT author FROM " + USER_TABLE);
        String minGuy = "";
        String maxGuy = "";
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;

        while (rs.next()) {
            ResultSet userScoreResultSet = conn.createStatement().executeQuery("SELECT sum(score) FROM " + COMMENT_TABLE + " WHERE author='" + rs.getString(1) + "'");
            int curr = Integer.valueOf(userScoreResultSet.getString(1));
            if (curr > max && !rs.getString(1).equals("[deleted]")) {
                max = curr;
                maxGuy = rs.getString(1);
            }
            if (curr < min) {
                min = curr;
                minGuy = rs.getString(1);
            }
        }

        System.out.println("Lowest Score of all users: " + min + ", user: " + minGuy);
        System.out.println("Highest Score of all users: " + max + ", user: " + maxGuy);
    }


    //6

    public void highestAndLowestSubreddits() throws SQLException{
        ResultSet rs = conn.createStatement().executeQuery("SELECT link_id, max(score) FROM " + COMMENT_TABLE);


        ResultSet subid = conn.createStatement().executeQuery("SELECT subreddit_id FROM "+ LINK_TABLE + " WHERE link_id='"+ rs.getString(1) +"'");
        ResultSet subname = conn.createStatement().executeQuery("SELECT subreddit FROM " + SUBREDDIT_TABLE + " WHERE subreddit_id='"+ subid.getString(1) +"'");
        System.out.println("Highest scored comment with score " + rs.getString(2) + " in subreddit: " + subname.getString(1));

        ResultSet min = conn.createStatement().executeQuery("SELECT link_id, min(score) FROM " + COMMENT_TABLE);
        ResultSet minid = conn.createStatement().executeQuery("SELECT subreddit_id FROM "+ LINK_TABLE + " WHERE link_id='"+ min.getString(1) +"'");
        ResultSet minsubname = conn.createStatement().executeQuery("SELECT subreddit FROM " + SUBREDDIT_TABLE + " WHERE subreddit_id='"+ minid.getString(1) +"'");
        System.out.println("Lowest scored comment with score " + min.getString(2) + " in subreddit: " + minsubname.getString(1));
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
