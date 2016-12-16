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
    private String AUTHSUB = "AUTHSUB";
    private String LINKSUB = "LINKSUB";

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

            joinLinksAndSub();
            joinAuthorAndSub();

            conn.commit();
            conn.setAutoCommit(true);           //Set back to true so we dont have to commit everything in future
            System.out.println(l + " items inserted, time: " + (System.nanoTime() - startime) / 1000000 + "ms");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 1. How many comments have a specific user posted?

    public void getCommentsForUser(String user) throws SQLException {
        long start = System.nanoTime();
        System.out.println("q1 - Amount of comments for user \"" + user + "\"");
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) AS count FROM " + COMMENT_TABLE + " WHERE author='" + user + "'");
        System.out.println(rs.getInt("count"));
        System.out.println("q1 done in " + (System.nanoTime() - start) / 1000000 + "ms");
    }

    // 2. How many comments does a specific subreddit get per day?

    public void getCommentsPerDayOnSub(String sub) throws SQLException {
        long start = System.nanoTime();
        System.out.println("q2 - Comments per day on sub \"" + sub + "\"");
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
        System.out.println("q2 done in " + (System.nanoTime() - start) / 1000000 + "ms");
    }

    // 3. How many comments include the word ‘lol’?

    public void getAmountOfCommentsWithSpecificWord(String word) throws SQLException {
        System.out.println("q3 - Amount of comments with word \"" + word + "\"");
        long start = System.nanoTime();
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) AS count FROM " + COMMENT_TABLE + " WHERE body LIKE '%" + word + "%'");
        System.out.println("Amount of comments with the word " + word + " is " + rs.getInt("count"));
        System.out.println("q3 done in " + (System.nanoTime() - start) / 1000000 + "ms");
    }

    // 4.
    public void usersWhoCommentedOnLinkHasPostedToSubreddits(String linkid) throws SQLException {
        long start = System.nanoTime();
        System.out.println("q4 - Users who commented on link: " + linkid + " has posted to these subs: ");
        ResultSet rs = conn.createStatement().executeQuery("SELECT DISTINCT subreddit FROM" +
                "(SELECT author FROM COMMENTS WHERE link_id='" + linkid + "') t1 " +
                " JOIN " +
                "(AUTHSUB) t2" +
                " ON t1.author = t2.author");
        int subs = printResultSet(rs);
        System.out.println(subs + "subreddits. (listed above)");
        System.out.println("q4 done in " + (System.nanoTime() - start) / 1000000 + "ms");
    }


    // 5. Which users have the highest and lowest combined scores? (combined as the sum of all scores)
    public void getHighestAndLowestUserScore() throws SQLException {
        long start = System.nanoTime();
        System.out.println("q5 - Highest and lowest userscore: ");
        ResultSet res = conn.createStatement().executeQuery("SELECT author, MAX(sum) FROM (SELECT author, SUM(score) AS sum FROM COMMENTS" +
                " GROUP BY author) UNION SELECT author, MIN(minsum) FROM (SELECT author, SUM(score) AS minsum FROM COMMENTS GROUP BY author)");

        printResultSet(res);
        System.out.println("q5 done in " + (System.nanoTime() - start) / 1000000 + "ms");

    }

    //6

    public void highestAndLowestSubreddits() throws SQLException {
        System.out.println("q6 - Highest and lowest scored links is found in subreddits: ");
        long start = System.nanoTime();
        ResultSet rs = conn.createStatement().executeQuery("SELECT link_id, max(score) FROM " + COMMENT_TABLE);

        ResultSet subname = conn.createStatement().executeQuery("SELECT subreddit FROM " + LINKSUB + " WHERE link_id='" + rs.getString(1) + "'");
        System.out.println("Highest scored comment with score " + rs.getString(2) + " in subreddit: " + subname.getString(1));

        ResultSet min = conn.createStatement().executeQuery("SELECT link_id, min(score) FROM " + COMMENT_TABLE);
        ResultSet minid = conn.createStatement().executeQuery("SELECT subreddit FROM " + LINKSUB + " WHERE link_id='" + min.getString(1) + "'");

        System.out.println("Lowest scored comment with score " + min.getString(2) + " in subreddit: " + minid.getString(1));
        System.out.println("q6 done in " + (System.nanoTime() - start) / 1000000 + "ms");
    }

    // 7. Given a specific user, list all the users he or she has potentially interacted with (i.e., everyone who as
    //    commented on a link that the specific user has commented on).

    public void userHasInteractedWith(String user) throws SQLException {

        System.out.println("q7 - User \"" + user + "\" has interacted with: ");
        ResultSet rs = conn.createStatement().executeQuery("SELECT DISTINCT author FROM " +
                "(SELECT link_id FROM " + COMMENT_TABLE + " WHERE author='" + user + "') t1" +
                " JOIN " +
                "(SELECT link_id, author FROM " + COMMENT_TABLE + ") t2" +
                " ON t1.link_id=t2.link_id AND author !='"+user+"'");

        int amount = printResultSet(rs);
        System.out.println(amount);
    }

    //8

    public void usersWhoPostedToASingleSubOnly() throws SQLException {
        System.out.println("q8 - These users have only posted to a single subreddit");
        long start = System.nanoTime();
        ResultSet resultSet = conn.createStatement().executeQuery("SELECT author, COUNT(*) as count FROM " + AUTHSUB + " GROUP BY author HAVING COUNT(*)=1");
        int j = printResultSet(resultSet);
        System.out.println(j + " users have only posted to one sub. (Listed above)");
        System.out.println("q8 done in " + (System.nanoTime() - start) / 1000000 + "ms");
    }


    public void joinLinksAndSub() throws SQLException {
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + LINKSUB + " AS SELECT link_id, SUBREDDITS.subreddit FROM " + LINK_TABLE + " JOIN " + SUBREDDIT_TABLE + " ON LINKS.subreddit_id = SUBREDDITS.subreddit_id");
    }

    public void joinAuthorAndSub() throws SQLException {
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + AUTHSUB + " AS SELECT DISTINCT author, LINKSUB.subreddit FROM " + COMMENT_TABLE + " JOIN LINKSUB ON LINKSUB.link_id = COMMENTS.link_id");
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
            statement = conn.createStatement();
            SQL = "DROP TABLE IF EXISTS " + AUTHSUB;
            statement.execute(SQL);
            statement = conn.createStatement();
            SQL = "DROP TABLE IF EXISTS " + LINKSUB;
            statement.execute(SQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private int printResultSet(ResultSet res) throws SQLException {
        ResultSetMetaData rsmd = res.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        int count = 0;
        while (res.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                System.out.print(res.getString(i));
            }
            count++;
            if (columnsNumber == 1) {
                System.out.print(", ");
            }
        }
        System.out.println();
        return count;
    }
}
