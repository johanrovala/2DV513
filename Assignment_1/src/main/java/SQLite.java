
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.sql.*;
import java.util.*;

public class SQLite {

    private static String TABLE_NAME = "POST";
    private static Connection conn;

    public static void main(String[] args) {
        conn = null;
        Statement st = null;

        //JSONParser parser = new JSONParser();

        try {
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:test.db");
            System.out.println("Success");

            clearTable();

            st = conn.createStatement();
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

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Table created successfully");
        File file = new File("src\\main\\resources\\RC_2011-07");
        System.out.println(file.exists());
        Long startTime = System.nanoTime();
        System.out.println("Starting to read + insert..");


        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

            List<PostEntry> objects = new ArrayList<PostEntry>();
            Iterator<PostEntry> iterator = mapper.reader(PostEntry.class).readValues(file);
            conn.setAutoCommit(false);
            PreparedStatement stmt = conn.prepareStatement
                    ("INSERT INTO POST VALUES(?,?,?,?,?,?,?,?,?,?)");
            int l = 0;
            while(iterator.hasNext()) {
                PostEntry obj = iterator.next();
                stmt.setString(1,obj.id);
                stmt.setString(2,obj.parent_id);
                stmt.setString(3,obj.link_id);
                stmt.setString(4,obj.name);
                stmt.setString(5,obj.author);
                stmt.setString(6,obj.body);
                stmt.setString(7,obj.subreddit_id);
                stmt.setString(8,obj.subreddit);
                stmt.setInt(9,obj.score);
                stmt.setString(10,obj.created_utc);
                stmt.addBatch();
                l++;
            }
            System.out.println("executing inserts...");
            stmt.executeBatch();
            conn.commit();
            long time = (System.nanoTime() - startTime) / 1000000000;
            System.out.println("Done inserting " + l + " objects. Time: " + time);
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void clearTable() {
        try {
            Statement statement = conn.createStatement();
            String SQL = "DELETE FROM " + TABLE_NAME;
            statement.execute(SQL);


        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


}