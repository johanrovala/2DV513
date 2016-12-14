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
public class DBWithoutConstraints implements DB {

    String TABLE_NAME = "POST";
    Connection conn;

    public DBWithoutConstraints(Connection conn) {
        this.conn = conn;
    }

    public void createTables() {
        try {
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
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void importData(File file) {
        try {
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
        } catch(SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void clearTables() {
        try {
            Statement statement = conn.createStatement();
            String SQL = "DROP TABLE IF EXISTS " + TABLE_NAME;
            statement.execute(SQL);
        }catch(SQLException e) {
            e.printStackTrace();
        }
    }

}
