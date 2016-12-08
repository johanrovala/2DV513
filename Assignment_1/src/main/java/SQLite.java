import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by johanrovala on 08/12/16.
 */
public class SQLite {
    public static void main(String[] args){
        Connection c = null;
        Statement st = null;

        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:test.db");
            System.out.println("Success");

            st = c.createStatement();
            String sql = "CREATE TABLE POST " +
                         "(id INT,"            +
                         "parent_id INT,"     +
                         "link_id INT, "      +
                         "name TEXT,"         +
                         "autor TEXT,"        +
                         "body TEXT,"         +
                         "subreddit_id INT,"  +
                         "subreddit TEXT,"    +
                         "score INT,"         +
                         "create_utc INT)";
            st.executeUpdate(sql);
            st.close();
            c.close();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Table created successfully");

        Gson gson = new Gson();
        String filename = ""
        JsonReader reader = new JsonReader(new FileReader())

    }
}
