
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
        File file = new File("src\\main\\resources\\RC_2007-10-test.txt");
        System.out.println(file.exists());
        Long startTime = System.nanoTime();
        System.out.println("Starting to read + insert..");


        try {
            //buffered fuck and simplejson
            /*
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                JSONObject o = (JSONObject) parser.parse(line);
                objects.add(o);
                //Long time = (System.nanoTime() - startTime) / 1000000;
                //System.out.println("time elapsed: " + time);

            }
             */

            ObjectMapper mapper = new ObjectMapper();
            mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

            List<PostEntry> objects = new ArrayList<PostEntry>();
            Iterator<PostEntry> iterator = mapper.reader(PostEntry.class).readValues(file);
            while(iterator.hasNext()) {
                objects.add(iterator.next());
            }

            // List<PostEntry> objects = Arrays.asList(mapper.readValues(file,PostEntry.class));
            //mapper.readValue(file, new TypeReference<List<PostEntry>>() {});
            System.out.println("Finished inserting");
            Long time = (System.nanoTime() - startTime) / 1000000000;
            System.out.println("Took " + time + " seconds to read file and add to list.");
            insertAll(objects);
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertAll(List<PostEntry> arr) {
        int l = arr.size();
        System.out.println("Stating insert.");
        long start = System.nanoTime();
        for (PostEntry o : arr) {
            insertRow(o);
        }
        long time = (System.nanoTime() - start) / 1000000000;
        System.out.println("Done inserting " + l + " objects. Time: " + time);

    }

    private static void insertRow(PostEntry obj) {
        try {
            PreparedStatement stmt = conn.prepareStatement
                    ("insert into POST values(?,?,?,?,?,?,?,?,?,?)");

            stmt.setString(1,obj.id);
            stmt.setString(2,obj.parent_id);
            stmt.setString(3,obj.link_id);
            stmt.setString(4,obj.name);
            stmt.setString(5,obj.author);
            stmt.setString(6,obj.body);
            stmt.setString(7,obj.subreddit_id);
            stmt.setString(8,obj.subreddit);
            stmt.setInt(9,obj.score);
           // stmt.setInt(10,obj.created_utc);
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

/*
    private static void insertRow(JSONObject o) {
        try {
            PreparedStatement stmt = conn.prepareStatement
                    ("insert into POST values(?,?,?,?,?,?,?,?,?,?)");
            stmt.setObject(1, o.get("id"));
            stmt.setObject(2, o.get("parent_id"));
            stmt.setObject(3, o.get("link_id"));
            stmt.setObject(4, o.get("name"));
            stmt.setObject(5, o.get("author"));
            stmt.setObject(6, o.get("body"));
            stmt.setObject(7, o.get("subreddit_id"));
            stmt.setObject(8, o.get("subreddit"));
            stmt.setObject(9, o.get("score"));
            stmt.setObject(10, o.get("created_utc"));

            stmt.executeUpdate();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
*/

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