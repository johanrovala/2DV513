import java.io.File;

/**
 * Created by Ludde on 2016-12-13.
 */
public interface DB {

    void createTables();
    void importData(File file);
    void clearTables();

}
