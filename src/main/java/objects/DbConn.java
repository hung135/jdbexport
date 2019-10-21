package objects;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.opencsv.CSVWriter;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import databaseobjects.Column;
import databaseobjects.Table;
import dto.DBConfigDto;
import enums.DbType;
import utils.DataUtils;
import utils.JLogger;

public class DbConn implements Cloneable{
    public Connection conn;
    // private Statement stmt; // tbd
    // final static Logger logger = Logger.getLogger(DbConn.class);
    public Statement stmt; // tbd
    public PreparedStatement ps;
    public String lastPSSql;
    public ResultSet rs;
    public DbType dbType;
    public String databaseName;
    public String url;
    public String username;
    public String password;
    public String  host;
    public String port ;
    public Logger logger;

    public DbConn clone() throws CloneNotSupportedException {
        return (DbConn) super.clone();
}
    /**
     * an attemp to release resource
     * 
     * @throws SQLException
     */
    public void flushReset() throws SQLException {
        this.ps.executeBatch();
        this.ps.clearBatch();
        this.ps.close();
        this.stmt.close();
        this.stmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        System.out.println(this.lastPSSql);
        this.ps = this.conn.prepareStatement(this.lastPSSql);
    }

    public String getUrl() {
        return this.url;
    }

    public DbConn(DBConfigDto dto, JLogger jlogger) 
        throws SQLException, PropertyVetoException, ClassNotFoundException
    {    
        this.dbType = dto.getDbtype();
        this.databaseName = dto.getDatabase_name();
        this.username = dto.getUser();
        this.password = dto.getPassword();
        this.host= dto.getHost();
        this.port = dto.getPort().toString();
        String url = MessageFormat.format(dbType.url(), host, port, databaseName);
        this.logger = jlogger.logger;
 
        this.url = url;
        Properties props = new Properties();
        props.setProperty("user", this.username);
        props.setProperty("password", this.password);
        
        Class.forName(this.dbType.driver());
        this.conn = DriverManager.getConnection(url, props);
        // MemoryListener.BindListeners(); // disabled for now
        //System.out.println("Connect to Database: " + this.url);
        // System.out.println("DB Connection Successful: " + dbtype);
    }

    public DbConn(DbType dbType, String userName, String password, String host, String port, String databaseName) 
    throws SQLException, PropertyVetoException, ClassNotFoundException {

        this.dbType = dbType;
        this.databaseName = databaseName;
        this.username = userName;
        this.password = password;
        this.host= host;
        this.port = port;
        String url = MessageFormat.format(dbType.url(), host, port, databaseName);
        // this.logger = jLogger.logger;
 
        this.url = url;
        Properties props = new Properties();
        props.setProperty("user", userName);
        props.setProperty("password", password);
        
        Class.forName(dbType.driver());
        this.conn = DriverManager.getConnection(url, props);
        // MemoryListener.BindListeners(); // disabled for now
        //System.out.println("Connect to Database: " + this.url);
        // System.out.println("DB Connection Successful: " + dbtype);
    }

    public void reConnect() throws ClassNotFoundException, SQLException {
        
        Properties props = new Properties();
        props.setProperty("user", this.username);
        props.setProperty("password", this.password);
        Class.forName(this.dbType.driver());
        /**setting to null because closing the connection may close the memory 
         * location of the other conns this was cloned from
         */
        this.conn=null;
        this.conn = DriverManager.getConnection(this.url, props);
        //System.out.println("Re-Connected to Database: " + this.url);
    }
 
    public Connection getSybaseConn(String userName, String password, String host, String databasename, String port)
            throws SQLException {
        Connection conn = null;

        String url = "jdbc:jtds:sybase://{0}:{1}/{2}";

        url = MessageFormat.format(url, host, port, databasename);

        Properties props = new Properties();
        props.setProperty("user", userName);
        props.setProperty("password", password);
        props.setProperty("host", host);
        props.setProperty("port", port);
        conn = DriverManager.getConnection(url, props);
        return conn;
    }

    /**
     * 
     * 
     * This could be faster with a query that returns it all in 1 pass
     * 
     * @param schemaName
     * @return
     * @throws SQLException
     */
    public List<Table> getAllTableColumns(String schemaName) throws SQLException {
        List<String> tables = this.getTableNames(schemaName);
        List<Table> items = new ArrayList<>();

        // For Loop for iterating ArrayList
        for (int i = 0; i < tables.size(); i++) {
            String tableName = tables.get(i);
            Table tbl = new Table(tableName);
            tbl.columnNames = this.getColumns(tableName);
            items.add(tbl);
        }
        return items;
    }

    public List<Table> getAllTableColumnAndTypes(String schemaName) throws SQLException {
        List<String> tables = this.getTableNames(schemaName);
        List<Table> items = new ArrayList<Table>();
        DatabaseMetaData metadata = conn.getMetaData();

        for (String tblName : tables) {
            Table tbl = new Table(tblName);
            ResultSet rs = metadata.getColumns(this.databaseName, schemaName, tblName, null);
            ResultSetMetaData rsMetaData = rs.getMetaData();

            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                String colName = rsMetaData.getColumnName(i);
                String type = rsMetaData.getColumnTypeName(i);
                //System.out.println("Table: " + tblName + "ColName: " + colName + ":" + type);
                //this.logger.debug("Table: " + tblName + "ColName: " + colName + ":" + type);
                tbl.columns.add(new Column(colName, type, i));
            }
            items.add(tbl);
        }
        return items;
    }

    public List<String> getViewNames(String schemaName) throws SQLException {

        String TABLE_NAME = "TABLE_NAME";
        String TABLE_SCHEMA = "TABLE_SCHEM";
        String[] VIEW_TYPES = { "VIEW" };
        DatabaseMetaData dbmd = conn.getMetaData();

        ResultSet rs = dbmd.getTables(this.databaseName, schemaName, null, VIEW_TYPES);
        List<String> items = new ArrayList<>();
        while (rs.next()) {

            if (schemaName.toLowerCase().equals(rs.getString(TABLE_SCHEMA).toLowerCase())) {
                items.add(rs.getString(TABLE_NAME));
            }
        }
        rs.close();
        return items;
    }

    public List<String> getTableNames(String schemaName) throws SQLException {
        String TABLE_NAME = "TABLE_NAME";
        String TABLE_SCHEMA = "TABLE_SCHEM";
        String[] TYPES = { "TABLE" };
        List<String> items = new ArrayList<>();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        // Print TABLE_TYPE "TABLE"
        ResultSet rs = databaseMetaData.getTables(this.databaseName, schemaName, null, TYPES);

        while (rs.next()) {

            if (schemaName.toLowerCase().equals(rs.getString(TABLE_SCHEMA).toLowerCase())) {
                items.add(rs.getString(TABLE_NAME));
            }
        }
        rs.close();
        return items;
    }

    public List<String> getSybaseObjNames(String schemaName, String objType) throws SQLException {
        String sql = "select distinct obj.name from dbo.sysobjects obj  join dbo.syscomments c on obj.id=c.id"
                + "   where obj.type = '" + objType + "'  and USER_NAME(uid)='" + schemaName + "' order by 1";

        List<String> items = new ArrayList<>();
        // Print TABLE_TYPE "TABLE"

        Statement stmt = null;
        stmt = this.conn.createStatement();
        // Let us check if it returns a true Result Set or not.
        ResultSet rs = stmt.executeQuery(sql);

        while (rs.next()) {
            items.add(rs.getString(1));

        }
        stmt.close();

        rs.close();

        return items;
    }

    /**
     * 
     * 
     * 
     * https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html
     * 
     * @param conn
     * @param tabeName
     * @return
     * @throws SQLException
     */
    public List<String> getColumns(String tabeName) throws SQLException {
        List<String> items = new ArrayList<>();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet resultSet = databaseMetaData.getColumns(this.databaseName, null, tabeName, null);
        try {
            while (resultSet.next()) {
                // Print
                // System.out.println(resultSet.getString("COLUMN_NAME"));
                items.add(resultSet.getString("COLUMN_NAME"));
            }
        } catch (Exception e) {

            throw new SQLException(e);

        } finally {
            resultSet.close();
        }
        return items;

    }

    public List<String> getTriggers(String tableName) throws SQLException {
        List<String> items = new ArrayList<>();
        DatabaseMetaData databaseMetaData = conn.getMetaData();

        ResultSet resultSet = databaseMetaData.getTables(this.databaseName, null, tableName,
                new String[] { "TRIGGER" });
        while (resultSet.next()) {
            items.add(resultSet.getString("TABLE_NAME"));
        }
        resultSet.close();
        return items;
    }

    public Boolean executeSql(String sqlText) throws SQLException {

        Statement stmt = null;
        stmt = conn.createStatement();

        // Let us check if it returns a true Result Set or not.
        Boolean ret = stmt.execute(sqlText);
        stmt.close();
        return ret;

    }

    /**
     * 
     * ToDos: Don't for get to figure out how to close the stmnt and rs , Executes a
     * quey and sets the instance rs variable to be used later.
     * 
     * @param selectQuery
     * @return Boolean stating if there are records
     * @throws Exception
     */
    public Boolean query(String selectQuery) throws Exception {
        Boolean hasRecords = false;

        Statement stmt = this.conn.createStatement();
        this.rs = stmt.executeQuery(selectQuery);
        if (this.rs.next() == true) {
            hasRecords = true;
            this.rs.beforeFirst();
        }
        rs.close();
        stmt.close();
        return hasRecords;
    }

    /**
     * Given a query will return the 1st column on the first row. Yes just that
     * simple
     * 
     * @param selectQuery
     * @return
     * @throws Exception
     */
    public String GetAValue(String selectQuery) throws Exception {

        String aValue = null;
        Statement stmt = this.conn.createStatement();
        this.rs = stmt.executeQuery(selectQuery);
        if (this.rs.next() == true) {
            aValue = rs.getString(1);
        }
        rs.close();
        stmt.close();
        return aValue;
    }

    public List<String[]> QueryToList(Task task) throws Exception {
        List<String[]> items = new ArrayList<>();

        Statement stmt = this.conn.createStatement();
        this.rs = stmt.executeQuery(task.getSql());
        ResultSetMetaData metadata = this.rs.getMetaData();
        int columnCount = metadata.getColumnCount();
    
        while (this.rs.next()) {
            String[] row = new String[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                row[i - 1] = (this.rs.getString(i));
            }
            items.add(row);
        }
        this.rs.close();
        stmt.close();

        return items;
    }

    /**
     * Take a query and writes it to a CSV file with the header
     * 
     * @param selectQuery
     * @param fullFilePath
     * @throws Exception
     */
    public void QueryToCSV(Task task) throws Exception {
        // System.out.println(selectQuery);

        // System.out.println("Writing to file: " + fullFilePath);
        Statement stmt = this.conn.createStatement();
        ResultSet rs = stmt.executeQuery(task.getSql());
        // int numCols = rs.getMetaData().getColumnCount();
        // System.out.println(selectQuery);

        CSVWriter writer = new CSVWriter(new FileWriter(task.getWritePath()));
        Boolean includeHeaders = true;

        writer.writeAll(rs, includeHeaders);

        writer.close();
        rs.close();
        stmt.close();
    }

    public void UploadImage(Task task) throws Exception {
        if(task.getFolder()) {
            System.out.println(this.GetAValue(String.format("SELECT COUNT(*) FROM %s", task.getDatabase())));

            List<File> filesInFolder = Files.walk(Paths.get(task.getFilePath()))
                .filter(p -> p.toString().endsWith("png") || p.toString().endsWith("jpeg"))
                .map(Path::toFile)
                .collect(Collectors.toList());

            for(File f : filesInFolder){
                int rowNum = Integer.parseInt(this.GetAValue(String.format("SELECT COUNT(*) FROM %s", task.getDatabase()))) + 1;
                DataUtils.UploadImage(this.conn, task.getinsertStatement(), f.getAbsolutePath(), rowNum);
            }
        } else {
            int rowNum = Integer.parseInt(this.GetAValue(String.format("SELECT COUNT(*) FROM %s", task.getDatabase()))) + 1;
            DataUtils.UploadImage(this.conn, task.getinsertStatement(), task.getFilePath(), rowNum);
        }
    }

    public void QuertyToCSVOutputBinary(Task task) throws Exception {
        Statement stmt = this.conn.createStatement();
        ResultSet rs = stmt.executeQuery(task.getSql());
        ResultSetMetaData metadata = rs.getMetaData();

        int columnCount = metadata.getColumnCount();
        List<Integer> imageColIndex = new ArrayList<Integer>();
        List<Integer> stringColIndex = new ArrayList<Integer>();
        String[] allColumnNames = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            String type = metadata.getColumnTypeName(i);
            String columnName = metadata.getColumnName(i);
            System.out.println(columnName + " -- " + type);
            List<String> dataTypes = Arrays.asList("VARBINARY", "BINARY", "CLOB", "BLOB", "IMAGE");

            if (dataTypes.contains(type.toUpperCase())) {
                imageColIndex.add(i);
            } else {
                stringColIndex.add(i);
            }
            allColumnNames[i - 1] = columnName;
        }

        List<String[]> data = new ArrayList<String[]>();
        allColumnNames = Arrays.copyOf(allColumnNames, allColumnNames.length + imageColIndex.size());
       
        int ii = 0;
        while (rs.next()) {
            ii++;
            String[] row = new String[columnCount];

            for (int stringIdx : stringColIndex) {
                row[stringIdx - 1] = rs.getString(stringIdx);
            }
            for (int imgIdx : imageColIndex) {

                byte[] blobBytes = rs.getBytes(imgIdx);

                String md5Hex = DigestUtils.md5Hex(blobBytes).toUpperCase();

                // change name here
                String dirPath = task.getWritePath() + "/image/" + md5Hex;
                File blobFile = new File(dirPath);
                if (!blobFile.getParentFile().exists()) {
                    blobFile.getParentFile().mkdirs();
                }
                FileOutputStream fos = new FileOutputStream(blobFile);

                fos.write(blobBytes);
                fos.close();
                row[imgIdx - 1] = dirPath;
                if (Math.floorMod(ii, 1000) == 0) {
                    System.out.println("Records Dumped: " + ii);
                }
            }
            data.add(row);
            
        }

        CSVWriter writer = new CSVWriter(new FileWriter(task.getWritePath() + "/index.csv"));
        Boolean includeHeaders = true;
        // this is the header
        data.add(0, allColumnNames);
        writer.writeAll(data, includeHeaders);

        writer.close();
        rs.close();
        stmt.close();
    }

    public void QueryToExcel(Task task) throws Exception {
        Statement stmt = this.conn.createStatement();

        /* Define the SQL query */
        ResultSet rs = stmt.executeQuery(task.getSql());
        /* Create Map for Excel Data */
        Map<String, Object[]> excel_data = new HashMap<String, Object[]>(); // create a map and define data
        int row_counter = 0;

        ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();
        List<String> columnNames = new ArrayList<>();

        for (int i = 1; i <= columnCount; i++) {

            columnNames.add(metadata.getColumnName(i));
        }

        /* Populate data into the Map */
        while (rs.next()) {
            row_counter = row_counter + 1;

            String[] row_data = new String[columnCount];

            // Data rows
            for (int i = 1; i <= columnCount; i++) {
                row_data[i - 1] = rs.getString(columnNames.get(i - 1));
            }

            excel_data.put(Integer.toString(row_counter), row_data);
        }
        /* Close all DB related objects */
        rs.close();
        stmt.close();
        DataUtils.WriteToExcel(columnNames, excel_data, "Sheet1", task.getWritePath());
    }

    /**
     * still in progress
     * 
     * @param tableName
     * @param filePath
     * @throws IOException
     * @throws SQLException
     */
    public void loadCSV(String tableName, String filePath) throws IOException, SQLException {
        List<String> tableColumns;

        PreparedStatement preparedStatement = null;

        List<String> question = new ArrayList<>();
        tableColumns = this.getColumns(tableName);
        for (int i = 0; i < tableColumns.size(); i++) {
            question.add("?");
        }
        String columns = String.join(",", question);
        String sql = "Insert into " + tableName + " values(" + columns + ")";
        System.out.println(sql);

        preparedStatement = this.conn.prepareStatement(sql);

        // CSVFormat fmt =
        // CSVFormat.DEFAULT.withDelimiter(',').withQuote('"').withRecordSeparator("\r\n");

        Reader file = new FileReader(filePath);

        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(file);
        for (CSVRecord record : records) {
            for (int i = 1; i <= tableColumns.size(); i++) {
                // TO remove. Better to check column types
                if (i == 1){
                    int val = Integer.parseInt(record.get(tableColumns.get(i-1)));
                    preparedStatement.setInt(i, val);
                } else{
                    preparedStatement.setString(i, record.get(tableColumns.get(i-1)));
                }
            }
            preparedStatement.addBatch();
        }

        int[] affectedRecords = preparedStatement.executeBatch();
        System.out.println("Total rows Inserted: " + affectedRecords.length);
        preparedStatement.close();

    }

    public String getSybaseViewDDL(String schemaName, String viewName) throws SQLException {

        Statement stmt = null;
        String sql = "select distinct obj.name, c.text from dbo.sysobjects obj  join dbo.syscomments c on obj.id=c.id"
                + "   where obj.type = 'V'  and USER_NAME(uid)='" + schemaName + "' and obj.name='" + viewName
                + "' order by 1";
        stmt = this.conn.createStatement();
        // Let us check if it returns a true Result Set or not.
        ResultSet rs = stmt.executeQuery(sql);
        String currDDL = null;
        while (rs.next()) {
            String snippetDDL = rs.getString(2);
            currDDL = (snippetDDL == null) ? currDDL + " " : currDDL + snippetDDL;
        }
        rs.close();
        stmt.close();

        return currDDL;
    }

    public String getSybaseCode(String name, String objType) throws SQLException {

        Statement stmt = null;
        String sql = "select distinct obj.type, obj.name, c.text from dbo.sysobjects obj join dbo.syscomments c on obj.id=c.id"
                + "  where obj.type in ('" + objType + "')  and obj.name='" + name + "' order by 1";
        stmt = this.conn.createStatement();
        // Let us check if it returns a true Result Set or not.

        ResultSet rs = stmt.executeQuery(sql);
        String currDDL = null;
        while (rs.next()) {
            String snippetDDL = rs.getString(3);
            currDDL = (snippetDDL == null) ? currDDL + " " : currDDL + snippetDDL;
        }
        rs.close();
        stmt.close();

        return currDDL;
    }

    /**
     * place hold to upload images
     * 
     * @param conn
     * @param file
     * @param uniqueid
     * @throws SQLException
     * @throws IOException
     */
    public static void uploadImage(Connection conn, String filep, int uniqueid) throws SQLException, IOException {
        File file = new File(filep);

        String filename = file.getName();
        int length = (int) file.length();

        FileInputStream filestream = null;

        filestream = new FileInputStream(file);

        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        // String query = "UPDATE assignment SET instructions_file = ?,
        // instructions_filename = ? WHERE a_key = " + uniqueid;
        String query = "INSERT INTO blobtest (pid, img) VALUES (2, ?)";
        PreparedStatement ps = conn.prepareStatement(query);
        ps.setBinaryStream(1, filestream, length);
        // ps.setString(2, filename);
        ps.execute();
        ps.close();
        stmt.close();
        filestream.close();
    }

    /**
     * place holder logic to download image
     * 
     * @param conn
     * @param primaryKey
     * @throws IOException
     * @throws SQLException
     * @throws FileNotFoundException
     */
    public void downloadImage(String tableName, String columnName, int primaryKey, String filePath)
            throws FileNotFoundException, SQLException, IOException {
        Statement stmt = this.conn.createStatement();
        String query = "SELECT " + columnName + "  FROM " + tableName + " WHERE pid = " + primaryKey;
        System.out.println(query);
        ResultSet rs = stmt.executeQuery(query);

        if (rs.next()) {
            File blobFile = null;
            blobFile = new File(filePath);

            Blob blob = rs.getBlob(columnName);
            InputStream in = blob.getBinaryStream();

            int length = in.available();
            byte[] blobBytes = new byte[length];
            in.read(blobBytes);

            FileOutputStream fos = new FileOutputStream(blobFile);
            fos.write(blobBytes);
            fos.close();
            rs.close();
            stmt.close();

        }

    }

    public Map<Integer, String> outputBinary(String path, String tableName, String columnName, String columnType)
            throws FileNotFoundException, SQLException, IOException {
        Map<Integer, String> results = new HashMap<Integer, String>();
        Statement stmt = this.conn.createStatement();
        String query = "SELECT " + columnName + " FROM " + tableName;

        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            Blob blob = rs.getBlob(columnName);
            InputStream in = blob.getBinaryStream();

            int length = in.available();
            byte[] blobBytes = new byte[length];
            in.read(blobBytes);

            String md5Hex = DigestUtils.md5Hex(blobBytes).toUpperCase();

            // change name here
            String dirPath = path + "/" + columnType + "/" + md5Hex;
            File blobFile = new File(dirPath);
            if (!blobFile.getParentFile().exists()) {
                blobFile.getParentFile().mkdirs();
            }
            FileOutputStream fos = new FileOutputStream(blobFile);

            results.put(rs.getRow(), dirPath);
            fos.write(blobBytes);
            fos.close();
        }

        rs.close();
        stmt.close();
        return results;
    }

    public void getSybaseStoredProcs() throws SQLException {
        String query = "SELECT u.name as name1, o.name, c.text FROM sysusers u, syscomments c, sysobjects o "
                + "WHERE o.type = 'P' AND o.id = c.id AND o.uid = u.uid  ORDER BY o.id, c.colid";

        Statement stmt = this.conn.createStatement();

        ResultSet rs = stmt.executeQuery(query);

        while (rs.next()) {
            String name1 = rs.getString("name1");
            String name = rs.getString("name");
            String txt = rs.getString("text");

            System.out.println(name1 + ", " + name + ", " + txt);
        }

    }
}