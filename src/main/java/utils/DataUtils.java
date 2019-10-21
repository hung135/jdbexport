package utils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import objects.DbConn;

/**
 * Functions we need to write that process some data, put them here
 */
public class DataUtils {
    // final static Logger logger = Logger.getLogger(DataUtils.class);

    // Generic function to convert list to set
    public static <T> Set<T> ConvertListToSet(List<T> list) {
        // create an empty set
        Set<T> set = new HashSet<>();

        // Add each element of list into the set
        for (T t : list)
            set.add(t);

        // return the set
        return set;
    }

    public static void CallTest(DbConn conn, List<DbConn> targetConnections) {
        System.out.println(targetConnections.size());
        // logger.log("My Message");
    }

    /**
     * Take a regex and a data string and extract all the data that matches the
     * regex and returns it as a collection of string
     * 
     * @param regExString
     * @param dataString
     */
    public static Set<String> RegexExtract(String regExString, String dataString) {
        Set<String> xx = new HashSet<>();

        Pattern pattern = Pattern.compile(regExString);
        Matcher matcher = pattern.matcher(dataString);
        while (matcher.find()) {
            // System.out.println(matcher.group());
            xx.add(matcher.group());
        }

        return xx;
    }

    public static Set<String> FindSybaseDatabase(String dataString) {
        Set<String> xx = new HashSet<>();

        Pattern pattern = Pattern.compile("\\w*\\.\\.");
        Matcher matcher = pattern.matcher(dataString);
        while (matcher.find()) {
            // System.out.println(matcher.group());
            xx.add(matcher.group().replace(".", ""));
        }

        return xx;
    }

    public static Set<String> FindTablesFromInsert(String dataString) {
        String x = dataString;
        x = x.replaceAll("\n", " ");
        x = x.replaceAll("/\\*.*\\*/", " ");
        System.out.println(x);
        x = x.replaceAll("(?i)insert (?i)into ", "~~");
        System.out.println(x);
        x = x.replaceAll(" .*?~~", "~~");
        x = x.replaceAll(" .*?$", "~~");
        System.out.println(x);
        List<String> items = Arrays.asList(x.split("~~"));
        return ConvertListToSet(items);
    }

    public static Set<String> FindTablesFromQuery(String dataString) {
        String x = dataString;
        x = x.replaceAll("\n", " ");
        x = x.replaceAll("^.*? select ", " select ");
        x = x.replaceAll("(?i)select .*? from ", "~~");
        x = x.replaceAll(" from ", "~~");
        x = x.replaceAll(" join ", "~~");
        x = x.replaceAll("(?i)where .*?\\(", "~~");
        x = x.replaceAll("\\).*?\\(", "~~");
        x = x.replaceAll("~~.*?(?i) from ", "~~");
        x = x.replaceAll("(?i)select .*? from ", "~~");
        x = x.replaceAll(" ", "");
        x = x.replaceAll("\\)~", "~");
        x = x.replaceAll("\\).*$", "~~");
        x = x.replaceAll("~~~~", "~~");
        List<String> items = Arrays.asList(x.split("~~"));
        return ConvertListToSet(items);

    }

    public static void GetSybaseStoredProcs(Connection conn) throws SQLException {

        String query = "SELECT u.name as name1, o.name, c.text FROM sysusers u, syscomments c, sysobjects o "
                + "WHERE o.type = 'P' AND o.id = c.id AND o.uid = u.uid  ORDER BY o.id, c.colid";

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery(query);

        while (rs.next()) {
            String name1 = rs.getString("name1");
            String name = rs.getString("name");
            String txt = rs.getString("text");

            System.out.println(name1 + ", " + name + ", " + txt);
        }
        rs.close();
        stmt.close();

    }

    public static void WriteListToCSV(List<String[]> stringList, String fullFilePath) throws Exception {
        try {
            CSVWriter writer = new CSVWriter(new FileWriter(fullFilePath));

            Boolean includeHeaders = true;

            writer.writeAll(stringList, includeHeaders);
            writer.close();

        } catch (Exception e) {
            System.out.println(e);
            // logger.error("Exception " + e.getMessage());
        }
    }

    private static boolean CompareRow(String[] row1, String[] row2) {
        if (row1 == null || row2 == null) {
            return false;
        }
        if (row1.length == row2.length) {
            for (int i = 0; i < row1.length; i++) {
                if (!row1[i].toLowerCase().toString().equals(row2[i].toString().toLowerCase())) {
                    return false;
                }
            }
        } else {
            return false;
        }
        return true;
    }

    private static List<String[]> ReadCSV(String csvPath) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(csvPath));
        List<String[]> results = reader.readAll();
        reader.close();
        return results;
    }

    public static Map<Integer, String> OutputBinary(Connection conn, String path, String tableName, String columnName,
            String columnType) throws FileNotFoundException, SQLException, IOException {
        Map<Integer, String> results = new HashMap<Integer, String>();
        Statement stmt = conn.createStatement();
        String query = "SELECT " + columnName + " FROM " + tableName;

        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            Blob blob = rs.getBlob(columnName);
            InputStream in = blob.getBinaryStream();

            int length = in.available();
            byte[] blobBytes = new byte[length];
            int bytestRead = in.read(blobBytes);

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

    /**
     * place holder logic to download image
     * 
     * @param conn
     * @param primaryKey
     * @throws IOException
     * @throws SQLException
     * @throws FileNotFoundException
     */
    public static void DownloadImage(Connection conn, String tableName, String columnName, int primaryKey,
            String filePath) throws FileNotFoundException, SQLException, IOException {

        Statement stmt = conn.createStatement();
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
            int bytestRead = in.read(blobBytes);

            FileOutputStream fos = new FileOutputStream(blobFile);
            fos.write(blobBytes);
            fos.close();

        }
        rs.close();
        stmt.close();

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
    public static void UploadImage(Connection conn, String insert, String filep, int pid) throws SQLException, IOException {
        File file = new File(filep);

        String filename = file.getName();
        int length = (int) file.length();

        FileInputStream filestream = null;

        filestream = new FileInputStream(file);

        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        String query = String.format(insert, Integer.toString(pid));
        PreparedStatement ps = conn.prepareStatement(query);
        ps.setBinaryStream(1, filestream, length);
        // ps.setString(2, filename);
        ps.execute();
        ps.close();
        stmt.close();
        filestream.close();
    }

    /**
     * Place holder for hdf5
     * 
     * @throws HDF5Exception
     * @throws NullPointerException
     * @throws HDF5LibraryException
     * @throws IOException
     */
    public void Hdf5load(String fileName)
            throws HDF5LibraryException, NullPointerException, HDF5Exception, IOException {

        DataOutputStream file = new DataOutputStream(new FileOutputStream("result.bin"));
        int data[][], datasetID, dataspaceID, i, j, rows, fileID;
        fileID = H5.H5Fopen(fileName, HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT);

        datasetID = H5.H5Dopen(fileID, "my_dataset");
        // we assume that the file was opened
        // previously
        dataspaceID = H5.H5Dget_space(datasetID);
        rows = (int) (H5.H5Sget_simple_extent_npoints(dataspaceID) / 1024);
        data = new int[rows][1024];
        H5.H5Dread(datasetID, HDF5Constants.H5T_NATIVE_INT, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                HDF5Constants.H5P_DEFAULT, data);
        // fn_order(data); // call hypothetical function that orders "data" in an
        // ascending way
        for (i = 0; i < rows; i++)
            for (j = 0; j < 1024; j++)
                file.writeInt(data[i][j]);
        file.close();
        H5.H5Sclose(dataspaceID);
        H5.H5Dclose(datasetID);
    }

    /**
     * 
     */

    public static void WriteToExcel(List<String> columnNames, Map<String, Object[]> excel_data, String sheetName,
            String fullFilePath) throws Exception {

        /* Create Workbook and Worksheet objects */
        HSSFWorkbook new_workbook = new HSSFWorkbook(); // create a blank workbook object
        HSSFSheet sheet = new_workbook.createSheet(sheetName); // create a worksheet with caption score_details
        int columnCount = columnNames.size();

        /* Load data into logical worksheet */
        Set<String> keyset = excel_data.keySet();
        int rownum = 0;
        // Header Row
        Row r = sheet.createRow(rownum);
        for (int i = 1; i <= columnCount; i++) {

            r.createCell(i - 1).setCellValue(columnNames.get(i - 1));

        }
        rownum++;
        for (String key : keyset) { // loop through the data and add them to the cell
            Row row = sheet.createRow(rownum++);
            Object[] objArr = excel_data.get(key);
            int cellnum = 0;
            for (Object obj : objArr) {
                Cell cell = row.createCell(cellnum++);
                if (obj instanceof Double)
                    cell.setCellValue((Double) obj);
                else
                    cell.setCellValue((String) obj);
            }
        }

        FileOutputStream output_file = new FileOutputStream(new File(fullFilePath)); // create XLS file
        new_workbook.write(output_file);// write excel document to output stream
        output_file.close(); // close the file
        new_workbook.close();
    }

    /**
     * Given list of columns , findex the index location of it in the header row
     * 
     * @param keyColumns
     * @param headerRow
     * @return
     */
    public static List<Integer> FindColumnIndex(List<String> keyColumns, String[] headerRow) {
        // System.out.println(keyColumns);
        List<Integer> headerIndex = new ArrayList<>();
        int ii = 0;
        for (String col : keyColumns) {

            int jj = 0;
            for (String headerCol : headerRow) {

                if (col.toLowerCase().equals(headerCol.toLowerCase())) {
                    headerIndex.add(jj);
                    // System.out.println(jj + col);
                }
                jj++;
            }
            ii++;
        }
        return headerIndex;

    }

    /**
     * Extract each keycolumn and makeit key, and the rest of the columns data
     * 
     * @param headerIndex
     * @param headerRow
     * @return
     */
    public static HashMap<String, String> FillHashMap(List<Integer> headerIndex, List<String[]> csv, String algorithm) {
        HashMap<String, String> mapCSVdata = new HashMap<>();
        for (String[] row : csv) {
            String hashKey = "";
            String data = "";
            // System.out.println("-------" + headerIndex);
            for (Integer keyColIndex : headerIndex) {

                if (hashKey.equals("")) {
                    hashKey = row[keyColIndex].toUpperCase();
                } else {
                    hashKey = hashKey + "-" + row[keyColIndex].toUpperCase();
                }
            }
            for (int i = 0; i < row.length; i++) {

                // casting to Integer object to use in contains check
                Integer iii = i;
                // if this is not a key column it is a data column
                if (!headerIndex.contains(iii)) {

                    if (data.equals("")) {
                        data = row[i];
                    } else {
                        data = data + "-" + row[i];
                    }
                }

            }
            String md5Hex = DigestUtils.md5Hex(data).toUpperCase();
            if (algorithm.toLowerCase().equals("hash")) {

                mapCSVdata.put(hashKey, md5Hex);

            } else {

                mapCSVdata.put(hashKey, data);
            }

        }

        return mapCSVdata;
    }

    public static void CompareCSV(String firstCSV, String secondCSV, String outFile, List<String> primaryColumn,
            String reportHeader, String algorithm) throws Exception {
        String[] headersCSV1, headersCSV2;
        List<String[]> csv1, csv2, results;
        // System.out.println(primaryColumn);
        // outputHeaders = new String[] { "File1", "File2", "Reason", "Primary Column"
        // };
        // results = new ArrayList<String[]>() {
        // {
        // add(outputHeaders);
        // }
        // };
        csv1 = ReadCSV(firstCSV);
        csv2 = ReadCSV(secondCSV);

        headersCSV1 = csv1.get(0);
        headersCSV2 = csv2.get(0);

        csv1.remove(0);
        csv2.remove(0);

        List<Integer> headerIndex1 = FindColumnIndex(primaryColumn, headersCSV1);
        List<Integer> headerIndex2 = FindColumnIndex(primaryColumn, headersCSV2);

        // Build array of primarykey column index locations

        if (!CompareRow(headersCSV1, headersCSV2)) {
            throw new Exception("Headers do not match");
        }

        HashMap<String, String> mapCSVdata1 = FillHashMap(headerIndex1, csv1, algorithm);
        HashMap<String, String> mapCSVdata2 = FillHashMap(headerIndex2, csv2, algorithm);
        HashMap<String, String[]> descrpencyMap = new HashMap<>();

        for (Map.Entry<String, String> entry : mapCSVdata1.entrySet()) {
            String keyCSV1 = entry.getKey();
            String valCSV1 = entry.getValue().toUpperCase();
            String valCSV2 = mapCSVdata2.get(keyCSV1);
            if (!valCSV1.equals(valCSV2)) {
                // add to final table
                String[] xxx = { valCSV1, valCSV2 };
                descrpencyMap.put(keyCSV1, xxx);
            }

        }

        for (Map.Entry<String, String> entry : mapCSVdata2.entrySet()) {
            String keyCSV2 = entry.getKey();
            String valCSV2 = entry.getValue();
            String valCSV1 = mapCSVdata1.get(keyCSV2);
            if (!valCSV2.equals(valCSV1)) {
                String[] xxx = { valCSV1, valCSV2 };
                descrpencyMap.put(keyCSV2, xxx);
            }

        }
        results = new ArrayList<>();

        results.add(reportHeader.split(","));
        for (Map.Entry<String, String[]> row : descrpencyMap.entrySet()) {
            String key = row.getKey();
            String[] val = row.getValue();

            String[] xxx = { key, val[0], val[1] };
            results.add(xxx);

        }

        // System.out.println(primaryColumn + "--------xxx------------");
        // List<String> headers = Arrays.asList(headersCSV1).stream().map(s ->
        // s.toLowerCase())
        // .collect(Collectors.toList());
        // System.out.println(headers + "--------------------");
        // for (String column : primaryColumn) {
        // int index = headers.indexOf(column.toLowerCase());
        // if (index == -1) {
        // String missing = "Missing in header ";
        // results.add(new String[] { missing, missing, missing, column });
        // } else {
        // int xx = 0;
        // /** Fore each Row in Csv1 */
        // for (String[] row1 : csv1) {
        // boolean found = false;
        // /** For Each Row in Csv2 */
        // int ii = 0;
        // String[] xxx = null;
        // for (String[] row2 : csv2) {
        // ii++;
        // xx = csv2.size();
        // if (row1[index].equals(row2[index])) {
        // if (!compareRow(row1, row2)) {
        // for (int i = 0; i < row1.length; i++) {
        // if (!row1[i].toLowerCase().equals(row2[i].toLowerCase())) {
        // results.add(new String[] { row1[i], row2[i],
        // "Values mismatch on column:" + headers.get(i), column });
        // System.out.println("Values didn't match for key: " + column);
        // }
        // }
        // }
        // found = true;
        // /** Race condition caused by this removed withe for each above... */
        // // csv2.remove(row2);
        // xxx = row2;
        // break;
        // }
        // }
        // if (!found) {
        // results.add(new String[] { row1[index], "missing", "Values missing", column
        // });
        // csv1.remove(row1);
        // } else {
        // csv2.remove(xxx);
        // }
        // }
        // }
        // }
        WriteListToCSV(results, outFile);
    }

    public static void FreeWayMigrateMulti(DbConn srcDbConn, List<DbConn> trgConns, String sql, String tableName,
            int batchSize, boolean truncate) throws SQLException, IOException {
        long runningBytes = 0;
        long largestBytes = 0;
        Statement stmt = srcDbConn.conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(batchSize);

        // org code
        // String sql = String.Format("SELECT * FROM %s WHERE %s BETWEEN %d AND %d",
        // tableName, primaryKey, min, max);
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData metadata = rs.getMetaData();

        int columnCount = metadata.getColumnCount();
        List<Integer> binaryColIndex = new ArrayList<Integer>();
        List<Integer> numberColIndex = new ArrayList<Integer>();
        List<Integer> stringColIndex = new ArrayList<Integer>();
        List<Integer> timeColIndex = new ArrayList<Integer>();

        //srcDbConn.logger.debug(sql);
        String[] allColumnNames = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            String type = metadata.getColumnTypeName(i);

            String columnName = metadata.getColumnName(i);
            //System.out.println(columnName + "------" + type);
            // For now; later create custom enum. "image" isn't supported by JAVA
            List<String> dataTypes = Arrays.asList("VARBINARY", "BINARY", "CLOB", "BLOB", "IMAGE");
            List<String> numDataTypes = Arrays.asList("TINYINT", "INT", "SMALLINT", "NUMERIC IDENTITY");
            List<String> timeDataTypes = Arrays.asList("DATE", "TIMESTAMP", "DATETIME");

            if (dataTypes.contains(type.toUpperCase())) {
                binaryColIndex.add(i);
            } else if (numDataTypes.contains(type.toUpperCase())) {
                numberColIndex.add(i);
            } else if (timeDataTypes.contains(type.toUpperCase())) {
                timeColIndex.add(i);
            }
            else {
                stringColIndex.add(i);
            }
            allColumnNames[i - 1] = columnName;
        }

        //srcDbConn.logger.debug("640");
        // List<String[]> data = new ArrayList<String[]>();
        /*************************************** */
        // build the insert
        String columnsComma = String.join(",", allColumnNames);
        String columnsQuestion = "?";
        for (int jj = 0; jj < columnCount; jj++) {
            if (jj > 0)
                columnsQuestion = columnsQuestion + ",?";
        }
        String sqlInsert = "INSERT INTO " + tableName + " (" + columnsComma + ") VALUES (" + columnsQuestion + ")";
        String sqlTruncate = "Truncate table " + tableName;

        trgConns.forEach(dbconn -> {
            // System.out.println(sqlInsert+"---------sql create stmnt");
            try {
                dbconn.stmt = dbconn.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            } catch (SQLException e) {
                //srcDbConn.logger.debug(e);
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                if (truncate) {
                    dbconn.ps = dbconn.conn.prepareStatement(sqlTruncate);
                    System.out.println(sqlTruncate);
                    dbconn.ps.execute();
                    dbconn.ps.close();
                }
                dbconn.ps = dbconn.conn.prepareStatement(sqlInsert);
                dbconn.lastPSSql = sqlInsert;
            } catch (SQLException e) {
                e.printStackTrace();
            }

        });
        int ii = 0;
        while (rs.next()) {
            ii++;

            for (int stringIdx : stringColIndex) {
                String xxx = rs.getString(stringIdx);
                if (xxx != null) {
                    if (xxx.getBytes().length > largestBytes)
                        largestBytes = xxx.getBytes().length;
                    runningBytes += xxx.getBytes().length;
                }
                for (DbConn trgConn : trgConns) {
                    trgConn.ps.setString(stringIdx, xxx);
                }
            }
            for (int numIdx : numberColIndex) {
                String dataItem = rs.getString(numIdx);
                for (DbConn trgConn : trgConns) {
                    if (dataItem == null) {
                        trgConn.ps.setNull(numIdx, java.sql.Types.INTEGER);

                    } else {
                        trgConn.ps.setInt(numIdx, Integer.valueOf(dataItem));
                    }
                }
            }
            for (int imgIdx : binaryColIndex) {
                byte[] dataItem = rs.getBytes(imgIdx);

                if (dataItem.length > largestBytes) {
                    largestBytes = dataItem.length;
                }
                runningBytes += dataItem.length;
                for (DbConn trgConn : trgConns) {
                    trgConn.ps.setBytes(imgIdx, dataItem);
                }
            }
            for (int idx : timeColIndex) {
                java.sql.Date dataItem = rs.getDate(idx);
                for (DbConn trgConn : trgConns) {
                    trgConn.ps.setDate(idx, dataItem);
                }
            }
            for (DbConn trgConn : trgConns) {
                trgConn.ps.addBatch();
            }

            if (Math.floorMod(ii, batchSize) == 0) {
                // Execute the batch every 1000 rows
                for (DbConn trgConn : trgConns) {
                    trgConn.flushReset();
                    runningBytes = 0;
                    largestBytes = 0;
                    if (Math.floorMod(2 * ii, batchSize) == 0)
                        System.gc();

                }
            }
        }
        for (DbConn trgConn : trgConns) {
            trgConn.ps.executeBatch();
            trgConn.stmt.close();
        }

        rs.close();
        stmt.close();

    }

    public static void FreeWayMigrate(DbConn srcDbConn, List<DbConn> trgConns, List<String> tableNames, int batchSize,
            Boolean truncate) throws SQLException, IOException {

        long startTime = System.nanoTime();
        long runningBytes = 0;
        long largestBytes = 0;
        Statement stmt = srcDbConn.conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(batchSize);

        for (String tableName : tableNames) {
            String sql = "select * from " + tableName;
            System.out.println(sql);

            ResultSet rs = stmt.executeQuery(sql);
            ResultSetMetaData metadata = rs.getMetaData();

            int columnCount = metadata.getColumnCount();
            List<Integer> binaryColIndex = new ArrayList<Integer>();
            List<Integer> numberColIndex = new ArrayList<Integer>();
            List<Integer> stringColIndex = new ArrayList<Integer>();
            List<Integer> timeColIndex = new ArrayList<Integer>();

            String[] allColumnNames = new String[columnCount];
            // System.out.println("before loop");
            for (int i = 1; i <= columnCount; i++) {
                String type = metadata.getColumnTypeName(i);

                String columnName = metadata.getColumnName(i);
                System.out.println(columnName + "------" + type);
                // For now; later create custom enum. "image" isn't supported by JAVA
                List<String> dataTypes = Arrays.asList("VARBINARY", "BINARY", "CLOB", "BLOB", "IMAGE");
                List<String> numDataTypes = Arrays.asList("TINYINT", "INT", "SMALLINT");
                List<String> timeDataTypes = Arrays.asList("DATE", "TIMESTAMP", "DATETIME");

                if (dataTypes.contains(type.toUpperCase())) {
                    binaryColIndex.add(i);
                } else if (numDataTypes.contains(type.toUpperCase())) {
                    numberColIndex.add(i);
                } else if (timeDataTypes.contains(type.toUpperCase())) {
                    timeColIndex.add(i);
                }

                else {
                    stringColIndex.add(i);
                }
                allColumnNames[i - 1] = columnName;
            }

            // List<String[]> data = new ArrayList<String[]>();
            /*************************************** */
            // build the insert
            String columnsComma = String.join(",", allColumnNames);
            String columnsQuestion = "?";
            for (int jj = 0; jj < columnCount; jj++) {
                if (jj > 0)
                    columnsQuestion = columnsQuestion + ",?";
            }
            String sqlInsert = "INSERT INTO " + tableName + " (" + columnsComma + ") VALUES (" + columnsQuestion + ")";
            String sqlTruncate = "Truncate table " + tableName;

            trgConns.forEach(dbconn -> {
                // System.out.println(sqlInsert+"---------sql create stmnt");
                try {
                    dbconn.stmt = dbconn.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                try {
                    if (truncate) {
                        dbconn.ps = dbconn.conn.prepareStatement(sqlTruncate);
                        System.out.println(sqlTruncate);
                        dbconn.ps.execute();
                        dbconn.ps.close();
                    }
                    dbconn.ps = dbconn.conn.prepareStatement(sqlInsert);
                    dbconn.lastPSSql = sqlInsert;
                    // System.out.println(sqlInsert+"---------sql create stmnt");
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            });

            /*************************************** */
            int ii = 0;
            while (rs.next()) {
                ii++;

                // String[] row = new String[columnCount];

                for (int stringIdx : stringColIndex) {
                    // System.out.println(ii+"----------xxxx-----------------");
                    String xxx = rs.getString(stringIdx);
                    if (xxx != null) {
                        if (xxx.getBytes().length > largestBytes)
                            largestBytes = xxx.getBytes().length;
                        runningBytes += xxx.getBytes().length;
                    }
                    for (DbConn trgConn : trgConns) {
                        // System.out.println(row[stringIdx - 1]+"---------------------------");
                        // String xxx = row[stringIdx - 1];
                        // if (xxx==null)
                        // {xxx="";}
                        trgConn.ps.setString(stringIdx, xxx);

                        // System.out.println(ii+"--setstring-------------------------");
                    }
                }
                for (int numIdx : numberColIndex) {
                    // System.out.println(ii+"----------xxxx-----------------");
                    String dataItem = rs.getString(numIdx);
                    for (DbConn trgConn : trgConns) {
                        if (dataItem == null) {
                            trgConn.ps.setNull(numIdx, java.sql.Types.INTEGER);

                        } else {
                            trgConn.ps.setInt(numIdx, Integer.valueOf(dataItem));
                        }
                    }
                }
                for (int imgIdx : binaryColIndex) {

                    // byte[] dataItem = rs.getBytes(imgIdx);

                    byte[] dataItem = rs.getBytes(imgIdx);

                    if (dataItem.length > largestBytes) {
                        largestBytes = dataItem.length;
                    }
                    runningBytes += dataItem.length;
                    for (DbConn trgConn : trgConns) {
                        trgConn.ps.setBytes(imgIdx, dataItem);
                    }
                }
                for (int idx : timeColIndex) {
                    java.sql.Date dataItem = rs.getDate(idx);
                    for (DbConn trgConn : trgConns) {
                        trgConn.ps.setDate(idx, dataItem);
                    }
                }
                for (DbConn trgConn : trgConns) {
                    trgConn.ps.addBatch();
                }

                if (Math.floorMod(ii, batchSize) == 0) {
                    // Execute the batch every 1000 rows
                    for (DbConn trgConn : trgConns) {
                        long endTime = System.nanoTime();
                        long totalTime = endTime - startTime;
                        long totalTimeMins = TimeUnit.MINUTES.convert(totalTime, TimeUnit.NANOSECONDS);
                        long totalTimeSec = TimeUnit.SECONDS.convert(totalTime, TimeUnit.NANOSECONDS)
                                - (60 * totalTimeMins);
                        System.out.println(
                                "Executing Batch" + "  LoadTime: " + totalTimeMins + " Mins " + totalTimeSec + " Secs");

                        long heapSize = Runtime.getRuntime().totalMemory();

                        // Get maximum size of heap in bytes. The heap cannot grow beyond this size.//
                        // Any attempt will result in an OutOfMemoryException.
                        long heapMaxSize = Runtime.getRuntime().maxMemory();

                        // Get amount of free memory within the heap in bytes. This size will increase
                        // // after garbage collection and decrease as new objects are created.
                        long heapFreeSize = Runtime.getRuntime().freeMemory();

                        System.out.println("heapsize: " + ConvertToStringRepresentation(heapSize));
                        System.out.println("heapmaxsize: " + ConvertToStringRepresentation(heapMaxSize));
                        System.out.println("heapFreesize: " + ConvertToStringRepresentation(heapFreeSize));
                        System.out.println("Batch Memory Size: " + ConvertToStringRepresentation(runningBytes));
                        // System.out.println("Largest Size: " +
                        // convertToStringRepresentation(largestBytes) + " MBs");

                        trgConn.flushReset();

                        runningBytes = 0;
                        largestBytes = 0;
                        if (Math.floorMod(2 * ii, batchSize) == 0)
                            System.gc();

                    }

                    long endTime = System.nanoTime();
                    long totalTime = endTime - startTime;
                    long totalTimeMins = TimeUnit.MINUTES.convert(totalTime, TimeUnit.NANOSECONDS);
                    long totalTimeSec = TimeUnit.SECONDS.convert(totalTime, TimeUnit.NANOSECONDS)
                            - (60 * totalTimeMins);
                    System.out.println("Records Loaded: " + ii + "  LoadTime: " + totalTimeMins + " Mins "
                            + totalTimeSec + " Secs");
                }

            }
            for (DbConn trgConn : trgConns) {
                trgConn.ps.executeBatch();
                trgConn.stmt.close();
            }
            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            long totalTimeMins = TimeUnit.MINUTES.convert(totalTime, TimeUnit.NANOSECONDS);
            long totalTimeSec = TimeUnit.SECONDS.convert(totalTime, TimeUnit.NANOSECONDS) - (60 * totalTimeMins);
            System.out.println(
                    "Records Loaded: " + ii + "  LoadTime: " + totalTimeMins + " Mins " + totalTimeSec + " Secs");

            rs.close();
        }
        stmt.close();

    }

    /**
     * call the proper get method
     * 
     * @throws SQLException
     */
    public static void ResultSetGet(ResultSet rs, int colIdx, String colType) throws SQLException {
        rs.getString(colIdx);
    }

    private static String Format(final long value, final long divider, final String unit) {
        final double result = divider > 1 ? (double) value / (double) divider : (double) value;
        return String.format("%.1f %s", Double.valueOf(result), unit);
    }

    public static String ConvertToStringRepresentation(final long value) {

        final long K = 1024;
        final long M = K * K;
        final long G = M * K;
        final long T = G * K;

        final long[] dividers = new long[] { T, G, M, K, 1 };
        final String[] units = new String[] { "TB", "GB", "MB", "KB", "B" };
        if (value < 1)
            throw new IllegalArgumentException("Invalid file size: " + value);
        String result = null;
        for (int i = 0; i < dividers.length; i++) {
            final long divider = dividers[i];
            if (value >= divider) {
                result = Format(value, divider, units[i]);
                break;
            }
        }
        return result;
    }
}