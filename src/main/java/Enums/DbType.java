package enums;
public enum DbType {
    // jdbc:oracle:thin:scott/tiger@//myhost:1521/myservicename
    ORACLE("oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@{0}:{1}:{2}"),
    ORACLESID("oracle.jdbc.OracleDriver",
            "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={0})(PORT={1})))(CONNECT_DATA=(SERVICE_NAME={2})))"),
    SYBASE("net.sourceforge.jtds.jdbc.Driver", "jdbc:jtds:sybase://{0}:{1}/{2}"),
    POSTGRES("org.postgresql.Driver", "jdbc:postgresql://{0}:{1}:{2}"),
    MYSQL("com.mysql.jdbc.Driver", "jdbc:mysql://{0}:{1}/{2}");

    private String driver;
    private String url;

    DbType(String driver, String url) {
        this.driver = driver;
        this.url = url;
    }

    public String driver() {
        //System.out.print(this.driver);
        return driver;
    }

    public String url() {
        return url;
    }

    public static DbType getMyEnumIfExists(String value) {
        for (DbType db : DbType.values()) {
            if (db.name().equalsIgnoreCase(value))
                return db;
        }
        return null;
    }
}