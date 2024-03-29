package objects;

public class Task {
    private String writePath;
    private String sql;
    private String connection;
    private String qualifier;
    private String taskName;
    private String insertStatement;
    private String database;
    private String filePath;
    private Boolean folder;
    private String table;

    public Task() {
    }

    public Task(String writePath, String sql) {
        this.writePath = writePath;
        this.sql = sql;
    }

    // public Task(String sql){
    //     this.sql = sql;
    // }

    // public Task(String insertStatement, String database, String filePath, Boolean folder) {
    //     this.database = database;
    //     this.insertStatement = insertStatement;
    //     this.filePath = filePath;
    //     this.folder = folder;
    // }

    public String getTable(){
        return this.table;
    }

    public void setTable(String table){
        this.table = table;
    }

    public Boolean getFolder() {
        return this.folder;
    }

    public void setFolder(boolean folder) {
        this.folder = folder;
    }

    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getinsertStatement() {
        return this.insertStatement;
    }

    public void setinsertStatement(String insertStatement) {
        this.insertStatement = insertStatement;
    }

    public String getFilePath() {
        return this.filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getWritePath() {
        return this.writePath;
    }

    public void setWritePath(String writePath) {
        this.writePath = writePath;
    }

    public String getSql() {
        return this.sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getConnection() {
        return this.connection;
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    public String getQualifier() {
        return this.qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getTaskName() {
        return this.taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public Task writePath(String writePath) {
        this.writePath = writePath;
        return this;
    }

    public Task sql(String sql) {
        this.sql = sql;
        return this;
    }

    public Task connection(String connection) {
        this.connection = connection;
        return this;
    }

    public Task qualifier(String qualifier) {
        this.qualifier = qualifier;
        return this;
    }

    public Task taskName(String taskName) {
        this.taskName = taskName;
        return this;
    } 
}