package Dto;

import Enums.DbType;

public class DBConfigDto {

    public DBConfigDto(String dbtype, String host, Integer port, String user, String password_envar, String database_name) {
        this.dbtype = dbtype;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password_envar = password_envar;
        this.database_name = database_name;
    }

    public DbType getDbtype() {
        return DbType.getMyEnumIfExists(this.dbtype);
    }

    public void setDbtype(String dbtype) {
        this.dbtype = dbtype;
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return this.port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return System.getenv(this.password_envar);
    }

    public String getPassword_envar() {
        return this.password_envar;
    }

    public void setPassword_envar(String password_envar) {
        this.password_envar = password_envar;
    }

    public String getDatabase_name() {
        return this.database_name;
    }

    public void setDatabase_name(String database_name) {
        this.database_name = database_name;
    }

    public DBConfigDto dbtype(String dbtype) {
        this.dbtype = dbtype;
        return this;
    }

    public DBConfigDto host(String host) {
        this.host = host;
        return this;
    }

    public DBConfigDto port(Integer port) {
        this.port = port;
        return this;
    }

    public DBConfigDto user(String user) {
        this.user = user;
        return this;
    }

    public DBConfigDto password_envar(String password_envar) {
        this.password_envar = password_envar;
        return this;
    }

    public DBConfigDto database_name(String database_name) {
        this.database_name = database_name;
        return this;
    }

    public DBConfigDto() {
    }
 
 
    private String dbtype;
    private String host;
    private Integer port;
    private String user;
    private String password_envar;
    private String database_name;
    private String password;
}