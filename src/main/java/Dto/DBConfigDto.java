package Dto;

import java.util.Objects;

public class DBConfigDto {

    public DBConfigDto(String dbtype, String host, Integer port, String user, String password_envar, String database_name) {
        this.dbtype = dbtype;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password_envar = password_envar;
        this.database_name = database_name;
    }

    public String getDbtype() {
        return this.dbtype;
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

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof DBConfigDto)) {
            return false;
        }
        DBConfigDto dbConnConfig = (DBConfigDto) o;
        return Objects.equals(dbtype, dbConnConfig.dbtype) && Objects.equals(host, dbConnConfig.host) && Objects.equals(port, dbConnConfig.port) && Objects.equals(user, dbConnConfig.user) && Objects.equals(password_envar, dbConnConfig.password_envar) && Objects.equals(database_name, dbConnConfig.database_name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbtype, host, port, user, password_envar, database_name);
    }

    @Override
    public String toString() {
        return "{" +
            " dbtype='" + getDbtype() + "'" +
            ", host='" + getHost() + "'" +
            ", port='" + getPort() + "'" +
            ", user='" + getUser() + "'" +
            ", password_envar='" + getPassword_envar() + "'" +
            ", database_name='" + getDatabase_name() + "'" +
            "}";
    }

    public DBConfigDto() {
    }
 
 
    private String dbtype;
    private String host;
    private Integer port;
    private String user;
    private String password_envar;
    private String database_name;
}