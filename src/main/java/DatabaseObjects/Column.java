package databaseobjects;

public class Column {
    public String columnName;
    public String columnType;
    public int index;
    public Column(String columnName,String columnType,int index){
        this.columnName=columnName;
        this.columnType=columnType;
        this.index=index;
    }
    public String toString() {
        return this.index +","+this.columnName + ":" + this.columnType;
    }
} 
