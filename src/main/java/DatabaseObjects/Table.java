package DatabaseObjects;

import java.util.ArrayList;
import java.util.List;

public class Table {
    public String tableName;
    public List<String> columnNames;
    public List<Column> columns;

    public Table(String tableName) {
        this.tableName = tableName;
        this.columnNames = new ArrayList<>();
        this.columns = new ArrayList<Column>();
    }

    public String toString() {// overriding the toString() method
        return "\n" + this.tableName + "\n\t -> " + this.columnNames;
    }

    public String[] TableInformation(){ 
        if(this.columns.size() >= 1){
            String[] arr = new String[this.columns.size() + 1];
            arr[0] = this.tableName;
            for(int i = 0; i < this.columns.size(); i++){
                arr[i+1] = this.columns.get(i).toString();
            }
            return arr;
        }
        return null;
    }
}