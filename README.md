# JDBExport
Java - JDBC RDBMS export Util

### Using the tool:
Simply download the release and run the following: `java -jar {JARPATH} -y {connections.yaml} -t {tasks.yaml}`

Setting up the connections.yaml:
```yaml
name-of-conneciton:
  dbtype: "SYBASE"
  host: "dbsybase"
  port: "5000"
  user: "sa"
  password_envar: "SYBASEPASSWORD"
  database_name: "master"
```

Setting put the tasks.yaml:
```yaml
Name-Of-Function:
    name-of-connection:
        qualifer: # Under this are the parameters for the function above
            - param1
              parm2
```

Example:
```yaml
QueryToCSV:
  sys-dev:
    create:
      - writePath: /workspace/output1.csv
        sql: select * from dbo.monBucketPool
```

Supports:
-  Exporting a query result to CSV (QueryToCSV)
- more coming!