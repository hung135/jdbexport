# JDBExport
Java - JDBC RDBMS export Util

### Using the tool:
Simply download the release and run the following: `java -jar {JARPATH} -d {debug-level} -y {connections.yaml} -t {tasks.yaml}`

Setting up the connections.yaml:
```yaml
name-of-connection:
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

Debug levels:
<table>
  <th>Level</th><th>Config</th>
<tr>
  <td>
    All
  </td>
<td>

  ```yaml
log4j.rootLogger=all, stdout, R

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout

# Pattern to output the caller's file name and line number.
log4j.appender.stdout.layout.ConversionPattern=%5p [%t] (%F:%L) - %m%n

log4j.appender.R=org.apache.log4j.RollingFileAppender
log4j.appender.R.File=error.log

log4j.appender.R.MaxFileSize=100KB
# Keep one backup file
log4j.appender.R.MaxBackupIndex=1

log4j.appender.R.layout=org.apache.log4j.PatternLayout
log4j.appender.R.layout.ConversionPattern=%p %t %c - %m%n
```

</td>

<tr>
  <td>
    debug
  </td>
<td>

  ```yaml
log4j.rootLogger=debug, stdout, R

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout

# Pattern to output the caller's file name and line number.
log4j.appender.stdout.layout.ConversionPattern=%5p [%t] (%F:%L) - %m%n

log4j.appender.R=org.apache.log4j.RollingFileAppender
log4j.appender.R.File=debug.log

log4j.appender.R.MaxFileSize=100KB
# Keep one backup file
log4j.appender.R.MaxBackupIndex=1

log4j.appender.R.layout=org.apache.log4j.PatternLayout
log4j.appender.R.layout.ConversionPattern=%p %t %c - %m%n
```

</td>

<tr>
  <td>
    default
  </td>
<td>

  ```yaml
log4j.rootLogger=error, stdout

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout

# Pattern to output the caller's file name and line number.
log4j.appender.stdout.layout.ConversionPattern=%5p [%t] (%F:%L) - %m%n
```

</td>


</tr>

<tr>
  <td>
    warning
  </td>
<td>

  ```yaml
log4j.rootLogger=warn, stdout, R

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout

# Pattern to output the caller's file name and line number.
log4j.appender.stdout.layout.ConversionPattern=%5p [%t] (%F:%L) - %m%n

log4j.appender.R=org.apache.log4j.RollingFileAppender
log4j.appender.R.File=warning.log

log4j.appender.R.MaxFileSize=100KB
# Keep one backup file
log4j.appender.R.MaxBackupIndex=1

log4j.appender.R.layout=org.apache.log4j.PatternLayout
log4j.appender.R.layout.ConversionPattern=%p %t %c - %m%n
```

</td>


</tr>
</table>


Supports:
- Exporting a query result to CSV (QueryToCSV)
- Exporting a query result to Excel (QueryToExcel)
- For quick creations use (QueryToList)
- Exporting binary MD5 hash to CSV (QuertyToCSVOutputBinary)
- UploadImage both single and entire paths 
- UploadCSV (takes into consideration the **first** column being an **int** will refactor soon)
