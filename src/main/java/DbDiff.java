import java.beans.PropertyVetoException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.calcite.util.JsonBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import Dto.DBConfigDto;
import Objects.DbConn;
import Objects.Task;
import Utils.YamlParser;

public class DbDiff {

    public static CommandLine ParseCLI(String[] args) throws ParseException {
        Options cliOptions = new Options();
        Option connections = new Option("y", "connections", true, "Path to connections file");
        Option tasks = new Option("t", "tasks", true, "Path to tasks file");
        connections.setRequired(true);
        tasks.setRequired(true);

        cliOptions.addOption(connections);
        cliOptions.addOption(tasks);

        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Database diff tool", cliOptions);
            System.exit(1);
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(cliOptions, args);

        return cmd;
    }

    public static Map<String, DbConn> BuildConnections(Map<String, Object> cons) throws JsonParseException,
            JsonMappingException, IOException, ClassNotFoundException, SQLException, PropertyVetoException {
        Map<String, DbConn> dbConfigs = new HashMap<String, DbConn>();
        JsonBuilder builder = new JsonBuilder();
        ObjectMapper mapper =new ObjectMapper();

        for(String key : cons.keySet()){
            String jsonString = builder.toJsonString(cons.get(key));
            DBConfigDto dto = mapper.readValue(jsonString,  new TypeReference<DBConfigDto>(){});
            DbConn connectionObject = new DbConn(dto);
            
            dbConfigs.put(key, connectionObject);
        }
        return dbConfigs;
    }

    public static List<Task> TestOutput(Map<String, Object> ob) 
        throws JsonParseException, JsonMappingException, IOException {
   
        List<Task> tasks = new ArrayList<Task>();
        JsonBuilder builder = new JsonBuilder();
        ObjectMapper mapper =new ObjectMapper();
        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);

        for(Entry taskName : ob.entrySet()){
            LinkedHashMap<String, LinkedHashMap> connections = (LinkedHashMap<String, LinkedHashMap>) taskName.getValue();
            for(Entry connection : connections.entrySet()) { // connection can have multipe qualifiers
                LinkedHashMap<String, Object> qualifers = (LinkedHashMap<String, Object>) connection.getValue();
                for(Entry qualifer : qualifers.entrySet()) {
                    ArrayList instructions = (ArrayList) qualifer.getValue();
                    for(Object instruction : instructions){
                        String params = builder.toJsonString(instruction);
                        Task task = mapper.readValue(params,  new TypeReference<Task>(){});
                        task.taskName(taskName.getKey().toString());
                        task.connection(connection.getKey().toString());
                        task.qualifier(qualifer.getKey().toString());
                        tasks.add(task);
                    }
                }
            }
        }
        return tasks;
    }

    public static void main(String[] args) throws ParseException {
        CommandLine results = ParseCLI(args);

        try {
            YamlParser parse_me = new YamlParser();
            Map<String, Object> connectionsYAML = parse_me.ReadYAML(results.getOptionValue("connections"));
            Map<String, Object> tasksYAML= parse_me.ReadYAML(results.getOptionValue("tasks"));

            Map<String, DbConn> connections = BuildConnections(connectionsYAML);
            List<Task> tasks = TestOutput(tasksYAML);

            // Execute
            for(Task task : tasks) {
                try{
                    System.out.println("Running: " + task.getTaskName());
                    DbConn myConnection = connections.get(task.getConnection());
                    Method method = myConnection.getClass().getMethod(task.getTaskName(), Task.class);
                    method.invoke(myConnection, task);
                } catch(Exception e){
                    System.out.println("Task failed " + task.getTaskName() + " failed, continuing");
                }
                
            }
        } catch(Exception e){
            System.out.println(e);
        }
        System.exit(1);
    }
}