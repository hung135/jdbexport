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

import dto.DBConfigDto;
import objects.DbConn;
import objects.Task;
import utils.JLogger;
import utils.YamlParser;

public class DbDiff {

    public static CommandLine ParseCLI(String[] args) throws ParseException {
        Options cliOptions = new Options();
        Option connections = new Option("y", "connections", true, "Path to connections file");
        Option tasks = new Option("t", "tasks", true, "Path to tasks file");
        Option debugLevel = new Option("d", "debug", true, "Debug level use: [all, debug, default, warning]");

        connections.setRequired(true);
        tasks.setRequired(true);
        debugLevel.setRequired(false);

        cliOptions.addOption(connections);
        cliOptions.addOption(tasks);
        cliOptions.addOption(debugLevel);

        if (args.length <= 2) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Database diff tool", cliOptions);
            System.exit(1);
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(cliOptions, args);

        return cmd;
    }

    public static Map<String, DbConn> BuildConnections(Map<String, Object> cons, JLogger logger) throws JsonParseException,
            JsonMappingException, IOException, ClassNotFoundException, SQLException, PropertyVetoException {
        Map<String, DbConn> dbConfigs = new HashMap<String, DbConn>();
        JsonBuilder builder = new JsonBuilder();
        ObjectMapper mapper =new ObjectMapper();

        for(String key : cons.keySet()){
            String jsonString = builder.toJsonString(cons.get(key));
            DBConfigDto dto = mapper.readValue(jsonString,  new TypeReference<DBConfigDto>(){});
            DbConn connectionObject = new DbConn(dto, logger);
            
            dbConfigs.put(key, connectionObject);
        }
        return dbConfigs;
    }

    public static List<Task> BuildTasks(Map<String, Object> ob) 
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

    public static JLogger BuildLogger(String identifier) throws IOException {
        try {
            return new JLogger(identifier);
        } catch(Exception e){
            System.out.println(String.format("Couldn't build logger with %s -- using default configuration", identifier));
            return new JLogger("default");
        }
    }

    public static void main(String[] args) throws ParseException, IOException {
        CommandLine results = ParseCLI(args);
        JLogger logger = BuildLogger(results.hasOption("debug") ? results.getOptionValue("debug") : "default");

        try {
            YamlParser parse_me = new YamlParser();
            Map<String, Object> connectionsYAML = parse_me.ReadYAML(results.getOptionValue("connections"));
            Map<String, Object> tasksYAML= parse_me.ReadYAML(results.getOptionValue("tasks"));

            Map<String, DbConn> connections = BuildConnections(connectionsYAML, logger);
            List<Task> tasks = BuildTasks(tasksYAML);

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