import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import Utils.YamlParser;

public class DbDiff {

    public static CommandLine ParseCLI(String[] args) throws ParseException {
        Options cliOptions = new Options();
        Option connections = new Option("y", "connections", true, "Path to connections file");
        Option tasks = new Option("t", "tasks", true, "Path to tasks file");
        connections.setRequired(true);
        tasks.setRequired(true);

        cliOptions.addOption(connections);

        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Database diff tool", cliOptions);
            System.exit(1);
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(cliOptions, args);

        return cmd;
    }

    public static void BuildDtos(Map<String, Object> cons)
            throws JsonParseException, JsonMappingException, IOException {
        Map<String, DBConfigDto> dtos = new HashMap<String, DBConfigDto>();
        //mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

        for(String key : cons.keySet()){
            JsonBuilder builder = new JsonBuilder();
            ObjectMapper mapper =new ObjectMapper();

            String jsonString = builder.toJsonString(cons.get(key));
            DBConfigDto obj = mapper.readValue(jsonString,  new TypeReference<DBConfigDto>(){});
            
            dtos.put(key, obj);
        }
        // return dtos;
    }

    public static void main(String[] args) throws ParseException {
        CommandLine results = ParseCLI(args);

        // Build up objects here
        try {
            //CommandLine  cmd = parser.parse(options, args);
            YamlParser parse_me = new YamlParser();
            Map<String, Object> connections = parse_me.ReadYAML(results.getOptionValue("connections"));
            
            Map<String, Object> tasks = parse_me.ReadYAML(results.getOptionValue("tasks"));

            BuildDtos(connections);
        } catch(Exception e){
            e.printStackTrace();
            System.out.println("broke");
        }
        // /** Should be able to use this example straing from Yaml file for dbtype */
        // //Enum dbtype = Enum.valueOf(DbConn.DbType.class, "SYBASE");
        // // This should be all you need to get a jdbc connection
        // try {
        //   //  DbConn sybaseConn = new DbConn(DbConn.DbType.SYBASE, "sa", sybasePassword, "dbsybase", "5000", "master");
        //     //DbConn oracleConn = new DbConn(DbConn.DbType.ORACLE, "system", "Docker12345", "dboracle", "1542", "dev");
        // } catch (Exception e) {
        //     System.out.println(e);

        // }
        // ;
        // main.readyaml();
    }
}