package utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class YamlParser {
    private String directory;

    public YamlParser() {
    }

    public YamlParser(String directory) {
        this.directory = directory;
    }

    /**
     * Read's a YAML based on the path using snakeYAML
     * 
     * @param path
     * @return Contents of YAML file
     */
    public Map<String, Object> ReadYAML(String path) {
        try {
            // Use this.getClass().getClassLoader().getResourceAsStream(path)
            // IFF you need to access something from the java/resouces/ of the JAR
            // Example:
            // InputStream inputStream =
            // this.getClass().getClassLoader().getResourceAsStream(path);
            Yaml yaml = new Yaml();
            InputStream inputStream = new FileInputStream(path);
            Map<String, Object> obj = yaml.load(inputStream);
            return obj;
        } catch (Exception ex) {
            System.out.println(ex);
            return null;
        }
    }

    /**
     * Reads an directory for all YAML files and then reads the contents of each
     * file.
     * 
     * @return
     */
    public Map<String, Map<String, Object>> ReadYAMLDriectory() {
        if (this.directory.isEmpty()) {
            throw new RuntimeException("The \"directory\" string should not be null, try recreating the YAML object");
        }
        Map<String, Map<String, Object>> yamls = new HashMap<String, Map<String, Object>>();
        try {
            FilenameFilter txtFileFilter = new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.endsWith(".yaml") || name.endsWith(".yml"))
                        return true;
                    else
                        return false;
                }
            };

            File directory = new File(this.directory);
            String[] files = directory.list(txtFileFilter);
            if (files != null)
                for (String file : files) {
                    yamls.put(file, ReadYAML(file));
                }

        } catch (Exception ex) {
            System.out.println(ex);
            return null;
        }
        return yamls;
    }

    @Override
    public String toString() {
        if (this.directory.isEmpty()) {
            return "Used paramless constructor";
        }
        return "Directory = " + this.directory;
    }
}