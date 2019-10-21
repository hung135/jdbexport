package utils;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class JLogger {
    public Logger logger;

    public JLogger(String identifier) throws IOException {
        this.logger = Logger.getLogger(identifier);

        Properties props = new Properties();
        props.load(getClass().getResourceAsStream(String.format("/logging-configs/%s", identifier)));

        PropertyConfigurator.configure(props);
    }
}