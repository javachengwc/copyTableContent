package ylutil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class PropertiesParser {
	private Properties properties = new Properties();
	
	public PropertiesParser(String propertiesFile) throws FileNotFoundException, IOException{
		properties.load(new FileInputStream(propertiesFile));
	}
	
	public String getProperty(String property){
		return properties.getProperty(property);
	}
}
