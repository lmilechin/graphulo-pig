package edu.mit.ll.graphulo.pig.backend;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import edu.mit.ll.graphulo.Graphulo;

/**
 * A collection of file utilities that read configuration files from the local file system (NOT HDFS). Used for initial development and prototyping.
 * 
 * @author ti26350
 *
 */
public class LocalFileUtil {

    /**
     * Creates an instantiated {@link edu.mit.ll.graphulo.Graphulo} object given the name of a
     *  local configuration file for Accumulo that contains the following server information:
     *
     * <ul>
     *   <li><b>instance:</b> name of the Accumulo instance</li>
     *   <li><b>user:</b> user name for the Accumulo</li>
     *   <li><b>password:</b> password for the user</li>
     *   <li><b>zkServers:</b> list of Zookeeper servers</li>
     * </ul>
     * 
     * @param accumuloConfigFileName Local name of the file containing the Accumulo properties
     * @return Instantiated {@link edu.mit.ll.graphulo.Graphulo} class
     */
    public static Graphulo createGraphuloConnection(String accumuloConfigFileName) {

    	Graphulo g = null;
    	
    	try {
			//Read Accumulo properties from file
	        Properties accProps = parseConfiguration(accumuloConfigFileName);
	        
			// Set up Accumulo connection parameters
	    	AuthenticationToken authToken = new PasswordToken(accProps.getProperty("password"));
	    	ZooKeeperInstance inst = new ZooKeeperInstance(accProps.getProperty("instance"), accProps.getProperty("zkServers"));
	    	Connector conn = inst.getConnector(accProps.getProperty("user"), authToken);

	    	// Connect using Graphulo Interface
			g = new Graphulo(conn, authToken);
    	} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		}
    	
    	return g;
	}
    
    /**
     * Creates a connection to the Accumulo database given a file name that contains the following server information:
     * 
     * <ul>
     *   <li><b>instance:</b> name of the Accumulo instance</li>
     *   <li><b>user:</b> user name for the Accumulo</li>
     *   <li><b>password:</b> password for the user</li>
     *   <li><b>zkServers:</b> list of Zookeeper servers</li>
     * </ul>
     * 
     * @param accumuloConfigFileName Local name of the file containing the Accumulo properties
     * @return Instantiated {@link org.apache.accumulo.core.client.Connector} class for Accumulo
     */
    public static Connector createAccumuloConnection(String accumuloConfigFileName) 
    {
    	Connector conn = null;
		try {

			//Read Accumulo properties from file
	        Properties accProps = parseConfiguration(accumuloConfigFileName);
	        
			// Set up Accumulo connection parameters
	    	AuthenticationToken authToken = new PasswordToken(accProps.getProperty("password"));
	    	ZooKeeperInstance inst = new ZooKeeperInstance(accProps.getProperty("instance"), accProps.getProperty("zkServers"));
			conn = inst.getConnector(accProps.getProperty("user"), authToken);
		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		}
    	
    	return conn;
    }//end: createConnection(String)

    /**
     * Given the name of a property file, this creates a {@link Properties} object
     * 
     * @param propertyFileName Name of the property file
     * @return Instantiated {@link java.util.Properties} object containing
     */
	public static Properties parseConfiguration(String propertyFileName) 
	{
		Properties props = null;
		
        try {
			Path p = Paths.get(System.getProperty("user.dir"), propertyFileName);
			BufferedReader br = new BufferedReader(new FileReader(p.toFile()));
	        
			props = new Properties();
			props.load(br);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
		return props;
	}//end: parseConfiguration(String)
	
	/**
	 * 
	 * @param inputFile Name of the input file
	 * @param config Map containing the given configuration parameters
	 */
	public static void writeTripleFiles(String inputFile, Map<String,Object> config) {
		
		BufferedReader br;
		boolean values = false;
		
		if(config.get("containsValues") != null) {
			String containsValues = (String) config.get("containsValues");
			values = containsValues.toLowerCase().equals("true");
		}
		
		try {
			br = new BufferedReader(new FileReader(inputFile));
		
			PrintWriter pwV = new PrintWriter("verts.tmp");
			PrintWriter pwE = new PrintWriter("edges.tmp");
			
			String str = br.readLine();
			boolean comma = false;
			while(str != null) {
				if(str.indexOf('#') == -1) {
					if(comma) {
						pwV.print(",");
						pwE.print(",");
					}
					else {
						comma = true;
					}
	
					String[] arr = str.split("\t");
					pwV.print(arr[0]);
					pwE.print(arr[1]);
				}
				str = br.readLine();
			}
			br.close();
			pwV.close();
			pwE.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}

//HDFS Code
//Configuration conf = new Configuration();
//FileSystem fs = FileSystem.get(conf);
//Path inFile = new Path(fs.getWorkingDirectory() + "/" + accConfigFile);
//FSDataInputStream in = fs.open(inFile);