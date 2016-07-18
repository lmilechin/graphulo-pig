package edu.mit.ll.graphulo.pig.backend;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import edu.mit.ll.graphulo.Graphulo;

public class LocalFileUtil {

    /**
     * Given the name of a configuration file for Accumulo, reads information about the user, password and cluster setup.
     * 
     * @param accConfigFile Local name of the configuration file
     * @return java.util.Properties class
     * @throws IOException
     * @throws AccumuloSecurityException 
     * @throws AccumuloException 
     */
    public static Graphulo createGraphuloConnection(String accConfigFile) throws IOException, AccumuloException, AccumuloSecurityException {
//    		Configuration conf = new Configuration();
//            FileSystem fs = FileSystem.get(conf);
//            Path inFile = new Path(fs.getWorkingDirectory() + "/" + accConfigFile);
//            FSDataInputStream in = fs.open(inFile);
    		Path p = Paths.get(System.getProperty("user.dir"), accConfigFile);
    		BufferedReader br = new BufferedReader(new FileReader(p.toFile()));
            Properties accProps = new Properties();
            accProps.load(br);
            
    		// Set up Accumulo connection parameters
	    	AuthenticationToken authToken = new PasswordToken(accProps.getProperty("password"));
	    	ZooKeeperInstance inst = new ZooKeeperInstance(accProps.getProperty("instance"), accProps.getProperty("zkServers"));
	    	Connector conn = inst.getConnector(accProps.getProperty("user"), authToken);
	    	
	    	// Connct using Graphulo Interface
	    	return new Graphulo(conn, authToken);
	}
	
}
