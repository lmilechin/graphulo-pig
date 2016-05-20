package edu.mit.ll.graphulo.pig;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

import edu.mit.ll.graphulo.*;
import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloConnection;

import org.apache.accumulo.core.client.security.tokens.*;
import org.apache.accumulo.core.client.*;

/**
 * Simple test UDF to connect to Accumulo and return the number of entries in the given table.
 * 
 * @author ti26350
 *
 */
public class testUDF extends EvalFunc<String> {
	
    public String exec(Tuple input) throws IOException {
    
    	try {
    		String instance = null;
	    	String zkServers = null;
	
	    	String principal = null;
	    	AuthenticationToken authToken = new PasswordToken("");
	
	    	ZooKeeperInstance inst = new ZooKeeperInstance(instance, zkServers);
	    	Connector conn = inst.getConnector(principal, authToken);
	    	
	//    	ConnectionProperties cp = new ConnectionProperties("txg-classdb04.cloud.llgrid.ll.mit.edu","AccumuloUser",
	//    			"onPKURqGF@zFRyEZDLsV460EH", "txg-classdb04",null);
	//    	AccumuloConnection ac = new AccumuloConnection(cp);
	//    	PasswordToken at = new PasswordToken("onPKURqGF@zFRyEZDLsV460EH");
	    	
	    	Graphulo g = new Graphulo(conn, authToken);
	    	
    	
	        if (input.size() == 0)
	            return null;
            String str = (String) input.get(0);
            return String.valueOf(g.countEntries(str));

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }

}