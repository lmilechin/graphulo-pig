package edu.mit.ll.graphulo.pig;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.WrappedIOException;

import edu.mit.ll.graphulo.Graphulo;

import org.apache.pig.backend.hadoop.accumulo.AccumuloStorage;


/**
 * 
 * @author ti26350
 *
 */
public class TableMult extends EvalFunc<String> {

    private static final Logger log = Logger.getLogger(AccumuloStorage.class);
    private static final String COLON = ":", EMPTY = "";
    private static final Text EMPTY_TEXT = new Text(new byte[0]);
    private static final DataByteArray EMPTY_DATA_BYTE_ARRAY = new DataByteArray(
            new byte[0]);

    /**
     * Executes the given TableMult command given the two tables
     */
    public String exec(Tuple input) throws IOException {
        
    	try {
    		
    		// Sanity Check
	    	if (input.size() == 0 || input.size() == 0)
	            return null;
	    	
	    	//Read Accumulo Configuration Information
            String accConfigFile = (String) input.get(0);
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path inFile = new Path(fs.getWorkingDirectory() + "/" + accConfigFile);
            FSDataInputStream in = fs.open(inFile);
            Properties accProps = new Properties();
            accProps.load(in);

	    	// TableMult configuration information
            String ATtable = (String) input.get(1);
            String Btable = (String) input.get(2);
            String Ctable = (String) input.get(3);

	    	//Read TableMult Configuration Information
            String tableMultOptsFile = (String) input.get(4);
            accConfigFile = (String) input.get(0);
            conf = new Configuration();
            fs = FileSystem.get(conf);
            inFile = new Path(fs.getWorkingDirectory() + "/" + accConfigFile);
            in = fs.open(inFile);
            Properties tmProps = new Properties();
            tmProps.load(in);
            
    		// Set up Accumulo connection parameters
	    	AuthenticationToken authToken = new PasswordToken(accProps.getProperty("password"));
	    	ZooKeeperInstance inst = new ZooKeeperInstance(accProps.getProperty("instance"), accProps.getProperty("zkServers"));
	    	Connector conn = inst.getConnector(accProps.getProperty("user"), authToken);
	    	
	    	// Connect using Graphulo Interface
	    	Graphulo g = new Graphulo(conn, authToken);

	    	// Execute TableMult
	    	long pp = g.TableMult(ATtable, Btable, Ctable, Ctable+"T", null, null);
/*	    	long pp = g.TableMult(ATtable, Btable, Ctable, Ctable+"T",
	    			BScanIteratorPriority, multOp, multOpOptions, plusOp, 
	    			rowFilter, colFilterAT, colFilterB, 
	    			alsoDoAA, alsoDoBB, 
	    			iteratorsBeforeA, iteratorsBeforeB, iteratorsAfterTwoTable, 
	    			reducer, reducerOpts, numEntriesCheckpoint, 
	    			ATauthorizations, Bauthorizations);*/
//	    	long pp = 0;
	    	System.out.println(pp);
	    	// Return number of partial products
            return String.valueOf(pp);

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }    
}
