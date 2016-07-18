package edu.mit.ll.graphulo.pig.algorithm;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.accumulo.AccumuloStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.pig.backend.LocalFileUtil;


/**
 * A class to execute Breadth-First Search on Graphulo graphs.
 * 
 *
 */
public class BFS extends EvalFunc<String> {

    private static final Logger log = Logger.getLogger(AccumuloStorage.class);
    private static final String COLON = ":", EMPTY = "";
    private static final Text EMPTY_TEXT = new Text(new byte[0]);
    private static final DataByteArray EMPTY_DATA_BYTE_ARRAY = new DataByteArray(
            new byte[0]);

    /**
     * Executes the BFS command, given the input parameters.
     * 
     * @param AccConfigFile			Name of file containing Accumulo configuration information
     * @param GraphTable 		    Name of Accumulo table containing the graph to be searched.
     * @param v0          			Starting nodes, like "a,f,b,c,". Null or empty string "" means start from all nodes.
     *                    			v0 may be a range of nodes like "c,:,e,g,k,:,".
     * @param k           			Number of steps
     * @param ResultTable			Name of table to store result. Null means don't store the result.
     * @param BFSConfigFile			Name of file containing additional BFS configuration information
     */
    public String exec(Tuple input) throws IOException {
        
    	try {
    		
    		// Sanity Check
	    	if (input.size() == 0 || input.size() == 0)
	            return null;
	    	
	    	// TableMult configuration information
            String accConfigFile 	= (String) input.get(0);
            String graphTable 		= (String) input.get(1);
            String v0 				= (String) input.get(2);
            int k 					= (Integer) input.get(3);
            String Rtable			= (String) input.get(4);
            String RtableTranspose 	= (String) input.get(5);

	    	//Set up Accumulo/Graphulo Connection
            Graphulo g = LocalFileUtil.createGraphuloConnection(accConfigFile);
            
	    	String s = g.AdjBFS(graphTable, v0, k, Rtable, RtableTranspose, null, null, false, 0, Integer.MAX_VALUE);
            return s;

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }    
}
