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
public class Jaccard extends EvalFunc<Double> {

    private static final Logger log = Logger.getLogger(AccumuloStorage.class);
    private static final String COLON = ":", EMPTY = "";
    private static final Text EMPTY_TEXT = new Text(new byte[0]);
    private static final DataByteArray EMPTY_DATA_BYTE_ARRAY = new DataByteArray(
            new byte[0]);

    /**
     * Executes the BFS command, given the input parameters.
     * 
     * @param Tuple Input tuple
     * <ol>
     *   <li> AccConfigFile			Name of file containing Accumulo configuration information</li>
     *   <li> GraphTable 		    Name of Accumulo table containing the graph to be searched.</li>
     *   <li> ResultTable			Name of table to store result. Null means don't store the result.</li>
     * </ol>
     */
    public Double exec(Tuple input) throws IOException {
        
    	try {
    		
    		// Sanity Check
	    	if (input.size() == 0 || input.size() == 0)
	            return -1.0;
	    	
	    	// TableMult configuration information
            String accConfigFile 	= (String) input.get(0);
            String graphTable 		= (String) input.get(1);
            String Rtable			= (String) input.get(4);

	    	//Set up Accumulo/Graphulo Connection
            Graphulo g = LocalFileUtil.createGraphuloConnection(accConfigFile);
            
	    	double s = g.Jaccard(graphTable, graphTable+"Deg", Rtable, null, null, null);
            return s;

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }    
}
