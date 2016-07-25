package edu.mit.ll.graphulo.pig.backend;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.accumulo.AccumuloStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.pig.backend.LocalFileUtil;
import edu.mit.ll.graphulo.util.TripleFileWriter;


/**
 * A class to execute Breadth-First Search on Graphulo graphs.
 * 
 *
 */
public class InsertAdjacencyGraph extends EvalFunc<String> {

    /**
     * Executes the BFS command, given the input parameters.
     * 
     * @param AccConfigFile			Name of file containing Accumulo configuration information
     * @param GraphTable 		    Name of Accumulo table containing the graph to be searched.
     * @param v0          			Starting nodes, like "a,f,b,c,". Null or empty string "" means start from all nodes.
     *                    			v0 may be a range of nodes like "c,:,e,g,k,:,".
     * @param k           			Number of steps
     * @param ResultTable			Name of table to store result. Null means don't store the result.
     * @param BFSConfigFile			Name of file containing additional BFS configuration information. 
     */
    public String exec(Tuple input) throws IOException {
        
    	try {
    		
    		// Sanity Check
	    	if (input.size() == 0 || input.size() == 0)
	            return null;
	    	
	    	// TableMult configuration information
            String accumuloConfiguration 	= (String) input.get(0);
            String inputFile 				= (String) input.get(1);
            String graphName 				= (String) input.get(2);
            String insertConfiguration		= (String) input.get(3);

            Map<String,Object> configuration = LocalFileUtil.parseConfiguration(insertConfiguration);
            
    		BufferedReader br = new BufferedReader(new FileReader(inputFile));
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
            
        	// Set up files
        	File verts = new File("verts.tmp");
        	File edges = new File("edges.tmp");
        	File vals = null;
    		
	    	TripleFileWriter tfw = new TripleFileWriter(LocalFileUtil.createConnection(accumuloConfiguration));
	    	long l = tfw.writeTripleFile_Adjacency(verts, edges, vals, ",", graphName, true, false);
	    	
	    	return Long.toString(l);

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }    
    
}
