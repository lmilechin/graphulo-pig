package edu.mit.ll.graphulo.pig.backend;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

import edu.mit.ll.graphulo.pig.backend.LocalFileUtil;
import edu.mit.ll.graphulo.util.TripleFileWriter;


/**
 * A class to create a Graphulo graph in Accumulo.
 * <p>
 * File is expected to have the form: 
 *	
 */
public class InsertIncidenceGraph extends EvalFunc<String> {

    /**
     * Inserts an Adjacency graph into Accumulo.
     * 
     * @param input Tuple containing three or four pieces of information required to insert data into Graphulo
     * 
     * <ol>
     * <li> <b>AccumuloConfiguration:</b> Name of the file containing Accumulo configuration information
     * <li> <b>inputFile:</b> File that contains the graph data
     * <li> <b>graphName:</b> Name of the graph as it will be stored in Graphulo
     * <li> <b>insertConfiguration:</b> Name of the file containing any additional insertion configuration information
     * </ol>
     */
    public String exec(Tuple input) throws IOException {
        
    	try {
    		
    		// Sanity Check
	    	if (input.size() < 3 || input.size() > 4)
	            return null;
	    	
	    	// TableMult configuration information
            String accumuloConfiguration 	= (String) input.get(0);
            String inputFile 				= (String) input.get(1);
            String graphName 				= (String) input.get(2);
            String insertConfiguration 		= null;
            
            if(input.size() == 4) {
	            insertConfiguration		= (String) input.get(3);
	            Properties configuration = LocalFileUtil.parseConfiguration(insertConfiguration);
            }
            
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
    		
	    	TripleFileWriter tfw = new TripleFileWriter(LocalFileUtil.createAccumuloConnection(accumuloConfiguration));
	    	long l = tfw.writeTripleFile_Incidence(verts, edges, vals, ",", graphName, true, false, 0);
	    	
	    	return Long.toString(l);

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }    
    
}
