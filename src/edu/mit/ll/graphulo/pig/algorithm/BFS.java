package edu.mit.ll.graphulo.pig.algorithm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.Text;
import org.apache.http.auth.AUTH;
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

    /**
     * Executes the BFS command, given the input parameters.
     * 
     * @param Tuple
     * <ol>
     *   <li>AccConfigFile			Name of file containing Accumulo configuration information</li>
     *   <li> GraphTable 		    Name of Accumulo table containing the graph to be searched.</li>
     *   <li> v0          			Starting nodes, like "a,f,b,c,". Null or empty string "" means start from all nodes.
     *                    			v0 may be a range of nodes like "c,:,e,g,k,:,".</li>
     *   <li> k           			Number of steps</li>
     *   <li> ResultTable			Name of table to store result. Null means don't store the result.</li>
     *   <li> BFSConfigFile			Name of file containing additional BFS configuration information.</li>
     * </ol>
     */
    public String exec(Tuple input) throws IOException {
        
    	try {
    		
    		// Sanity Check
	    	if (input.size() == 0 || input.size() == 0)
	            return null;
	    	
	    	// TableMult configuration information

            Tuple dbTable           = (Tuple) input.get(0);
            String accConfigFile    = (String) dbTable.get(0);
            String mainTable        = (String) dbTable.get(1);
            String schema           = (String) input.get(1);
            String v0 				= (String) input.get(2);
            int k 					= (Integer) input.get(3);
            String BFSConfigFile    = null;
            if (input.size() == 5)
                BFSConfigFile       = (String) input.get(4);

            // Parse Config File
            Properties bfsProps = new Properties ();

            if (BFSConfigFile != null) {
                try {
                    Path p = Paths.get(System.getProperty("user.dir"), BFSConfigFile);
                    BufferedReader br = new BufferedReader(new FileReader(p.toFile()));

                    bfsProps = new Properties();
                    bfsProps.load(br);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
	    	//Set up Accumulo/Graphulo Connection
            Graphulo g = LocalFileUtil.createGraphuloConnection(accConfigFile);

            String result = null;

            //General Params
            String Rtable=mainTable + "_BFS";
            if (bfsProps.getProperty("Rtable") != null && bfsProps.getProperty("Rtable").length()>0
                    && !bfsProps.getProperty("Rtable").equals("null"))
                Rtable = bfsProps.getProperty("Rtable");
            String RTtable=null;
            if (bfsProps.getProperty("RTtable") != null && bfsProps.getProperty("RTtable").length()>0
                    && !bfsProps.getProperty("RTtable").equals("null"))
                RTtable = bfsProps.getProperty("RTtable");
            String degTable=null;
            if (bfsProps.getProperty("degTable") != null && bfsProps.getProperty("degTable").length()>0 && !bfsProps.getProperty("degTable").equals("null"))
                degTable = bfsProps.getProperty("degTable");
            String degColumn="out";
            if (schema.equals("single"))
                degColumn="deg";
            if (bfsProps.getProperty("degColumn") != null && bfsProps.getProperty("degColumn").length()>0 && !bfsProps.getProperty("degColumn").equals("out"))
                degColumn = bfsProps.getProperty("degColumn");
            Boolean degInColQ=false;
            if (bfsProps.getProperty("degInColQ") != null && bfsProps.getProperty("degInColQ").length()>0 && bfsProps.getProperty("degInColQ").equals("true"))
                degInColQ=true;
            Integer minDegree=0;
            if (bfsProps.getProperty("minDegree") != null && bfsProps.getProperty("minDegree").length()>0 && !bfsProps.getProperty("minDegree").equals("0"))
                minDegree=Integer.parseInt(bfsProps.getProperty("minDegree"));
            Integer maxDegree=Integer.MAX_VALUE;
            if (bfsProps.getProperty("maxDegree") != null && bfsProps.getProperty("maxDegree").length()>0 && !bfsProps.getProperty("minDegree").equals("Integer.MAX_VALUE"))
                maxDegree=Integer.parseInt(bfsProps.getProperty("maxDegree"));

            //Incidence Params
            String startPrefixes="out|,";
            if (schema.equals("single"))
                startPrefixes = ",";
            if (bfsProps.getProperty("startPrefixes") != null && bfsProps.getProperty("startPrefixes").length()>0)
                startPrefixes = bfsProps.getProperty("startPrefixes");
            String endPrefixes="in|,";
            if (schema.equals("single"))
                endPrefixes = ",";
            if (bfsProps.getProperty("endPrefixes") != null && bfsProps.getProperty("endPrefixes").length()>0)
                endPrefixes = bfsProps.getProperty("endPrefixes");
            IteratorSetting plusOp=null;
            Integer EScanIteratorPriority=-1;
            Authorizations Eauthorizations=null;
            Authorizations EDegauthorizations=null;
            String newVisibility=null;
            boolean useNewTimestamp=true;
            boolean outputUnion=false;
            if (bfsProps.getProperty("outputUnion") != null && bfsProps.getProperty("outputUnion").length()>0 && bfsProps.getProperty("outputUnion").equals("true"))
                outputUnion = true;
            MutableLong numEntriesWritten=null;

            //Single Params
            String edgeColumn="edge";
            if (bfsProps.getProperty("edgeColumn") != null && bfsProps.getProperty("edgeColumn").length()>0)
                edgeColumn = bfsProps.getProperty("edgeColumn");
            Character edgeSep='|';
            if (bfsProps.getProperty("edgeSep") != null && bfsProps.getProperty("edgeSep").length()>0)
                edgeSep = bfsProps.getProperty("edgeSep").charAt(0);
            String SDegtable=null;
            if (bfsProps.getProperty("SDegtable") != null && bfsProps.getProperty("SDegtable").length()>0 && !bfsProps.getProperty("SDegtable").equals("null"))
                SDegtable = bfsProps.getProperty("SDegtable");
            boolean copyOutDegrees=false;
            if (bfsProps.getProperty("copyOutDegrees") != null && bfsProps.getProperty("copyOutDegrees").length()>0 && bfsProps.getProperty("copyOutDegrees").equals("false"))
                copyOutDegrees = true;
            boolean computeInDegrees=false;
            if (bfsProps.getProperty("computeInDegrees") != null && bfsProps.getProperty("computeInDegrees").length()>0 && bfsProps.getProperty("computeInDegrees").equals("false"))
                computeInDegrees = true;
            ColumnVisibility singleNewVisibility = null;
            Authorizations Sauthorizations=null;


            if (schema.equals("adjacency"))
                result = g.AdjBFS(mainTable, v0, k, Rtable, RTtable, degTable, degColumn, degInColQ, minDegree, maxDegree);
            else if (schema.equals("incidence"))
                result = g.EdgeBFS(mainTable, v0, k, Rtable, RTtable, startPrefixes, endPrefixes, degTable, degColumn, degInColQ,
                        minDegree, maxDegree, plusOp, EScanIteratorPriority, Eauthorizations, EDegauthorizations,
                        newVisibility, useNewTimestamp, outputUnion, numEntriesWritten);
            else if (schema.equals("single"))
                result = g.SingleBFS(mainTable, edgeColumn, edgeSep, v0, k, Rtable, SDegtable, degColumn, copyOutDegrees,
                        computeInDegrees, null, singleNewVisibility, minDegree, maxDegree, plusOp, outputUnion,
                        Sauthorizations, numEntriesWritten);

            return result;

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }

}
