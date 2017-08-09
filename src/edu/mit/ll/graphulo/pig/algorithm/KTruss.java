package edu.mit.ll.graphulo.pig.algorithm;

import edu.mit.ll.graphulo.pig.backend.LocalFileUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.pig.backend.LocalFileUtil;


/**
 * Created by Lauren on 7/11/17.
 */
public class KTruss  extends EvalFunc<Long> {

    public Long exec(Tuple input) throws IOException {

        try {

            // Sanity Check
            if (input.size() == 0 || input.size() == 0)
                return null;

            // TableMult configuration information

            Tuple dbTable           = (Tuple) input.get(0);
            String accConfigFile    = (String) dbTable.get(0);
            String mainTable        = (String) dbTable.get(1);
            String transposeTable   = (String) dbTable.get(2);
            String schema           = (String) input.get(1);
            int k 					= (Integer) input.get(2);
            String kTrussConfigFile    = null;
            if (input.size() == 4)
                kTrussConfigFile       = (String) input.get(3);

            // Parse Config File
            Properties kTrussProps = new Properties ();

            if (kTrussConfigFile != null) {
                try {
                    Path p = Paths.get(System.getProperty("user.dir"), kTrussConfigFile);
                    BufferedReader br = new BufferedReader(new FileReader(p.toFile()));

                    kTrussProps = new Properties();
                    kTrussProps.load(br);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            //Set up Accumulo/Graphulo Connection
            Graphulo g = LocalFileUtil.createGraphuloConnection(accConfigFile);

            Long result = null;

            //General Params
            String Rfinal=mainTable + "_kTruss";
            if (kTrussProps.getProperty("Rfinal") != null && kTrussProps.getProperty("Rfinal").length()>0
                    && !kTrussProps.getProperty("Rfinal").equals("null"))
                Rfinal = kTrussProps.getProperty("Rfinal");
            String filterRowCol = null;
            if (kTrussProps.getProperty("filterRowCol") != null && kTrussProps.getProperty("filterRowCol").length()>0
                    && !kTrussProps.getProperty("filterRowCol").equals("null"))
                filterRowCol = kTrussProps.getProperty("filterRowCol");
            Boolean forceDelete = true;
            Authorizations tableAuths = null;
            String RNewVisibility = null;
            int maxiter = Integer.MAX_VALUE;
            if (kTrussProps.getProperty("maxiter") != null && kTrussProps.getProperty("maxiter").length()>0
                    && !kTrussProps.getProperty("maxiter").equals("Integer.MAX_VALUE"))
                maxiter = Integer.parseInt(kTrussProps.getProperty("maxiter"));

            //Incidence Only Params
            String RTfinal=mainTable + "_kTrussT";
            if (kTrussProps.getProperty("RTfinal") != null && kTrussProps.getProperty("RTfinal").length()>0
                    && !kTrussProps.getProperty("RTfinal").equals("null"))
                RTfinal = kTrussProps.getProperty("RTfinal");
            String edgeFilter = null;
            if (kTrussProps.getProperty("edgeFilter") != null && kTrussProps.getProperty("edgeFilter").length()>0
                    && !kTrussProps.getProperty("edgeFilter").equals("null"))
                edgeFilter = kTrussProps.getProperty("edgeFilter");


            if (schema.equals("adjacency"))
                result = g.kTrussAdj(mainTable, Rfinal, k, filterRowCol, forceDelete, tableAuths, RNewVisibility, maxiter);
            else if (schema.equals("incidence"))
                result = g.kTrussEdge(mainTable, transposeTable, Rfinal, RTfinal, k, edgeFilter, forceDelete, tableAuths);

            return result;

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }


}
