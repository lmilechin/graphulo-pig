package edu.mit.ll.graphulo.pig.backend;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.accumulo.AccumuloStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.*;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;
import edu.mit.ll.d4m.db.cloud.D4mDbInfo;

/**
 * Created by Lauren on 7/7/17.
 */
public class DeleteTable extends EvalFunc<String> {

    private static final Logger log = Logger.getLogger(AccumuloStorage.class);
    private static final String COLON = ":", EMPTY = "";
    private static final Text EMPTY_TEXT = new Text(new byte[0]);
    private static final DataByteArray EMPTY_DATA_BYTE_ARRAY = new DataByteArray(
            new byte[0]);

    public String exec(Tuple input) throws IOException {

        try {

            // Sanity Check
            if (input.size() == 0 || input.size() == 0)
                return null;

            //Read Accumulo Configuration Information

            String accConfigFile;
            ArrayList<String> tablesToDelete = new ArrayList<String>();

            if (input.get(0) instanceof Tuple) {
                Tuple dbTable = (Tuple) input.get(0);
                accConfigFile = (String) dbTable.get(0);
                tablesToDelete.add((String) dbTable.get(1));
                tablesToDelete.add((String) dbTable.get(2));
            } else
                accConfigFile = (String) input.get(0);

            for (int i = 1; i<input.size(); i++)
                tablesToDelete.add((String) input.get(i));


            Properties accProps = LocalFileUtil.parseConfiguration(accConfigFile);
            StringBuilder output = new StringBuilder();
            D4mDbTableOperations dbTableOps = new D4mDbTableOperations(accProps.getProperty("instance"), accProps.getProperty("zkServers"), accProps.getProperty("user"), accProps.getProperty("password"));
            D4mDbInfo dbInfo = new D4mDbInfo(accProps.getProperty("instance"), accProps.getProperty("zkServers"), accProps.getProperty("user"), accProps.getProperty("password"));
            String tables = dbInfo.getTableList();
            for (String table : tablesToDelete) {
                if (!(table == null || table.isEmpty()) && tables.indexOf(table) >= 0) { // Check if table exists before deleting
                    dbTableOps.deleteTable(table);
                    output.append(table + ", ");
                }
            }

            String outputString = output.toString();
            return "Deleted: " + outputString.substring(0,outputString.length() - 2);

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }


}
