package edu.mit.ll.graphulo.pig.backend;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.accumulo.AccumuloStorage;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

import java.io.*;
import java.util.Iterator;
import java.util.Properties;

import edu.mit.ll.d4m.db.cloud.D4mDbInsert;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;
import edu.mit.ll.d4m.db.cloud.D4mDbInfo;
import edu.mit.ll.graphulo.util.TripleFileWriter;
import edu.mit.ll.graphulo.Graphulo;


/**
 * Created by Lauren Milechin on 7/6/17.
 */
public class PutTriple extends EvalFunc<String> {

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

            //Read Accumulo Configuration Information and Table Names
            Tuple dbTable = (Tuple) input.get(0);
            String accConfigFile = (String) dbTable.get(0);
            String schema = (String) input.get(1);
            // input.get(2) is triples, either a filename or Tuples
            String mainTable = (String) dbTable.get(1);
            String transposeTable = (String) dbTable.get(2);
            Boolean deleteTables = false; // by default don't delete existing tables
            String delimiter = "\n";
            Boolean directed = false;
            String inputSchema = "adjacency";
            Boolean DegTableWeightedSum = false;
            if (input.size() > 3) {
                for (int i = 3; i < input.size() - 1; i+=2) {
                    if (input.get(i).equals("DeleteTables"))
                        deleteTables = (Boolean) input.get(i+1);
                    else if (input.get(i).equals("InputSchema"))
                        inputSchema = (String) input.get(i+1);
                    else if (input.get(i).equals("Directed"))
                        directed = (Boolean) input.get(i+1);
                    //else if (input.get(i).equals("DegTableWeightedSum")) // Add this option in the future
                    //    DegTableWeightedSum = (Boolean) input.get(i+1);
                }
            }


            Properties accProps = LocalFileUtil.parseConfiguration(accConfigFile);

            long numEntries = 0;

            Boolean hasPredefinedSchema = (schema.toLowerCase().equals("incidence") || schema.toLowerCase().equals("adjacency") || schema.toLowerCase().equals("single"));
            Boolean useTripleFileWriter = hasPredefinedSchema && inputSchema.equals("adjacency") && (directed || schema.equals("single"));


            if (useTripleFileWriter) {
                // Schema specified, using TripleFileWriter to auto-generate tables

                PrintWriter pwR = new PrintWriter("row.tmp");
                PrintWriter pwC = new PrintWriter("col.tmp");
                PrintWriter pwV = new PrintWriter("val.tmp");

                if (input.get(2) instanceof String) { //input is name of file containing the triples
                    String fname = (String) input.get(2);
                    BufferedReader br = new BufferedReader(new FileReader(fname));

                    String str = br.readLine();

                    while (str != null) {
                        if (str.indexOf('#') == -1) {

                            String[] arr = str.split("\t");

                            pwR.print(arr[0] + delimiter);
                            pwC.print(arr[1] + delimiter);
                            if (arr.length == 3)
                                pwV.print(arr[2] + delimiter);
                            else
                                pwV.print("1" + delimiter);

                            numEntries++;
                        }
                        str = br.readLine();
                    }
                    br.close();

                } else { // input is a Databag of triples
                    DataBag triples = (DataBag) input.get(2);
                    Iterator it = triples.iterator();

                    while (it.hasNext()) {
                        Tuple t = (Tuple) it.next();

                        pwR.print(t.get(0) + delimiter);
                        pwC.print(t.get(1) + delimiter);

                        String tmpVal = (String) t.get(2);
                        if (tmpVal != null && !tmpVal.isEmpty())// && !noVals)
                            pwV.print(t.get(2) + delimiter);
                        else
                            pwV.print("1" + delimiter);

                        numEntries++;
                    }

                }

                pwR.close();
                pwC.close();
                pwV.close();

                File rowFile = new File("row.tmp");
                File colFile = new File("col.tmp");
                File valFile = new File("val.tmp");

                TripleFileWriter tfw = new TripleFileWriter(LocalFileUtil.createAccumuloConnection(accConfigFile));

                if (schema.toLowerCase().equals("incidence")) {
                    if (mainTable.substring(mainTable.length() - 4, mainTable.length()).toLowerCase().equals("edge"))
                        mainTable = mainTable.substring(0, mainTable.length() - 4);
                    numEntries = tfw.writeTripleFile_Incidence(rowFile, colFile, valFile, delimiter, mainTable, deleteTables, false, -1);
                    if (DegTableWeightedSum) {
                        // Delete Degree table and re-create with weighted sum
                    }
                }
                else if (schema.toLowerCase().equals("adjacency"))
                    numEntries = tfw.writeTripleFile_Adjacency(rowFile, colFile, valFile, delimiter, mainTable, deleteTables, false);
                else if (schema.toLowerCase().equals("single")) {
                    if (mainTable.substring(mainTable.length() - 6, mainTable.length()).toLowerCase().equals("single"))
                        mainTable = mainTable.substring(0, mainTable.length() - 6);
                    numEntries = tfw.writeTripleFile_Single(rowFile, colFile, valFile, delimiter, mainTable, deleteTables, false);

                    if (DegTableWeightedSum) {
                        // Delete Degree table and re-create with weighted sum
                    }
                }
                // Delete temp files
                rowFile.delete();
                colFile.delete();
                valFile.delete();
            }
            else { // No schema specified, just make table of provided triples and transpose table if specified

                StringBuilder rowBuilder = new StringBuilder();
                StringBuilder colBuilder = new StringBuilder();
                StringBuilder valBuilder = new StringBuilder();

                if (input.get(2) instanceof String) { //is filename
                    String fname = (String) input.get(2);
                    BufferedReader br = new BufferedReader(new FileReader(fname));

                    String str = br.readLine();

                    while(str != null) {
                        if(str.indexOf('#') == -1) {

                            String[] arr = str.split("\t");


                                rowBuilder.append(arr[0] + delimiter);
                                colBuilder.append(arr[1] + delimiter);

                                if (arr.length == 3)
                                    valBuilder.append(arr[2] + delimiter);
                                else
                                    valBuilder.append("1" + delimiter);
                        }
                        str = br.readLine();
                        numEntries++;
                    }
                    br.close();

                } else {
                    DataBag triples = (DataBag) input.get(2);
                    Iterator it = triples.iterator();

                    while (it.hasNext()) {
                        Tuple t = (Tuple) it.next();

                            rowBuilder.append(t.get(0) + delimiter);
                            colBuilder.append(t.get(1) + delimiter);

                            String tmpVal = (String) t.get(2);
                            if (tmpVal != null && !tmpVal.isEmpty())// && !noVals)
                                valBuilder.append(t.get(2) + delimiter);
                            else
                                valBuilder.append("1" + delimiter);

                        numEntries++;
                    }
                }

                String row = rowBuilder.toString();
                String col = colBuilder.toString();
                String val = valBuilder.toString();

                if (deleteTables) {
                    D4mDbInfo dbInfo = new D4mDbInfo(accProps.getProperty("instance"), accProps.getProperty("zkServers"), accProps.getProperty("user"), accProps.getProperty("password"));
                    String tables = dbInfo.getTableList();
                    D4mDbTableOperations dbTableOps = new D4mDbTableOperations(accProps.getProperty("instance"), accProps.getProperty("zkServers"), accProps.getProperty("user"), accProps.getProperty("password"));
                    if (tables.indexOf(mainTable) >= 0) // if the table already exists
                        dbTableOps.deleteTable(mainTable);
                    if (transposeTable !=null && transposeTable.length() > 0 && tables.indexOf(transposeTable) >= 0)
                        dbTableOps.deleteTable(transposeTable);
                }


                D4mDbInsert dbInsert = new D4mDbInsert(accProps.getProperty("instance"), accProps.getProperty("zkServers"), mainTable, accProps.getProperty("user"), accProps.getProperty("password"));
                dbInsert.doProcessing(row, col, val, "", "");

                // Insert Transpose Table, if specified
                if (transposeTable !=null && transposeTable.length() > 0) {
                    D4mDbInsert dbInsertT = new D4mDbInsert(accProps.getProperty("instance"), accProps.getProperty("zkServers"), transposeTable, accProps.getProperty("user"), accProps.getProperty("password"));
                    dbInsertT.doProcessing(col, row, val, "", "");
                }

                // Insert Degree Table
                if (hasPredefinedSchema) {
                    Graphulo g = LocalFileUtil.createGraphuloConnection(accConfigFile);
                    if (schema.equals("adjacency"))
                        g.generateDegreeTable(mainTable, mainTable + "Deg", DegTableWeightedSum, "deg");
                    else // Incidence Schema is sum across cols of Transpose Table
                        g.generateDegreeTable(transposeTable, mainTable + "DegT", DegTableWeightedSum, "deg");
                }

            }

            return "Ingested " + Long.toString(numEntries) + " entries using " + schema + " schema.";

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }
}
