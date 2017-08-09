package edu.mit.ll.graphulo.pig.backend;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
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
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.ParseException;
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
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloConnection;
import edu.mit.ll.d4m.db.cloud.D4mDataSearch;
import edu.mit.ll.d4m.db.cloud.D4mDbResultSet;
import edu.mit.ll.graphulo.Graphulo;


import org.apache.pig.backend.hadoop.accumulo.AccumuloStorage;

/**
 * Created by Lauren on 7/6/17.
 */
public class D4mQuery extends EvalFunc<DataBag> {


        private static final Logger log = Logger.getLogger(AccumuloStorage.class);
        private static final String COLON = ":", EMPTY = "";
        private static final Text EMPTY_TEXT = new Text(new byte[0]);
        private static final DataByteArray EMPTY_DATA_BYTE_ARRAY = new DataByteArray(
                new byte[0]);

        public DataBag exec(Tuple input) throws IOException {

            try {

                // Sanity Check
                if (input.size() == 0 || input.size() == 0)
                    return null;

                //Read Accumulo Configuration Information and Table Names
                Tuple dbTable = (Tuple) input.get(0);
                String accConfigFile = (String) dbTable.get(0);
                String mainTable = (String) dbTable.get(1);
                String transposeTable = (String) dbTable.get(2);

                // Query row/col ranges
                String rowRange = (String) input.get(1);
                String colRange = (String) input.get(2);

                DataBag db = BagFactory.getInstance().newDefaultBag();
                TupleFactory tf = TupleFactory.getInstance();

                Properties accProps = LocalFileUtil.parseConfiguration(accConfigFile);

                D4mDataSearch ds = new D4mDataSearch(accProps.getProperty("instance"), accProps.getProperty("zkServers"), mainTable, accProps.getProperty("user"), accProps.getProperty("password"));

                if (rowRange.equals(":") && !colRange.equals(":") && transposeTable.length()>0) {
                    ds.setTableName(transposeTable);
                    D4mDbResultSet results = ds.doMatlabQuery(colRange, rowRange, "", "");
                } else {
                    D4mDbResultSet results = ds.doMatlabQuery(rowRange, colRange, "", "");
                }

                String tmpRowString = ds.getRowReturnString();

                String delimiter = tmpRowString.substring(tmpRowString.length()-1);

                String[] d4mCol;
                String[] d4mRow;

                if (ds.getTableName().equals(transposeTable)) {
                    d4mCol = ds.getRowReturnString().split(delimiter);
                    d4mRow = ds.getColumnReturnString().split(delimiter);
                } else {
                    d4mRow = ds.getRowReturnString().split(delimiter);
                    d4mCol = ds.getColumnReturnString().split(delimiter);
                }
                String[] d4mVal = ds.getValueReturnString().split(delimiter);

                for(int i=0; i<d4mRow.length; i++) {

                    String row = d4mRow[i];
                    String column = d4mCol[i];
                    String value = d4mVal[i];

                    Tuple t = tf.newTuple(3);
                    t.set(0,row); t.set(1,column); t.set(2, value);
                    db.add(t);
                }

                return db;

            } catch (Exception e) {
                // TODO: handle exception
                throw WrappedIOException.wrap(
                        "Caught exception processing input row ", e);
            }
        }

        public Schema outputSchema(Schema input) {
            try{
                Schema bagSchema = new Schema();
                bagSchema.add(new Schema.FieldSchema("row", DataType.CHARARRAY));
                bagSchema.add(new Schema.FieldSchema("col", DataType.CHARARRAY));
                bagSchema.add(new Schema.FieldSchema("val", DataType.INTEGER));
                return new Schema(new Schema.FieldSchema("result",bagSchema, DataType.TUPLE));
            }catch (Exception e){
                return null;
            }
        }
}
