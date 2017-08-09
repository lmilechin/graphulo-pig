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

/**
 * Created by Lauren on 7/6/17.
 */
public class DbTableBinder extends EvalFunc<Tuple> {

    private static final Logger log = Logger.getLogger(AccumuloStorage.class);
    private static final String COLON = ":", EMPTY = "";
    private static final Text EMPTY_TEXT = new Text(new byte[0]);
    private static final DataByteArray EMPTY_DATA_BYTE_ARRAY = new DataByteArray(
            new byte[0]);

    public Tuple exec(Tuple input) throws IOException {

        try {

            // Sanity Check
            if (input.size() == 0 || input.size() == 0)
                return null;

            //Read Accumulo Configuration Information
            String accConfigFile = (String) input.get(0);

            // Get table name(s) and info
            String mainTable = (String) input.get(1);
            String transposeTable = null;
            if(input.size() == 3)
                transposeTable = (String) input.get(2);

            TupleFactory tf = TupleFactory.getInstance();
            Tuple t = tf.newTuple(3);
            t.set(0,accConfigFile);
            t.set(1,mainTable);
            if (transposeTable != null)
                t.set(2,transposeTable);

            return t;

        } catch (Exception e) {
            // TODO: handle exception
            throw WrappedIOException.wrap(
                    "Caught exception processing input row ", e);
        }
    }

    public Schema outputSchema(Schema input) {
        try{
            Schema schema = new Schema();
            schema.add(new Schema.FieldSchema("accConfig", DataType.CHARARRAY));
            schema.add(new Schema.FieldSchema("mainTable", DataType.CHARARRAY));
            schema.add(new Schema.FieldSchema("transposeTable", DataType.CHARARRAY));
            return new Schema(new Schema.FieldSchema("dbTable",schema, DataType.TUPLE));
        }catch (Exception e){
            return null;
        }
    }
}
