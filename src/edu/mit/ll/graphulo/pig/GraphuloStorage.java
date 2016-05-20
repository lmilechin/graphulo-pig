package edu.mit.ll.graphulo.pig;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.pig.backend.hadoop.accumulo.AccumuloStorage;


public class GraphuloStorage extends AccumuloStorage {

    private static final Logger log = Logger.getLogger(AccumuloStorage.class);
    private static final String COLON = ":", EMPTY = "";
    private static final Text EMPTY_TEXT = new Text(new byte[0]);
    private static final DataByteArray EMPTY_DATA_BYTE_ARRAY = new DataByteArray(
            new byte[0]);

    // Not sure if AccumuloStorage instances need to be thread-safe or not
    final Text _cfHolder = new Text(), _cqHolder = new Text();
	
    /**
     * Creates an GraphuloStorage which writes all values in a {@link Tuple}
     * with an empty column family and doesn't group column families together on
     * read (creates on {@link Map} for all columns)
     */
    public GraphuloStorage() throws ParseException, IOException {
        this(EMPTY, EMPTY);
    }
	
    /**
     * Create an GraphuloStorage with a CSV of columns-families to use on write
     * and whether columns in a row should be grouped by family on read.
     * 
     * @param columns
     *            A comma-separated list of column families to use when writing
     *            data, aligned to the n'th entry in the tuple
     * @param aggregateColfams
     *            Should unique column qualifier and value pairs be grouped
     *            together by column family when reading data
     */
    public GraphuloStorage(String columns) throws ParseException, IOException {
        this(columns, EMPTY);
    }

    public GraphuloStorage(String columnStr, String args)
            throws ParseException, IOException {
        super(columnStr, args);
    }
}
