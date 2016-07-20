package edu.mit.ll.graphulo.pig.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;

/**
 * Class to convert the original Accumulo-backed (FromVertex, {[ToVertex#Value]}) to (FromVertex,ToVertex,Value)
 * 
 * @author ti26350
 *
 */
public class ExtractMaps extends EvalFunc<DataBag> {
	
	 private static final BagFactory bagFactory = BagFactory.getInstance();
	
	/**
	 * 
	 */
    public DataBag exec(Tuple input) throws IOException 
    {
        if (input == null || input.size() != 2)
            return null;
        
        try{
//        	Iterator<Object> it = input.iterator();
//        	while(it.hasNext()) {
//       		Tuple T = (Tuple) it.next();
//        		System.out.println(T.get(0).toString());
//        	}
        	
        	//Tuple output = TupleFactory.getInstance().newTuple(2);
        	//output.set(0, input.get(1));
            //output.set(1, input.get(0));
            
            DataBag db = bagFactory.newDefaultBag();
            
            Map<String,Object> m = (Map<String,Object>) input.get(1);
            Iterator<Entry<String,Object>> it = m.entrySet().iterator();
            while(it.hasNext()) {
            	Entry<String,Object> e = it.next();
            	Tuple output = TupleFactory.getInstance().newTuple(3);
            	output.set(0, input.get(0));
            	output.set(1, e.getKey().substring(1));
            	output.set(2, e.getValue());
            	db.add(output);
            }
            
            return db;
        } catch(Exception e){
            System.err.println("Failed to process input; error - " + e.getMessage());
            return null;
        }
    }
    
    public Schema outputSchema(Schema input) {
        try{
            Schema tupleSchema = new Schema();
            tupleSchema.add(input.getField(0));
            tupleSchema.add(input.getField(0));
            tupleSchema.add(input.getField(0));
            return new Schema(new Schema.FieldSchema("",tupleSchema, DataType.TUPLE));
        }catch (Exception e){
                return null;
        }
    }
}