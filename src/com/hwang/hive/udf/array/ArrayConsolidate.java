package com.hwang.hive.udf.array;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;

import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/*
 * 1. Filter to keep structs with distinct slice, MAX timestamp
 * 2. Remove structs where timestamp is before a specified period (eg. 30 days)  OR 
 */

public class ArrayConsolidate extends GenericUDF{
	
	List<Object> ret = new ArrayList<Object>();
    
    ListObjectInspector loi;
    StructObjectInspector elOi;
    
	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
	    if(arg0.length != 2 ) {
	        throw new UDFArgumentException("2 arguments needed, found " + arg0.length );
	    }
	    
	 // first argument must be a list/array
	    if (! arg0[0].getCategory().equals(LIST)) {
	        throw new UDFArgumentTypeException(0, "Argument 1"
	            + " of function " + this.getClass().getCanonicalName() + " must be " + Constants.LIST_TYPE_NAME
	            + ", but " + arg0[0].getTypeName()
	            + " was found.");
	    }
	    
	    // a list/array is read by a LIST object inspector
	    loi = (ListObjectInspector) arg0[0];
	    
	    // a list has an element type associated to it
	    // elements must be structs for this UDF
	    if( loi.getListElementObjectInspector().getCategory() != ObjectInspector.Category.STRUCT) {
	        throw new UDFArgumentTypeException(0, "Argument 1"
	            + " of function " +  this.getClass().getCanonicalName() + " must be an array of structs " +
	            " but is an array of " + loi.getListElementObjectInspector().getCategory().name());
	    }
	    
	 // store the object inspector for the elements
	    elOi = (StructObjectInspector)loi.getListElementObjectInspector();
	 
	    // returns the same object inspector
	    return  loi;
	}
	
	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		// get list
	    if(arg0==null || arg0.length != 2) {
	        throw new HiveException("received " + (arg0 == null? "null" :
	            Integer.toString(arg0.length) + " elements instead of 2"));
	    }
	 
	    // each object is supposed to be a struct
	    // we make a shallow copy of the list. We don't want to sort 
	    // the list in place since the object could be used elsewhere in the
	    // hive query
	    ArrayList al = new ArrayList(loi.getList(arg0[0].get()));
	    
	    //String structField = (String) arg0[1].get();
	    //check each element in the arrayist, if dup, remove
	    
	    int numElements = loi.getListLength(arg0[0].get());
	    
	    for (int i = 0; i < numElements; i++) {
	    	//r is struct, structField is slice
	    	//check each r, whether the slice field value exist, if exists, check timestamp
            LazyLong slice = (LazyLong) (elOi.getStructFieldData(loi.getListElement(arg0[0].get(), i), elOi.getStructFieldRef("slice")));
            LazyLong timeStamp = (LazyLong) (elOi.getStructFieldData(loi.getListElement(arg0[0].get(), i), elOi.getStructFieldRef("timestamp")));
            
            
            ret.add(slice);
        }
        return ret;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return null;
	}
}
