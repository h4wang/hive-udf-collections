package com.hwang.hive.udf.array;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

public class ArrayIndex extends GenericUDF {
        private ListObjectInspector listInspector;
        private ObjectInspector elemInspector;
        private IntObjectInspector intInspector;
        

        @Override
        public Object evaluate(DeferredObject[] arg0) throws HiveException {
                Object list =  arg0[0].get();
                int idx = intInspector.get( arg0[1].get() );
                
                if (idx < 0) {
                        idx = listInspector.getListLength( list ) + idx;
                }

                Object unInsp =  listInspector.getListElement(list, idx);
                                
                return unInsp;
        }

        @Override
        public String getDisplayString(String[] arg0) {
                return "array_index( " + arg0[0] + " , " + arg0[1]  + " )";
        }

        @Override
        public ObjectInspector initialize(ObjectInspector[] arg0)
                        throws UDFArgumentException {
                if( arg0.length != 2) {
                        throw new UDFArgumentException("array_index takes an array and an int as arguments");
                }
                if( arg0[0].getCategory() != Category.LIST 
                        || arg0[1].getCategory() != Category.PRIMITIVE 
                        || ((PrimitiveObjectInspector)arg0[1]).getPrimitiveCategory() != PrimitiveCategory.INT) {
                        throw new UDFArgumentException("array_index takes an array and an int as arguments");
                }
                listInspector = (ListObjectInspector) arg0[0];
                intInspector = (IntObjectInspector) arg0[1];
                
                return  listInspector.getListElementObjectInspector();
        }

}