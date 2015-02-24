package com.hwang.hive.udf.array;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.text.ParseException;


public final class ArrayElementDiff extends UDF{
	public static List<Long> evaluate(ArrayList<Long> array) {
		if (array == null || array.size() < 2)
			return null;

		List<Long> res = new ArrayList<Long>();
		for(int i=1; i<array.size(); i++)
		{
			Long cur_dt = array.get(i); 	
			Long prev_dt = array.get(i-1);

		    long diff = 0; 

					diff = cur_dt - prev_dt;

		 res.add(diff);	
		}
		
		return res;	
	}

}