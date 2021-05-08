package com.manolo.firebase2bq;

import java.util.HashMap;
import com.google.cloud.bigquery.StandardSQLTypeName;

public class TypeCoverter {

    public static final String FB_INT64 = "INT64";
    public static final String FB_CHAR =  "CHAR";
    public static final String FB_VARCHAR =  "VARCHAR";
    public static final String FB_TIMESTAMP = "TIMESTAMP"; 
    public static final String FB_DECIMAL = "DECIMAL";
    public static final String FB_FLOAT = "FLOAT";
    public static final String FB_BLOB = "BLOB";
    public static final String FB_INTEGER = "INTEGER";
    public static final String FB_SMALLINT= "SMALLINT";
    public static final String FB_DOUBLE = "DOUBLE";

    private static HashMap<String,StandardSQLTypeName> fb2bqTypeMapping = new HashMap<String,StandardSQLTypeName>();
    
    static{
        fb2bqTypeMapping.put(FB_INT64, StandardSQLTypeName.INT64);
        fb2bqTypeMapping.put(FB_CHAR, StandardSQLTypeName.STRING);
        fb2bqTypeMapping.put(FB_VARCHAR, StandardSQLTypeName.STRING);
        fb2bqTypeMapping.put(FB_TIMESTAMP, StandardSQLTypeName.DATETIME);
        fb2bqTypeMapping.put(FB_DECIMAL, StandardSQLTypeName.NUMERIC);
        fb2bqTypeMapping.put(FB_FLOAT, StandardSQLTypeName.FLOAT64);
        fb2bqTypeMapping.put(FB_BLOB, StandardSQLTypeName.BYTES);
        fb2bqTypeMapping.put(FB_INTEGER, StandardSQLTypeName.INT64);
        fb2bqTypeMapping.put(FB_SMALLINT, StandardSQLTypeName.INT64);
        fb2bqTypeMapping.put(FB_DOUBLE, StandardSQLTypeName.FLOAT64);
        //fb2bqTypeMapping.put(FB_DOUBLE, StandardSQLTypeName.BIGNUMERIC);
    }

    public static StandardSQLTypeName getDataType(String fbDataType) throws Exception{
        StandardSQLTypeName res = fb2bqTypeMapping.get(fbDataType);
        if(res == null)
            throw new RuntimeException("Firebird data type not found: " + fbDataType);
        return res;
    }

}
