package com.manolo.firebase2bq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;

import java.sql.*;

public class Loader {

    private static final Logger log = Logger.getLogger( Loader.class.getName() );

    public static void main(String[] args) {
		java.sql.Connection firebirdConn = null;
		try {
			firebirdConn = Connections.getFirebirdConnection();
			String tables[] = Config.getTables().split(",");
 
			for(String table : tables){
				List<Column> tableColumns = getFirebirdTableInfo(firebirdConn, table);
				int insertedRows = processTable(firebirdConn, table, tableColumns);
				log.log(Level.INFO, "TABLE " + table + " processed. " + insertedRows + " documents loaded");
			}	
			
		} catch (Exception e) {
            log.log(Level.SEVERE, e.getMessage());
			e.printStackTrace();
		}
		finally {
			if(firebirdConn != null){
				try {
					firebirdConn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static List<Column> getFirebirdTableInfo(java.sql.Connection firebirdConn, String tableName) throws Exception{
        try {  
            List<Column> columns = new ArrayList<Column>();

            DatabaseMetaData databaseMetaData = firebirdConn.getMetaData();
            ResultSet rs = databaseMetaData.getColumns(null,null, tableName, null);
            while(rs.next()){
				String columnName = rs.getString("COLUMN_NAME");
				String columnType = rs.getString("TYPE_NAME");
                String[]columnTypeTokens = columnType.split("[ (,)]+");
				int columnIndex = rs.getInt("ORDINAL_POSITION");
				columns.add(new Column(columnIndex, columnName, columnTypeTokens[0]));
            }
            rs.close();
            Collections.sort(columns);

            //log.log(Level.INFO, "TABLE " + tableName + " Columns:");
            //log.log(Level.INFO, Arrays.toString(columns.toArray(new Column[0])));

            return columns;

        } catch (Exception e) {
			log.log(Level.SEVERE, "Error processing table info: " + tableName);
            throw e;
		}
    }
 
    public static int processTable(java.sql.Connection firebirdConn, String tableName, List<Column> tableColumns) throws Exception{
        log.log(Level.INFO, "Processing table " + tableName + " ...");
        int total = 0;

        // Get BigQuery table. Create if needed
        BigQuery bigquery = Connections.getBigqueryClient();
        TableId tableId = Connections.getBigqueryTableId(tableName);
        Table table = bigquery.getTable(tableId);
        if(table != null && table.exists()){
            log.log(Level.INFO, "table " + tableName + " exists");
        }
        else{
            log.log(Level.INFO, "table " + tableName + " do NOT exists. Creating table ...");
            createBQTable(bigquery, tableId, tableColumns);
            tableId = Connections.getBigqueryTableId(tableName);
        }

        // Read Firebird records and write into BigQuery
        String querySource = "SELECT * FROM " + tableName;
        if(Config.getFilter() != null){
            querySource = querySource + Config.getFilter();
        }
        log.log(Level.INFO, "Query for source table: " + querySource);
        Statement stmt = firebirdConn.createStatement();
        ResultSet rsSrcTable = stmt.executeQuery(querySource);
        int buffCount = 0;
        int pointCount = 0;
       
        InsertAllRequest.Builder request = InsertAllRequest.newBuilder(tableId);

        while (rsSrcTable.next()) {
            Map<String, Object> rowContent = new HashMap<>();
            for(Column col : tableColumns){
                Object bqValue = getColumnFormatValue(col, rsSrcTable);
                rowContent.put(col.getName(), bqValue);
            }
            request = request.addRow(rowContent);
            buffCount ++;
            if(buffCount >= Config.getBigqueryBatchInsertSize()){
                // Write rows to BQ
                InsertAllResponse response = bigquery.insertAll(request.build());
                if (response.hasErrors()) {
                    for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                      System.out.println("Response error: \n" + entry.getValue());
                    }
                }
                request = InsertAllRequest.newBuilder(tableId);
                pointCount++;
                System.out.print(".");
                total += buffCount;
                buffCount = 0;
                if(pointCount % 50 == 0){
                    System.out.println(" " + total + " items written");
                    pointCount = 0;
                }   
            }
        }
        // Write remaining items
        if(buffCount > 0){
            InsertAllResponse response = bigquery.insertAll(request.build());
            System.out.println(".");
            total += buffCount;
            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                  System.out.println("Response error: \n" + entry.getValue());
                }
            }            
        }
       
        return total;
    }

    private static Object getColumnFormatValue(Column col, ResultSet rsSrcTable) throws Exception {
        Object res = null;
        StandardSQLTypeName bqType = TypeCoverter.getDataType(col.getType());
        if(bqType == StandardSQLTypeName.INT64)
            res = rsSrcTable.getInt(col.getName());
        else if(bqType == StandardSQLTypeName.STRING)
            res = rsSrcTable.getString(col.getName());
        else if(bqType == StandardSQLTypeName.DATETIME)
            res = rsSrcTable.getDate(col.getName());
        else if(bqType == StandardSQLTypeName.NUMERIC)
            res = rsSrcTable.getDouble(col.getName());
        else if(bqType == StandardSQLTypeName.FLOAT64)
            res = rsSrcTable.getDouble(col.getName());
            //res = rsSrcTable.getFloat(col.getName());
        else if(bqType == StandardSQLTypeName.BYTES){
            Blob blob = rsSrcTable.getBlob(col.getName());
            if(blob == null)
                return null;
            int blobLength = (int) blob.length();  
            byte[] blobAsBytes = blob.getBytes(1, blobLength);
            res = new String(Base64.getEncoder().encode(blobAsBytes));
            blob.free();
        }
        else{
            log.log(Level.SEVERE, "Unknown BQ type: " + col.getName());
            throw new RuntimeException("Unknown BQ type: " + col.getName());
        }
        return res;
    }

    private static void createBQTable(BigQuery bigquery, TableId tableId, List<Column> tableColumns) throws Exception {
        ArrayList<Field> fields = new ArrayList<Field>();
        for(Column col : tableColumns){
            fields.add(Field.of(col.getName(), TypeCoverter.getDataType(col.getType())));
        }
        Schema schema = Schema.of(fields);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigquery.create(tableInfo);
        log.log(Level.INFO, "table created");
    }
}
