package com.manolo.firebase2bq;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.http.HttpTransportOptions;

import org.threeten.bp.Duration;

public class Connections {

    private static Connections instance = new Connections();
	
	//private BigQuery bigqueryClient;
	private java.sql.Connection firebirdConnection;
	private BigQuery bigquery;

	private static final Logger log = Logger.getLogger( Connection.class.getName() );

    private Connections(){
		try {
			bigquery = BigQueryOptions.newBuilder()
				.setRetrySettings(RetrySettings.newBuilder()
					.setMaxAttempts(10)
					.setRetryDelayMultiplier(1.5)
					.setTotalTimeout(Duration.ofMinutes(5))
					.build())
				.build()
			.getService();
			
			/*HttpTransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions();
			transportOptions = transportOptions.toBuilder().setConnectTimeout(60000).setReadTimeout(60000)
				.build();
			BigQueryOptions bigqueryOptions = BigQueryOptions.newBuilder().
				.setRetrySettings(retrySettings())
				.setTransportOptions(transportOptions)
				.build();*/

            firebirdConnection = DriverManager.getConnection(
                "jdbc:firebirdsql://" + Config.getFirebirdAddress() + ":" + Config.getFirebirdPort() + "/" + Config.getFirebirdDatabase(),
                Config.getFirebirdUser(), 
                Config.getFirebirdPassword()
            ); 
		} catch (Exception e) {
			e.printStackTrace();
			log.log(Level.SEVERE, "Could not establish connection. Exiting. Error message: " + e.getMessage());
			System.exit(0);
		}
    }
    
	public static java.sql.Connection  getFirebirdConnection(){
		return instance.firebirdConnection;
	}

	public static BigQuery  getBigqueryClient(){
		return instance.bigquery;
	}	

	public static TableId getBigqueryTableId(String tableName){
		return TableId.of(Config.getBigqueryDataset(), tableName);
	}	

}
