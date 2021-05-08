package com.manolo.firebase2bq;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Config {

    private static final Logger log = Logger.getLogger( Config.class.getName() );
	
	public static final String DEFAULT_FIREBIRD_PORT = "3050";
    public static final String DEFAULT_BATCH_SIZE = "200";
	
	private String firebirdAddress;
	private int firebirdPort;
    private String firebirdDatabase;
    private String firebirdUser;
    private String firebirdPassword;
	private String bigqueryProject;
	private String bigqueryDataset;
    private int bigqueryBatchInsertSize;
    private String tables;
    private String filter;

	private static Config instance = new Config();

    private Config() {
        this.firebirdAddress = System.getProperty("firebirdAddress");
        this.firebirdPort = Integer.parseInt(System.getProperty("firebirdPort", DEFAULT_FIREBIRD_PORT));
        this.firebirdDatabase = System.getProperty("firebirdDatabase");
        this.firebirdUser = System.getProperty("firebirdUser");
        this.firebirdPassword = System.getProperty("firebirdPassword");
        this.bigqueryProject = System.getProperty("bigqueryProject");
        this.bigqueryDataset = System.getProperty("bigqueryDataset");
        this.bigqueryBatchInsertSize = Integer.parseInt(System.getProperty("bigqueryBatchInsertSize", DEFAULT_BATCH_SIZE));
        this.tables = System.getProperty("tables");
        this.filter = System.getProperty("filter");

        log.log(Level.INFO, "Configuration Loaded: "+ this.toString());
    }

    public static String getFirebirdAddress() {
        return instance.firebirdAddress;
    }

    public static int getFirebirdPort() {
        return instance.firebirdPort;
    }

    public static String getFirebirdDatabase() {
        return instance.firebirdDatabase;
    }
    
    public static String getFirebirdUser() {
        return instance.firebirdUser;
    }
    
    public static String getFirebirdPassword() {
        return instance.firebirdPassword;
    }   

    public static String getBigqueryProject() {
        return instance.bigqueryProject;
    }

    public static String getBigqueryDataset() {
        return instance.bigqueryDataset;
    }
    
    public static int getBigqueryBatchInsertSize() {
        return instance.bigqueryBatchInsertSize;
    }

    public static String getTables() {
        return instance.tables;
    }

    public static String getFilter() {
        return instance.filter;
    }

    @Override
    public String toString() {
        return "{" +
            "\nfirebirdAddress='" + firebirdAddress + "'" +
            ",\nfirebirdPort='" + firebirdPort + "'" +
            ",\nfirebirdDatabase='" + firebirdDatabase + "'" +
            ",\nfirebirdUser='" + firebirdUser + "'" +
            ",\nfirebirdPassword='" + firebirdPassword + "'" +
            ",\nbigqueryProject='" + bigqueryProject + "'" +
            ",\nbigqueryDataset='" + bigqueryDataset + "'" +
            ",\nbigqueryBatchInsertSize='" + bigqueryBatchInsertSize + "'" +
            ",\ntables='" + tables + "'" +
            ",\nfilter='" + filter + "'" +
            "}";
    }   

}
