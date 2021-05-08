# firebird2bigquery

## Goal

This is a Java migration tool for coping and transforming data from  Firebird database into BigQuery.

The Java utility will do direct connection to Firebird and BigQuery. There are no extra import/export files in the process.

## Datatype mapping

Datatype mapping between Firebird and BigQuery is as follows:

| Firebird | BigQuery |
| --- | ----------- |
| INT64 | INT64 |
| CHAR | STRING |
| VARCHAR | STRING |
| TIMESTAMP | DATETIME |
| DECIMAL | NUMERIC |
| FLOAT | FLOAT64 |
| BLOB | BYTES |
| INTEGER | INT64 |
| SMALLINT | INT64 |
| DOUBLE | FLOAT64 |

Tables in BigQuery are created if not already present.
Then data is inserted with that mapping rules.

## Dependencies

Java libraries included as maven dependecies:

* **[Firebird JDBC Driver](https://firebirdsql.org/en/jdbc-driver/)**. 
* **[BigQuery API Client Libraries](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-java)**. 

## Build

Prerequisites:
* Java JDK
* Maven


Clone the repo:
```
git clone https://github.com/mahurtado/firebird2bigquery
```

Build:
```
cd firebird2bigquery
mvn clean compile assembly:single
```

This will create a single jar file with all the dependencies.


## Usage

Prerequisites:
* Java JRE/JDK
* Direct connectivity with Firebird and BigQuery. Firebird connection string and credentials.
* [BigQuery authentication](https://cloud.google.com/bigquery/docs/authentication)

Command line:
```
java -Dparameter1=value1 -Dparameter2=value2 ... -jar target/firebase2bq-1.0-SNAPSHOT-jar-with-dependencies.jar
```

You can run also directly from maven:
```
mvn compile exec:java -Dexec.mainClass="com.manolo.firebase2bq.Loader" [...options...]
```

Parameters:

* **firebirdAddress**: Firebird database IP or DNS name. Example: 192.168.13.13
* **firebirdPort**: Firebird database Port. Example: 3050
* **firebirdDatabase**: the firebird database alias. Example: d:/MY_DATABASE.FDB
* **firebirdUser**: Firebird database User. Example: SYSDBA
* **firebirdPassword**:  Firebird database User password
* **bigqueryProject**: Your Google Cloud project. 
* **bigqueryDataset**: BigQuery target dataset.
* **tables**: Comma-separated list of tables to copy from Firebird to  BigQuery. Example: EMPLOYEE,JOB,OFFICES
* **filter**: Filter to import subset of data. Example: " WHERE CREATION_DATE > '2021-04-17 05:27:45'"

Example:

```
java -DfirebirdAddress=10.10.10.10 -DfirebirdPort=3050 -DfirebirdDatabase=d:/MY_DATABASE.FDB -Dfirebir
dUser=SYSDBA -DfirebirdPassword='superSecret' -DbigqueryProject=my-gcp-project -DbigqueryDataset=my_bq_dataset -Dta
bles=EMPLOYEE,JOB,OFFICES -jar target/firebase2bq-1.0-SNAPSHOT-jar-with-dependencies.jar
````
