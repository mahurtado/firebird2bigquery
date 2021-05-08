package com.manolo.firebase2bq;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID;

public class HelloBigQuery {

    public static void main(String... args) throws Exception {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        System.out.println("bigquery " + bigquery.toString());
        QueryJobConfiguration queryConfig =
            QueryJobConfiguration.newBuilder(
                    "SELECT commit, author, repo_name "
                        + "FROM `bigquery-public-data.github_repos.commits` "
                        + "WHERE subject like '%bigquery%' "
                        + "ORDER BY subject DESC LIMIT 10")
                .setUseLegacySql(false)
                .build();
    
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
    
        queryJob = queryJob.waitFor();
    
        // Check for errors
        if (queryJob == null) {
          throw new RuntimeException("Job no longer exists");
        } 
        else if (queryJob.getStatus().getError() != null) {
          throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        TableResult result = queryJob.getQueryResults();
    
        for (FieldValueList row : result.iterateAll()) {
          String commit = row.get("commit").getStringValue();
          FieldValueList author = row.get("author").getRecordValue();
          String name = author.get("name").getStringValue();
          String email = author.get("email").getStringValue();
          String repoName = row.get("repo_name").getRecordValue().get(0).getStringValue();
          System.out.printf(
              "Repo name: %s Author name: %s email: %s commit: %s\n", repoName, name, email, commit);
        }
    }
}