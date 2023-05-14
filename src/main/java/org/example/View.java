package org.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.io.IOException;
import java.util.List;

public class View {

    public static final String AWS_ACCOUNT_ID = "...";
    public static final long SLEEP_AMOUNT_IN_MS = 1000;


    public static void main(String[] args) throws IOException, InterruptedException {

        AthenaClient athenaClient = AthenaClient.builder().region(Region.US_EAST_1).build();

        String queryExecutionId = "";


        try {

            // database is defined on QueryExecutionContext
            QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                    .database("d1")
                    .build();

            // output bucket is defined on result configuration
            ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                    .outputLocation("s3://athena-output-2023-04")
                    .build();

            StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                    .queryString("CREATE VIEW view1 AS SELECT * FROM simple WHERE Name LIKE '%a%'")
                    .queryExecutionContext(queryExecutionContext)
                    .resultConfiguration(resultConfiguration)
                    .build();

            StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
            queryExecutionId = startQueryExecutionResponse.queryExecutionId();

        } catch (AthenaException e) {
            e.printStackTrace();
            System.exit(1);
        }


        // Wait for an Amazon Athena query to complete, fail or to be cancelled

        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId)
                .build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("The Amazon Athena query failed to run with error message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("The Amazon Athena query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Sleep and retry
                Thread.sleep(SLEEP_AMOUNT_IN_MS);
            }
            System.out.println("The current status is: " + queryState);
        }


        try {

            // Max Results can be set but if its not set it will choose the maximum page size

            GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                    .queryExecutionId(queryExecutionId)
                    .build();

            GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);

            for (GetQueryResultsResponse result : getQueryResultsResults) {
                List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
                List<Row> results = result.resultSet().rows();
                for (Row myRow : results) {
                    List<Datum> allData = myRow.data();
                    for (Datum data : allData) {
                        System.out.println("The value of the column is "+data.varCharValue());
                    }
                }
            }

        } catch (AthenaException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

}
