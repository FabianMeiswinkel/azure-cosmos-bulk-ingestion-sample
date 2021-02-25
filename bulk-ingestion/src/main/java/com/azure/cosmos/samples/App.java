package com.azure.cosmos.samples;

import com.azure.cosmos.BulkOperations;
import com.azure.cosmos.BulkProcessingOptions;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosItemOperation;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashSet;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class App
{
    private final static Logger LOGGER = LoggerFactory.getLogger(App.class);
    private static final long RECORD_COUNT = 5_000_000;
    private static final int BINARY_PAYLOAD_SIZE = 2000;
    private static final Semaphore MAX_IN_FLIGHT_ITEMS_THRESHOLD = new Semaphore(100_000);
    private static final Random RND = new Random();
    private static final AtomicBoolean completed = new AtomicBoolean(false);
    private static final AtomicBoolean continueLoop = new AtomicBoolean(true);
    private static final AtomicLong successfullyIngestedDocuments = new AtomicLong(0);
    private static final AtomicLong failureCount = new AtomicLong(0);
    private static final AtomicLong totalRU = new AtomicLong();

    private static final HashSet<String> responsesAccountedForInTelemetry = new HashSet<>();

    private static final AtomicLong processedCount = new AtomicLong(0);
    private static final AtomicLong finishedEpochMs = new AtomicLong(0);

    public static void main( String[] args ) throws InterruptedException {
        Long startEpochMs = Instant.now().toEpochMilli();

        String testRunId = UUID.randomUUID().toString();
        AtomicLong itemsGenerated = new AtomicLong(0);
        Flux<CosmosItemOperation> testData = Flux.generate(
            sink -> {
                if (itemsGenerated.incrementAndGet() < RECORD_COUNT) {
                    String id = UUID.randomUUID().toString();
                    CosmosItemOperation operation = BulkOperations.getCreateItemOperation(
                        generateTestItem(id),
                        new PartitionKey(id));

                    try {
                        MAX_IN_FLIGHT_ITEMS_THRESHOLD.acquire();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    sink.next(operation);
                } else {
                    sink.complete();
                }
            });

        BulkProcessingOptions<String> options = new BulkProcessingOptions<>(testRunId)
            .setMaxMicroBatchConcurrency(2)
            .setMaxMicroBatchSize(300);

        getContainer()
            .processBulkOperations(testData.publishOn(Schedulers.boundedElastic()), options)
            .subscribe(r -> {
                MAX_IN_FLIGHT_ITEMS_THRESHOLD.release();
                long processedCountSnapshot = processedCount.incrementAndGet();
                if (processedCountSnapshot % 100_000 == 0) {
                    System.out.println(String.format("Generated#%d, Processed#%d",itemsGenerated.get(),processedCountSnapshot));
                }
                if (!r.getResponse().isSuccessStatusCode()) {
                    LOGGER.warn(
                        String.format(
                            "Failed to ingest document '%s' - StatusCode %d - ActivityId %s - Diagnostics %s",
                            r.getOperation().getId(),
                            r.getResponse().getStatusCode(),
                            r.getResponse().getActivityId(),
                            r.getResponse().getCosmosDiagnostics().toString()));
                    failureCount.incrementAndGet();
                } else {
                    if (responsesAccountedForInTelemetry.add(r.getResponse().getActivityId())) {
                        Double ru = r.getResponse().getRequestCharge() * 100d;
                        totalRU.addAndGet(ru.longValue());
                    }
                    successfullyIngestedDocuments.incrementAndGet();
                }
            },
            t -> {
                LOGGER.error("Failed to bulk ingest", t);
                continueLoop.set(false);
            },
            () -> {
                finishedEpochMs.set(Instant.now().toEpochMilli());
                completed.set(true);
                continueLoop.set(false);
            });

        while (continueLoop.get()) {
            printStatistics();
            Thread.sleep(5000);
        }
        printStatistics();

        System.out.println(
            String.format("Ingestion took %s",Duration.ofMillis(finishedEpochMs.get() - startEpochMs).toString()));
        System.out.println( "Finished" );
    }

    private static void printStatistics() {
        long successCountSnapshot = successfullyIngestedDocuments.get();
        long failureCountSnapshot = failureCount.get();
        int microBatchCountSnapshot = responsesAccountedForInTelemetry.size();
        double microBatchCount = ((Integer)microBatchCountSnapshot).doubleValue();
        double totalRUSnapshot = ((Long)totalRU.get()).doubleValue();
        double totalRecordCount = successCountSnapshot + failureCountSnapshot;

        double avgRUSnapshot = microBatchCount != 0 ? (totalRUSnapshot / microBatchCount) : 0;
        double avgItemsPerMicroBatch = microBatchCount != 0 ? (totalRecordCount / microBatchCount) : 0;

        System.out.println(
            String.format(
                "Succeeded: %d, Failures: %d, #micro batches: %d, Avg. RU: %f, Avg. items per micro batch: %f",
                successCountSnapshot,
                failureCountSnapshot,
                microBatchCountSnapshot,
                avgRUSnapshot,
                avgItemsPerMicroBatch));
    }

    private static CosmosAsyncContainer getContainer() {
        CosmosAsyncClient client = new CosmosClientBuilder()
            .endpoint("https://fabianm-samples-westus2.documents.azure.com:443/")
            .key("<key/>")
            .directMode()
            .userAgentSuffix("BulkIngestionSample")
            .contentResponseOnWriteEnabled(false)
            .consistencyLevel(ConsistencyLevel.EVENTUAL)
            .throttlingRetryOptions(
                new ThrottlingRetryOptions()
                    .setMaxRetryAttemptsOnThrottledRequests(10000000)
                    .setMaxRetryWaitTime(Duration.ofDays(10))
            )
            .buildAsyncClient();

        return
            client.getDatabase("samples").getContainer("bulk04");
    }

    private static ObjectNode generateTestItem(String id) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        byte[] blob = new byte[BINARY_PAYLOAD_SIZE];
        RND.nextBytes(blob);
        node.put("id", id);
        node.put("payload", Base64.getUrlEncoder().encodeToString(blob));
        return node;
    }
}
