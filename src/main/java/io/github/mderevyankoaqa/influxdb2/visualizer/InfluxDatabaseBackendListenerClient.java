package io.github.mderevyankoaqa.influxdb2.visualizer;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.github.mderevyankoaqa.influxdb2.visualizer.config.KafkaConfig;
import io.github.mderevyankoaqa.influxdb2.visualizer.config.TestStartEndMeasurement;
import io.github.mderevyankoaqa.influxdb2.visualizer.config.VirtualUsersMeasurement;
import io.github.mderevyankoaqa.influxdb2.visualizer.kafka.client.KafkaMetricsClient;
import io.github.mderevyankoaqa.influxdb2.visualizer.result.SampleResultPointContext;
import io.github.mderevyankoaqa.influxdb2.visualizer.result.SampleResultPointProvider;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.http.sampler.HTTPSampleResult;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterContextService.ThreadCounts;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Backend listener that publishes JMeter metrics to Kafka as InfluxDB line protocol.
 *
 * @author Alexander Wert
 * @author Michael Derevyanko (minor changes and improvements)
 */

public class InfluxDatabaseBackendListenerClient extends AbstractBackendListenerClient implements Runnable {

    /**
     * Constructs a new instance of {@link InfluxDatabaseBackendListenerClient}.
     * This constructor is currently empty, but can be used to initialize any necessary
     * resources or configurations in future extensions.
     */
    public InfluxDatabaseBackendListenerClient() {

    }

    /**
     * Logger.
     */
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(InfluxDatabaseBackendListenerClient.class);

    /**
     * Parameter Keys.
     */
    private static final String KEY_USE_REGEX_FOR_SAMPLER_LIST = "useRegexForSamplerList";
    private static final String KEY_TEST_NAME = "testName";
    private static final String KEY_RUN_ID = "runId";
    private static final String KEY_INCLUDE_BODY_OF_FAILURES = "saveResponseBodyOfFailures";
    private static final String KEY_NODE_NAME = "nodeName";
    private static final String KEY_SAMPLERS_LIST = "samplersList";
    private static final String KEY_RECORD_SUB_SAMPLES = "recordSubSamples";

    private final WritePrecision writePrecision = WritePrecision.MS;

    /**
     * Constants.
     */
    private static final String SEPARATOR = ";";
    private static final int ONE_MS_IN_NANOSECONDS = 1000000;

    /**
     * Scheduler for periodic metric aggregation.
     */
    private ScheduledThreadPoolExecutor scheduler;

    /**
     * Name of the test.
     */
    private String testName;

    /**
     * A unique identifier for a single execution (aka 'run') of a load test.
     * In a CI/CD automated performance test, a Jenkins or Bamboo build id would be a good value for this.
     */
    private String runId;

    /**
     * Name of the name.
     */
    private String nodeName;

    /**
     * Regex if samplers are defined through regular expression.
     */
    private String regexForSamplerList;

    /**
     * Set of samplers to record.
     */
    private Set<String> samplersToFilter;

    /**
     * Random number generator.
     */
    private SecureRandom randomNumberGenerator;

    /**
     * Indicates whether to record Sub samples.
     */
    private boolean recordSubSamples;

    /**
     * The Kafka writer configuration.
     */
    private KafkaConfig kafkaConfig;

    private ScheduledFuture<?> scheduledFuture;

    /**
     * Processes sampler results.
     */
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        // Gather all the listeners
        List<SampleResult> allSampleResults = new ArrayList<>();
        for (SampleResult sampleResult : sampleResults) {
            allSampleResults.add(sampleResult);

            if (recordSubSamples) {
                Collections.addAll(allSampleResults, sampleResult.getSubResults());
            }
        }

        for (SampleResult sampleResult : allSampleResults) {
            getUserMetrics().add(sampleResult);
            // Determine the type of sample, whether it is a request or a transaction controller
            String samplerType = "transaction";
            if (sampleResult instanceof HTTPSampleResult){
                samplerType = "request";
            }

            if ((null != regexForSamplerList && sampleResult.getSampleLabel().matches(regexForSamplerList))
                    || samplersToFilter.contains(sampleResult.getSampleLabel())) {

                SampleResultPointContext sampleResultContext = new SampleResultPointContext();
                sampleResultContext.setRunId(this.runId);
                sampleResultContext.setTestName(this.testName);
                sampleResultContext.setNodeName(this.nodeName);
                sampleResultContext.setSampleResult(sampleResult);
                sampleResultContext.setSamplerType(samplerType);
                sampleResultContext.setTimeToSet(System.currentTimeMillis() * ONE_MS_IN_NANOSECONDS + this.getUniqueNumberForTheSamplerThread());
                sampleResultContext.setErrorBodyToBeSaved(context.getBooleanParameter(KEY_INCLUDE_BODY_OF_FAILURES, false));
                sampleResultContext.setResponseBodyLength(this.kafkaConfig.getResponseBodyLength());
                var sampleResultPointProvider = new SampleResultPointProvider(sampleResultContext);

                Point resultPoint = sampleResultPointProvider.getPoint();
                KafkaMetricsClient.getInstance(this.kafkaConfig, LOGGER).send(resultPoint);
            }
        }
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        arguments.addArgument(KEY_TEST_NAME, "Test");
        arguments.addArgument(KEY_NODE_NAME, "Test-Node");
        arguments.addArgument(KEY_RUN_ID, "R001");
        arguments.addArgument(KafkaConfig.KEY_BOOTSTRAP_SERVERS, KafkaConfig.DEFAULT_BOOTSTRAP_SERVERS);
        arguments.addArgument(KafkaConfig.KEY_TOPIC, KafkaConfig.DEFAULT_TOPIC);
        arguments.addArgument(KafkaConfig.KEY_ACKS, KafkaConfig.DEFAULT_ACKS);
        arguments.addArgument(KafkaConfig.KEY_COMPRESSION_TYPE, KafkaConfig.DEFAULT_COMPRESSION_TYPE);
        arguments.addArgument(KafkaConfig.KEY_LINGER_MS, Integer.toString(KafkaConfig.DEFAULT_LINGER_MS));
        arguments.addArgument(KafkaConfig.KEY_BATCH_SIZE, Integer.toString(KafkaConfig.DEFAULT_BATCH_SIZE));
        arguments.addArgument(KafkaConfig.KEY_MAX_BLOCK_MS, Integer.toString(KafkaConfig.DEFAULT_MAX_BLOCK_MS));
        arguments.addArgument(KafkaConfig.KEY_RETRIES, Integer.toString(KafkaConfig.DEFAULT_RETRIES));
        arguments.addArgument(KafkaConfig.KEY_DELIVERY_TIMEOUT_MS, Integer.toString(KafkaConfig.DEFAULT_DELIVERY_TIMEOUT_MS));
        arguments.addArgument(KafkaConfig.KEY_THRESHOLD_ERROR, Integer.toString(KafkaConfig.DEFAULT_THRESHOLD_ERROR));
        arguments.addArgument(KEY_SAMPLERS_LIST, ".*");
        arguments.addArgument(KEY_USE_REGEX_FOR_SAMPLER_LIST, "true");
        arguments.addArgument(KEY_RECORD_SUB_SAMPLES, "true");
        arguments.addArgument(KEY_INCLUDE_BODY_OF_FAILURES, "true");
        arguments.addArgument(KafkaConfig.KEY_RESPONSE_BODY_LENGTH, Integer.toString(KafkaConfig.DEFAULT_RESPONSE_BODY_LENGTH));

        return arguments;
    }

    @Override
    public void setupTest(BackendListenerContext context) {
        this.testName = context.getParameter(KEY_TEST_NAME, "Test");
        this.runId = context.getParameter(KEY_RUN_ID, "R001"); //Will be used to compare performance of R001, R002, etc of 'Test'
        this.randomNumberGenerator = new SecureRandom ();
        this.nodeName = context.getParameter(KEY_NODE_NAME, "Test-Node");

        this.setupKafkaClient(context);

        Point setupPoint = Point.measurement(TestStartEndMeasurement.MEASUREMENT_NAME).time(System.currentTimeMillis(), writePrecision)
                .addTag(TestStartEndMeasurement.Tags.TYPE, TestStartEndMeasurement.Values.STARTED)
                .addTag(TestStartEndMeasurement.Tags.NODE_NAME, this.nodeName)
                .addTag(TestStartEndMeasurement.Tags.RUN_ID, this.runId)
                .addTag(TestStartEndMeasurement.Tags.TEST_NAME, this.testName)
                .addField(TestStartEndMeasurement.Fields.PLACEHOLDER, "1");

        KafkaMetricsClient.getInstance(this.kafkaConfig, LOGGER).send(setupPoint);

        this.parseSamplers(context);
        this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(2);
        this.scheduler.setRemoveOnCancelPolicy(true);

        this.scheduledFuture = this.scheduler.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);

        // Indicates whether to write sub sample records to the database
        this.recordSubSamples = Boolean.parseBoolean(context.getParameter(KEY_RECORD_SUB_SAMPLES, "false"));
    }

    @Override
    public void teardownTest(BackendListenerContext context) throws Exception {
        LOGGER.info("Shutting down Kafka metrics publisher...");

        this.scheduledFuture.cancel(false);
        this.scheduler.shutdown();

        this.addVirtualUsersMetrics(0, 0, 0, 0, JMeterContextService.getThreadCounts().finishedThreads);

        Point teardownPoint = Point.measurement(TestStartEndMeasurement.MEASUREMENT_NAME).time(System.currentTimeMillis(), writePrecision)
                .addTag(TestStartEndMeasurement.Tags.TYPE, TestStartEndMeasurement.Values.FINISHED)
                .addTag(TestStartEndMeasurement.Tags.NODE_NAME, this.nodeName)
                .addTag(TestStartEndMeasurement.Tags.RUN_ID, this.runId)
                .addTag(TestStartEndMeasurement.Tags.TEST_NAME, this.testName)
                .addField(TestStartEndMeasurement.Fields.PLACEHOLDER, "1");

        KafkaMetricsClient.getInstance(this.kafkaConfig, LOGGER).send(teardownPoint);

        try {
            LOGGER.info("Kafka metrics scheduler terminated!");
            if(!this.scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                LOGGER.info("Threads didn't finish in 30 seconds!");
            }
        } catch (InterruptedException e) {
            LOGGER.error("Error waiting for end of scheduler " + e);
        }

        KafkaMetricsClient.getInstance(this.kafkaConfig, LOGGER).close();
        this.samplersToFilter.clear();
        super.teardownTest(context);
    }

    /**
     * Periodically writes virtual users metrics to influxDB.
     */
    public void run() {
        ThreadCounts tc = JMeterContextService.getThreadCounts();

        this.addVirtualUsersMetrics(getUserMetrics().getMinActiveThreads(),
                getUserMetrics().getMeanActiveThreads(),
                getUserMetrics().getMaxActiveThreads(),
                tc.startedThreads,
                tc.finishedThreads);
    }

    /**
     * Setups the Kafka writer.
     *
     * @param context {@link BackendListenerContext}.
     */
    private void setupKafkaClient(BackendListenerContext context) {
        this.kafkaConfig = new KafkaConfig(context);

        KafkaMetricsClient client = KafkaMetricsClient.getInstance(this.kafkaConfig, LOGGER);
        client.setMessageKey(this.runId + ":" + this.nodeName);
    }

    /**
     * Parses list of samplers.
     *
     * @param context {@link BackendListenerContext}.
     */
    private void parseSamplers(BackendListenerContext context) {

        //List of samplers to record.
        String samplersList = context.getParameter(KEY_SAMPLERS_LIST, "");
        this.samplersToFilter = new HashSet<>();

        if (context.getBooleanParameter(KEY_USE_REGEX_FOR_SAMPLER_LIST, false)) {
            this.regexForSamplerList = samplersList;
        } else {
            this.regexForSamplerList = null;
            String[] samplers = samplersList.split(SEPARATOR);

            this.samplersToFilter = new HashSet<>();
            Collections.addAll(this.samplersToFilter, samplers);
        }
    }

    /**
     * Writes thread metrics.
     */
    private void addVirtualUsersMetrics(int minActiveThreads, int meanActiveThreads, int maxActiveThreads, int startedThreads, int finishedThreads) {
        Point virtualUsersMetricsPoint = Point.measurement(VirtualUsersMeasurement.MEASUREMENT_NAME).time(System.currentTimeMillis(), writePrecision)
                .addField(VirtualUsersMeasurement.Fields.MIN_ACTIVE_THREADS, minActiveThreads)
                .addField(VirtualUsersMeasurement.Fields.MAX_ACTIVE_THREADS, maxActiveThreads)
                .addField(VirtualUsersMeasurement.Fields.MEAN_ACTIVE_THREADS, meanActiveThreads)
                .addField(VirtualUsersMeasurement.Fields.STARTED_THREADS, startedThreads)
                .addField(VirtualUsersMeasurement.Fields.FINISHED_THREADS, finishedThreads)
                .addTag(VirtualUsersMeasurement.Tags.NODE_NAME, this.nodeName)
                .addTag(VirtualUsersMeasurement.Tags.TEST_NAME, this.testName)
                .addTag(VirtualUsersMeasurement.Tags.RUN_ID, this.runId);

        KafkaMetricsClient.getInstance(this.kafkaConfig, LOGGER).send(virtualUsersMetricsPoint);
    }

    /**
     * Try to get a unique number for the sampler thread.
     */
    private int getUniqueNumberForTheSamplerThread() {

        return randomNumberGenerator.nextInt(ONE_MS_IN_NANOSECONDS);
    }

}