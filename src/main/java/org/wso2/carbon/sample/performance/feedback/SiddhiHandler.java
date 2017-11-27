package org.wso2.carbon.sample.performance.feedback;

import org.wso2.carbon.sample.performance.feedback.Constants;
import org.wso2.carbon.sample.performance.feedback.TCPClient;
import org.wso2.siddhi.core.*;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.event.Event;

import java.awt.*;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Random;

import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;

/**
 * Created by chamod on 8/28/17.
 */
public class SiddhiHandler {
    private static final Logger log = Logger.getLogger(SiddhiHandler.class);
    private static final int RECORD_WINDOW = 100; //This is the number of events to record.
    private static final Histogram histogram = new Histogram(2);
    private static final Histogram histogram2 = new Histogram(2);
    private static long firstTupleTime = -1;
    private static String logDir = "./results-passthrough-4.0.0-M20";
    private static String filteredLogDir = "./filtered-results-passthrough-4.0.0-M20";
    private static long eventCountTotal = 0;
    private static long eventCount = 0;
    private static long timeSpent = 0;
    private static long totalTimeSpent = 0;
    private static long warmupPeriod = 0;
    private static long totalExperimentDuration = 0;
    private static long startTime = System.currentTimeMillis();
    private static boolean flag = false;
    private static long veryFirstTime = System.currentTimeMillis();
    private static Writer fstream = null;
    private static long outputFileTimeStamp;
    private static boolean exitFlag = false;
    private static int sequenceNumber = 0;
    private static SiddhiHandler siddhiHandler = null;
    /////////////////////////////////////////////////////////////////////////////
    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;
    private String eventStreamName;
    private InputHandler inputHandler;
    private TCPClient feedbackTcpClient;

    public SiddhiHandler() {
        totalExperimentDuration = Long.parseLong("15") * 60000;
        warmupPeriod = 10000;
//        warmupPeriod = Long.parseLong("1") * 60000;

        try {
            File directory = new File(logDir);

            if (!directory.exists()) {
                if (!directory.mkdir()) {
                    log.error("Error while creating the output directory.");
                }
            }

            sequenceNumber = getLogFileSequenceNumber();
            outputFileTimeStamp = System.currentTimeMillis();
            fstream = new OutputStreamWriter(new FileOutputStream(new File(logDir + "/output-" +
                    sequenceNumber + "-" +

                    (outputFileTimeStamp)
                    + ".csv")
                    .getAbsoluteFile()), StandardCharsets.UTF_8);

        } catch (IOException e) {
            log.error("Error while creating statistics output file, " + e.getMessage(), e);
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        siddhiManager = new SiddhiManager();
        createExecutionPlan();
    }

    public static SiddhiHandler getInstance() {
        if (siddhiHandler == null) {
            siddhiHandler = new SiddhiHandler();
        }
        return siddhiHandler;
    }

    /**
     * This method preprocesses the collected data by ignoring the warmup period.
     */
    private static void preprocessPerformanceData() {

        try {
            File directory = new File(filteredLogDir);

            if (!directory.exists()) {
                if (!directory.mkdir()) {
                    log.error("Error while creating the output directory.");
                }
            }


            fstream = new OutputStreamWriter(new FileOutputStream(new File(filteredLogDir + "/output-" +
                    sequenceNumber + "-" +

                    (outputFileTimeStamp)
                    + ".csv")
                    .getAbsoluteFile()), StandardCharsets
                    .UTF_8);

        } catch (IOException e) {
            log.error("Error while creating statistics output file, " + e.getMessage(), e);
        }

        //String csvFile = (logDir + "/output-" + sequenceNumber + "-" + (outputFileTimeStamp) + ".csv");
        BufferedReader br = null;

        //File reading
        try {
            String line;
            String csvSplitBy = ",";
            int iteration = 0;
            br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(logDir + "/output-" + sequenceNumber + "-" +
                            (outputFileTimeStamp) + ".csv"),
                            Charset.forName("UTF-8")));


            while ((line = br.readLine()) != null) {
                if (iteration == 0) {
                    iteration++;
                    continue;
                }
                //use coma separator./
                String[] filteredData = line.split(csvSplitBy);
                // log.error(filteredData[0] + " " + filteredData[1]);
                float time = Float.parseFloat(filteredData[3]);
                if (time > warmupPeriod / 1000.0) {
                    fstream.write(
                            filteredData[0] + "," + filteredData[1] + "," + filteredData[2] + "," + filteredData[3]
                                    + ","
                                    + "" + filteredData[4] + "," + filteredData[5] + "," + filteredData[6] + ","
                                    + "" + filteredData[7] + "," + filteredData[8] + "," + filteredData[9] + ","
                                    + "" + filteredData[10] + "," + filteredData[11] + "," + filteredData[12]);
                    fstream.write("\r\n");
                    fstream.flush();
                }

            }
        } catch (IOException ex) {
            log.error(ex);
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fstream.close();

                } catch (IOException e) {
                    log.error(e);
                }
            }
        }

    }

    /**
     * This method generates the PDF by scanning through the preprocessed data.
     * The report will be kept inside the
     */
    private static void generateReport() {
        try {
            Runtime.getRuntime().exec("python ReportGeneration/reportGeneration.py");
        } catch (IOException e) {
            log.error(e);
        }

    }

    /**
     * This method returns a unique integer that can be used as a sequence number for log files.
     */
    private static int getLogFileSequenceNumber() {
        int result = -1;
        BufferedReader br = null;

        //read the flag
        try {
            String sCurrentLine;

            File directory = new File(logDir);

            if (!directory.exists()) {
                if (!directory.mkdir()) {
                    log.error("Error while creating the output directory.");
                }
            }

            File sequenceFile = new File(logDir + "/sequence-number.txt");

            if (sequenceFile.exists()) {
                br = new BufferedReader(new InputStreamReader(new FileInputStream(logDir + "/sequence-number.txt"),
                        Charset.forName("UTF-8")));

                while ((sCurrentLine = br.readLine()) != null) {
                    result = Integer.parseInt(sCurrentLine);
                    break;
                }
            }
        } catch (IOException e) {
            log.error("Error when reading the sequence number from sequence-number.txt file. " + e.getMessage(), e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        //write the new flag
        try {
            if (result == -1) {
                result = 0;
            }

            String content = "" + (result + 1); //need to increment by one for next round
            File file = new File(logDir + "/sequence-number.txt");

            // if file doesn't exists, then create it
            if (!file.exists()) {
                boolean fileCreateResults = file.createNewFile();
                if (!fileCreateResults) {
                    log.error("Error when creating the sequence-number.txt file.");
                }
            }

            Writer fstream = new OutputStreamWriter(new FileOutputStream(file.getAbsoluteFile()), StandardCharsets
                    .UTF_8);

            fstream.write(content);
            fstream.flush();
            fstream.close();
        } catch (IOException e) {
            log.error("Error when writing performance information. " + e.getMessage(), e);
        }

        return result;
    }

    private static int setCompletedFlag(int sequenceNumber) {
        try {


            String content = "" + sequenceNumber; //need to increment by one for next round
            File file = new File(logDir + "/completed-number.txt");

            // if file doesn't exists, then create it
            if (!file.exists()) {
                boolean fileCreateResults = file.createNewFile();
                if (!fileCreateResults) {
                    log.error("Error when creating the completed-number.txt file.");
                }
            }

            Writer fstream = new OutputStreamWriter(new FileOutputStream(file.getAbsoluteFile()), StandardCharsets
                    .UTF_8);

            fstream.write(content);
            fstream.flush();
            fstream.close();
        } catch (IOException e) {
            log.error("Error when writing performance information. " + e.getMessage(), e);
        }

        return 0;
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private void createExecutionPlan() {

        String definition = "@config(async = 'true') " +
                "define stream inputStream (humidity int, sensorValue double, timestamp long, sensorId int, meta_punctuation int);";

//        String query = "@info(name = 'query1') from inputStream#window.custom:customLengthBatch(1000, meta_punctuation, sensorId) " +
//                "select avg(humidity) as avgHumidity, count(sensorId) as count, timestamp " +
//                "insert into outputStream ;";

//        String query = "@info(name = 'query1') from inputStream#approximate:percentile(sensorId, 0.5, 0.3) " +
//                "select percentile, timestamp " +
//                "insert into outputStream ;";
//        String query = "@info(name = 'query1') from inputStream " +
//                "select math:percentile(sensorId,50.0) as percentile, timestamp " +
//                "insert into outputStream ;";
//        String query = "@info(name = 'query1') from inputStream#window.custom:customLengthBatch(1000, meta_punctuation, sensorId) " +
//                "select max(humidity) as maxx, count(sensorId) as count, timestamp " +
//                "insert into outputStream ;";

//        String query = "@info(name = 'query1') " +
//                "from inputStream#window.timeBatch(1000) " +
//                "select avg(humidity) as avgHumidity, count(sensorId) as count, timestamp " +
//                "insert into outputStream ;";
//        String query = "@info(name = 'query1') " +
//                "from inputStream#window.unique:timeBatch(humidity, 1000, true) " +
//                "select count(*) as count, timestamp " +
//                "insert into outputStream ;";

        String query = "@info(name = 'query1') " +
                "from inputStream " +
                "select count(*) as count, max(humidity) as maxHumidity, timestamp " +
                "insert into outputStream ;";

        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(definition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            long eventCount = 0;
            long startTime;
            @Override
            public synchronized void receive(Event[] events) {
//                        System.out.println("event received..........................");
//                        EventPrinter.print(events);
//                        for (Event event : events) {
////                            new TCPClient(Constants.TCP_HOST, Constants.TCP_PORT).sendMsg(
////                                    "FEEDBACK FROM CONSUMER : SiddhiHandler : " + event.toString());
//                        }
                for (Event evt : events) {
                    if (eventCountTotal == 0) {
//                        feedbackTcpClient = new TCPClient(Constants.TCP_HOST, Constants.TCP_PORT);
                        startTime = System.currentTimeMillis();
                    }
                    long currentTime = System.currentTimeMillis();
//                    if (eventCountTotal % 100 == 0) {
                    feedbackTcpClient.sendMsg("SIDDHI_QUERY_FEEDBACK : EVENT_COUNT : " + evt.getData()[0]
                            + " : AVG : " + evt.getData()[1]);
                    if (eventCountTotal % 100 == 0) {
                        System.out.println("Event: count : " + evt.getData()[0]
                                + ", maxHumidity : " + evt.getData()[1]
                                + ", timeSpent : " + (currentTime - startTime)
                        );
                    }
//                    feedbackTcpClient.sendMsg("TIME_BATCH_EXPIRE_FEEDBACK : EXPIRED");
//                        eventCount += (long)evt.getData()[1];
//                        System.out.println("eventCount : " + eventCountTotal + ", totalEventCount : " + eventCount);
//                    }

                    ////////////////////////////////////////////////////////////////////////////////////
/*
                    histogram.recordValue(timeSpent);
                    histogram2.recordValue(timeSpent);

                    if (firstTupleTime == -1) {
                        firstTupleTime = currentTime;
                    }

                    long iijTimestamp = Long.parseLong(evt.getData()[2].toString());

                    try {
                        eventCount++;*/
                    eventCountTotal++;/*
                        timeSpent += (currentTime - iijTimestamp);
//                        System.out.println("Time diff in Siddhi Handler: " + (currentTime - iijTimestamp));//TODO : testing..........

                        if (eventCount % RECORD_WINDOW == 0) {
                            totalTimeSpent += timeSpent;
                            long value = currentTime - startTime;

                            if (value == 0) {
                                value++;
                            }

                            if (!flag) {
                                flag = true;
                                fstream.write("Id, Throughput in this window (events/second), Entire throughput " +
                                        "for the run (events/second), Total elapsed time(s), Average "
                                        + "latency "
                                        +
                                        "per event (ms), Entire Average latency per event (ms), Total "
                                        + "number"
                                        + " of "
                                        +
                                        "events received (non-atomic)," + "AVG latency from start (90),"
                                        + "" + "AVG latency from start(95), " + "AVG latency from start "
                                        + "(99)," + "AVG latency in this "
                                        + "window(90)," + "AVG latency in this window(95),"
                                        + "AVG latency "
                                        + "in this window(99)");
                                fstream.write("\r\n");
                            }

                            //System.out.print(".");
                            fstream.write(
                                    (eventCountTotal / RECORD_WINDOW) + "," + ((eventCount * 1000) / value) + "," +
                                            ((eventCountTotal * 1000) / (currentTime - veryFirstTime)) + "," +
                                            ((currentTime - veryFirstTime) / 1000f) + "," + (timeSpent * 1.0
                                            / eventCount) +
                                            "," + ((totalTimeSpent * 1.0) / eventCountTotal) + "," +
                                            eventCountTotal + "," + histogram.getValueAtPercentile(90) + "," + histogram
                                            .getValueAtPercentile(95) + "," + histogram.getValueAtPercentile(99) + ","
                                            + "" + histogram2.getValueAtPercentile(90) + ","
                                            + "" + histogram2.getValueAtPercentile(95) + ","
                                            + "" + histogram2.getValueAtPercentile(99));

                            fstream.write("\r\n");
                            fstream.flush();
                            histogram2.reset();
                            startTime = System.currentTimeMillis();
                            eventCount = 0;
                            timeSpent = 0;

                            if (!exitFlag && ((currentTime - veryFirstTime) > totalExperimentDuration)) {
                                log.info("Exit flag set.");
                                //Need to filter the output file
                                setCompletedFlag(sequenceNumber);
                                exitFlag = true;
                            }

                        }

                    } catch (Exception e) {
                        log.error("Error while consuming event, " + e.getMessage(), e);
                    }

                    if (exitFlag) {
                        //Preprocess the collected benchmark data
                        preprocessPerformanceData();
                        log.info("Done the experiment. Exitting the benchmark.");
                    }
                    */
                }


            }
        });

        executionPlanRuntime.start();
        inputHandler = executionPlanRuntime.getInputHandler("inputStream");

//        while (!exitFlag) {
//            //dataLoader.shutdown();
//            //siddhiAppRuntime.shutdown();
//            try {
//                Thread.sleep(10 * 1000);
//            } catch (InterruptedException e) {
//                log.error("Thread interrupted. " + e.getMessage(), e);
//            }
//        }
        //Preprocess the collected benchmark data
//        preprocessPerformanceData();
        //Generate the report PDF
        //generateReport();
    }


    public void sendEvents(int eventCount) {

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        new Thread(new Runnable() {
            @Override
            public void run() {
                long counter = 0;
                Random randomGenerator = new Random(1233435);
                String streamId = "inputStream";
                long lastTime = System.currentTimeMillis();
                DecimalFormat decimalFormat = new DecimalFormat("#");
                int filteredEvents = 0;
                while (counter < eventCount) {
//                    boolean isPowerSaveEnabled = randomGenerator.nextBoolean();
//                    int sensorId = randomGenerator.nextInt();
//                    double longitude = randomGenerator.nextDouble();
//                    double latitude = randomGenerator.nextDouble();
                    float humidity = randomGenerator.nextFloat();
                    double sensorValue = randomGenerator.nextDouble();


//                  Stream sampling
//                    if (streamSampler.isAddable(counter)) {
                    try {
                        inputHandler.send(new Object[]{humidity, sensorValue});
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    filteredEvents++;
//                    }

//                    if ((counter > warmUpCount) && ((counter + 1) % elapsedCount == 0)) {
//
//                        long currentTime = System.currentTimeMillis();
//                        long elapsedTime = currentTime - lastTime;
//                        double throughputPerSecond = (((double) elapsedCount) / elapsedTime) * 1000;
//                        lastTime = currentTime;
//                        log.info("Sent " + elapsedCount + " sensor events in " + elapsedTime
//                                + " milliseconds with total throughput of " + decimalFormat.format(throughputPerSecond)
//                                + " events per second.");
//                    }

//                    FeedbackProcessor.getInstance().incrementTotalEvents();
                    counter++;
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                System.out.println("EVENT SENDING FINISHED.................");
                System.out.println("TOTAL NO OF EVENTS : " + eventCount);
                System.out.println("FILTERED NO OF EVENTS : " + filteredEvents);

                executionPlanRuntime.shutdown();

            }
        }).start();
    }

    public synchronized void sendEvent(Object[] event) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    inputHandler.send(event);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    /**
     * Calculate the accuracy by observing execution plans and send the feedback to the client
     *
     * @param eventStreamName
     */
    public void sendFeedBack(String eventStreamName) {
        this.eventStreamName = eventStreamName;
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Siddhi Feedback running.................");
//      must be calculated
                double accuaracy = 0.7;
                FeedBack feedBack = new FeedBack(eventStreamName, accuaracy,
                        null, false, null);
                new TCPClient(Constants.TCP_HOST, Constants.TCP_PORT).sendMsg(
                        "ACCURACY_FEEDBACK:" + feedBack.toString());
            }
        }).start();
    }
}
