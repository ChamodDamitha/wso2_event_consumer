package org.wso2.carbon.sample.performance.feedback;

import org.wso2.carbon.sample.performance.feedback.Constants;
import org.wso2.carbon.sample.performance.feedback.TCPClient;
import org.wso2.siddhi.core.*;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.event.Event;

import java.awt.*;
import java.text.DecimalFormat;
import java.util.Random;

/**
 * Created by chamod on 8/28/17.
 */
public class SiddhiHandler {
    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;
    private String eventStreamName;
    private InputHandler inputHandler;

    public SiddhiHandler() {
        siddhiManager = new SiddhiManager();
        createExecutionPlan();
    }


    private void createExecutionPlan() {

        String definition = "@config(async = 'true') " +
                "define stream inputStream (humidity float, sensorValue double, timestamp long);";

        String query = "@info(name = 'query1') from inputStream#window.timeBatch(1000)  " +
                "select avg(humidity) as avgHumidity, avg(timestamp) as timestamp " +
                "insert into outputStream ;";

        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(definition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
                    @Override
                    public void receive(Event[] events) {
//                        System.out.println("event received..........................");
//                        EventPrinter.print(events);
//                        for (Event event : events) {
////                            new TCPClient(Constants.TCP_HOST, Constants.TCP_PORT).sendMsg(
////                                    "FEEDBACK FROM CONSUMER : SiddhiHandler : " + event.toString());
//                        }
                        for(Event e : events) {
                            System.out.println("Event: avgHumidity:" + e.getData()[0]
                                    + ", timestamp : " + e.getData()[1]);
                        }
                    }
                }
        );
        executionPlanRuntime.start();
        inputHandler = executionPlanRuntime.getInputHandler("inputStream");

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

    public void sendEvent(Object[] event) {
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
