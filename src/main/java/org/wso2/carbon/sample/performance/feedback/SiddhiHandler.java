package org.wso2.carbon.sample.performance.feedback;

import org.wso2.carbon.sample.performance.feedback.Constants;
import org.wso2.carbon.sample.performance.feedback.TCPClient;
import org.wso2.siddhi.core.*;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.awt.*;

/**
 * Created by chamod on 8/28/17.
 */
public class SiddhiHandler extends Thread {
    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;

    public SiddhiHandler() {
        siddhiManager = new SiddhiManager();
        createExecutionPlan();
    }

    @Override
    public void run() {
        System.out.println("Siddhi Handler running.................");
        sendEvents();
    }

    private void createExecutionPlan() {

        String definition = "@config(async = 'true') " +
                "define stream cseEventStream (symbol string, price float, volume long);";

        String query = "@info(name = 'query1') from cseEventStream#window.timeBatch(500)  " +
                "select symbol, sum(price) as price, sum(volume) as volume group by symbol " +
                "insert into outputStream ;";

        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(definition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
                    @Override
                    public void receive(Event[] events) {
                        EventPrinter.print(events);
                        for (Event event : events) {
                            new TCPClient(Constants.TCP_HOST, Constants.TCP_PORT).sendMsg(
                                    "FEEDBACK FROM CONSUMER : SiddhiHandler : " + event.toString());
                        }
                    }
                }
        );
        executionPlanRuntime.start();

    }

    public void sendEvents() {
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        try {
            inputHandler.send(new Object[]{"ABC", 700f, 100l});
            inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
            inputHandler.send(new Object[]{"DEF", 700f, 100l});
            inputHandler.send(new Object[]{"ABC", 700f, 100l});
            inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
            inputHandler.send(new Object[]{"DEF", 700f, 100l});
            inputHandler.send(new Object[]{"ABC", 700f, 100l});
            inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
            inputHandler.send(new Object[]{"DEF", 700f, 100l});
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executionPlanRuntime.shutdown();
    }


}
