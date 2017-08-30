package org.wso2.carbon.sample.performance.feedback;

/**
 * Created by chamod on 8/23/17.
 */
public class FeedBack {
    private String eventStreamName;
    private double accuracy;
    private String typeOfOperation;
    private boolean isPersisted;
    private String[] usedFields;


    public FeedBack(String eventStreamName, double accuracy, String typeOfOperation,
                    boolean isPersisted, String[] usedFields) {
        this.eventStreamName = eventStreamName;
        this.accuracy = accuracy;
        this.typeOfOperation = typeOfOperation;
        this.isPersisted = isPersisted;
        this.usedFields = usedFields;
    }

    public String getEventStreamName() {
        return eventStreamName;
    }

    public double getAccuracy() {
        return accuracy;
    }

    public String getTypeOfOperation() {
        return typeOfOperation;
    }

    public boolean isPersisted() {
        return isPersisted;
    }

    public String[] getUsedFields() {
        return usedFields;
    }

    @Override
    public String toString() {
        return "eventStreamName:" + eventStreamName + ",accuracy:" + accuracy;
    }
}
