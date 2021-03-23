package org.jlab.alarms;

public class FilterCommandKey {
    private String outputTopic;

    public FilterCommandKey() {

    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public String toString() {
        return "{\"outputTopic\": \"" + outputTopic + "\"}";
    }
}
