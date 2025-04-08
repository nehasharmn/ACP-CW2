package uk.ac.ed.acp.cw2.model;

import lombok.Data;

@Data
public class ProcessMessagesRequest {
    private String readTopic;
    private String writeQueueGood;
    private String writeQueueBad;
    private int messageCount;
}