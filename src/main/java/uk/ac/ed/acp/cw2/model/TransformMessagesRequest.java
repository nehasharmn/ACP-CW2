package uk.ac.ed.acp.cw2.model;

import lombok.Data;

@Data
public class TransformMessagesRequest {
    private String readQueue;
    private String writeQueue;
    private int messageCount;
}