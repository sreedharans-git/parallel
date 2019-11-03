package com.sree.parallel;

import java.util.ArrayList;
import java.util.Collection;

public class Chunk<I> {
    Collection<I> data;
    String inboundQueueName;
    String outboundQueueName;

    public void addItem(I item) {
        if (data == null) {
            data = new ArrayList<I>();
        }
        data.add(item);
    }

    public int getDataSize() {
        if (data == null) {
            return 0;
        }
        return data.size();
    }

    public String toString() {
        if (data == null) {
            return "";
        }
        return data.toString();
    }

    public Collection<I> getData() {
        return data;
    }

    public void setData(Collection<I> data) {
        this.data = data;
    }

    public void setOutboundQueueName(String outboundQueueName) {
        this.outboundQueueName = outboundQueueName;
    }

    public String getInboundQueueName() {
        return inboundQueueName;
    }

    public void setInboundQueueName(String inboundQueueName) {
        this.inboundQueueName = inboundQueueName;
    }

    public String getOutboundQueueName() {
        return outboundQueueName;
    }
}
