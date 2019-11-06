package com.sree.parallel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

public class Chunk<I> {
    Collection<I> data;
    BlockingQueue<Chunk<?>> outboundQueue;
    Processor<I> processor;

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

    public Processor<I> getProcessor() {
        return processor;
    }

    public void setProcessor(Processor<I> processor) {
        this.processor = processor;
    }

    public BlockingQueue<Chunk<?>> getOutboundQueue() {
        return outboundQueue;
    }

    public void setOutboundQueue(BlockingQueue<Chunk<?>> outboundQueue) {
        this.outboundQueue = outboundQueue;
    }
}
