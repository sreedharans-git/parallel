package com.sree.parallel;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class QueueManager<I> {

    static final Logger logger = Logger.getLogger(QueueManager.class);

    int numberOfProcessors=4;
    ArrayBlockingQueue outboundQueue = null;
    List<ArrayBlockingQueue<Chunk<I>>> inboundQueues = new ArrayList<>();

    public QueueManager(){
        this.numberOfProcessors = 4;
        this.outboundQueue = new ArrayBlockingQueue(2000);
    }

    public QueueManager(int numberOfProcessors){
        this.numberOfProcessors = numberOfProcessors;
    }

    public void addInboundQueue(String queueName){

    }

    public void addOutboundQueue(String queueName){

    }

    public void submitJob(Collection<I> allData, int chunkSize){
        Iterator<I> iterator = allData.iterator();
        Chunk<I> chunk = new Chunk<>();
        int submitIndex=0;
        while(iterator.hasNext()){
            chunk.addItem(iterator.next());
            if(chunk.getDataSize()==chunkSize){
                submitChunk(chunk,submitIndex);
                submitIndex = resetSubmitIndex(submitIndex);
                if(iterator.hasNext()) {
                    chunk = new Chunk<>();
                }
            }
        }

        if(chunk.getDataSize()>0){
            submitChunk(chunk,submitIndex);
        }
    }

    private void submitChunk(Chunk<I> chunk, int submitIndex){
        try {
            getInboundQueueToSubmitChunk(submitIndex).put(chunk);
        } catch (InterruptedException e) {
            logger.error("Error while submitting chunk into inbound queue, submitIndex = "+submitIndex);
        }
    }

    private int resetSubmitIndex(int submitIndex){
        submitIndex++;
        if(((submitIndex+1) % numberOfProcessors) == 0 ){
            submitIndex = 0;
        }
        return submitIndex;
    }

    private ArrayBlockingQueue<Chunk<I>> getInboundQueueToSubmitChunk(int index) {
        if(inboundQueues==null || inboundQueues.size()==0 || inboundQueues.size()<numberOfProcessors ){
            inboundQueues.add(new ArrayBlockingQueue<>(1000));
        }
        return inboundQueues.get(index);
    }
}
