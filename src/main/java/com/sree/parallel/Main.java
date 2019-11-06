package com.sree.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Main {
    public static void main(String[] args) {
        Processor<Integer> processor = new Processor<>();
        QueueManager<Integer> queueManager = new QueueManager<>();

        List<Integer> data = new ArrayList<>();
        for(int i=0;i<1000000;i++){
            data.add(i);
        }

        int numberOfChunks = queueManager.submitJob(data,10,"Test", processor);
        ArrayBlockingQueue<Chunk<?>> outboundQueue = queueManager.getOutboundQueue("Test");

        for(int i=0;i<numberOfChunks;i++){
            try {
                Chunk<?> processed = outboundQueue.take();
                System.out.println("Processed = "+processed.getData());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        queueManager.shutdown();
    }
}
