package com.sree.parallel;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

public class QueueManager<I> {

    static final Logger logger = Logger.getLogger(QueueManager.class);

    int numberOfProcessors=4;
    ArrayBlockingQueue outboundQueue = null;
    List<ArrayBlockingQueue<Chunk<I>>> inboundQueues = new ArrayList<>();

    Map<String, ArrayBlockingQueue<Chunk<?>>> outboundQueueMap = new HashMap<>();
    Map<String, CompletionService<Chunk<?>>> completionServiceMap = new HashMap<>();
    Map<String, ExecutorService> executorServiceMap = new HashMap<>();

    boolean shutdown = false;

    public QueueManager(){
        this.numberOfProcessors = 4;
        for(int i=0;i<numberOfProcessors;i++) {
            ArrayBlockingQueue inboundQueue = new ArrayBlockingQueue<>(1000);
            inboundQueues.add(inboundQueue);
            ExecutorService executorService = Executors.newFixedThreadPool(30);
            CompletionService<Chunk<?>> completionService = new ExecutorCompletionService<>(executorService);
            String executorServiceName = "completionService-"+i;
            executorServiceMap.put(executorServiceName,executorService);
            String completionServiceName = "completionService-"+i;
            completionServiceMap.put(completionServiceName, completionService );
            startInboundReader(inboundQueue, executorServiceName, completionServiceName);
        }
    }

    public void shutdown(){
        shutdown = true;
    }

    private void startInboundReader(ArrayBlockingQueue<Chunk<I>> inboundQueue, String executorServiceName, String completionServiceName){
        new Thread(new Runnable() {
            @Override
            public void run() {
                CompletionService<Chunk<?>> completionService = completionServiceMap.get(completionServiceName);
                while (!shutdown){
                    try {
                        Chunk<I> chunk = inboundQueue.poll(2, TimeUnit.SECONDS);
                        if(chunk!=null) {
                            ProcessorTask task = new ProcessorTask(chunk);
                            completionService.submit(task);
                        }
                    } catch (InterruptedException e) {
                        logger.error("Error while reading inbound queue",e);
                    }
                }
                logger.info("Stopped reading inbound queue - "+completionServiceName);
                executorServiceMap.get(executorServiceName).shutdown();
            }
        }).start();
    }

    public QueueManager(int numberOfProcessors){
        this.numberOfProcessors = numberOfProcessors;
    }

    public int submitJob(Collection<I> allData, int chunkSize, String requestId, Processor<I> processor){
        ArrayBlockingQueue<Chunk<?>> outboundQueue = new ArrayBlockingQueue<Chunk<?>>(2000);
        outboundQueueMap.put(requestId,outboundQueue);
        Iterator<I> iterator = allData.iterator();

        Chunk<I> chunk = createChunk(processor,outboundQueue);

        int numberOfChunk=0;

        int submitIndex=0;
        while(iterator.hasNext()){
            chunk.addItem(iterator.next());
            if(chunk.getDataSize()==chunkSize){
                submitChunk(chunk,submitIndex);
                numberOfChunk++;
                submitIndex = resetSubmitIndex(submitIndex);
                if(iterator.hasNext()) {
                    chunk = createChunk(processor,outboundQueue);
                }
            }
        }

        if(chunk.getDataSize()>0){
            submitChunk(chunk,submitIndex);
            numberOfChunk++;
        }
        return numberOfChunk;
    }

    private Chunk<I> createChunk(Processor<I> processor, ArrayBlockingQueue<Chunk<?>> outboundQueue){
        Chunk<I> chunk = new Chunk<>();
        chunk.setProcessor(processor);
        chunk.setOutboundQueue(outboundQueue);
        return chunk;
    }

    private void submitChunk(Chunk<I> chunk, int submitIndex){
        try {
            logger.info("Submitting the chunk in the inbound queue = "+submitIndex);
            getInboundQueueToSubmitChunk(submitIndex).put(chunk);
        } catch (InterruptedException e) {
            logger.error("Error while submitting chunk into inbound queue, submitIndex = "+submitIndex);
        }
    }

    private int resetSubmitIndex(int submitIndex){
        submitIndex++;
        if(((submitIndex) % numberOfProcessors) == 0 ){
            submitIndex = 0;
        }
        return submitIndex;
    }

    private ArrayBlockingQueue<Chunk<I>> getInboundQueueToSubmitChunk(int index) {
        return inboundQueues.get(index);
    }

    public ArrayBlockingQueue<Chunk<?>> getOutboundQueue(String requestId){
        return outboundQueueMap.get(requestId);
    }
}
