package com.sree.parallel;

import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

public class ProcessorTask<I> implements Callable<Chunk<?>> {
    static final Logger logger = Logger.getLogger(ProcessorTask.class);
    Chunk<I> chunk;

    public ProcessorTask(Chunk<I> chunk) {
        this.chunk = chunk;
    }

    @Override
    public Chunk<?> call() throws Exception {
        Chunk<?> processed = chunk.getProcessor().process(chunk);
        logger.info("Chunk has been processed");
        chunk.getOutboundQueue().put(processed);
        logger.info("Processed chunk has been placed into outbound queue");
        return processed;
    }
}
