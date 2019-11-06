package com.sree.parallel;

import org.apache.log4j.Logger;

public class Processor<I> {
    final static Logger logger = Logger.getLogger(Processor.class);
    public Chunk<?> process(Chunk<I> chunk){
        logger.info("Processing "+chunk);
        Chunk<I> processed = new Chunk<I>();
        processed.setData(chunk.getData());
        return processed;
    }
}
