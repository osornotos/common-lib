package com.github.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.batch.item.Chunk;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class JobService {

    public void doSomething(Chunk<? extends Integer> chunk) {
        try {
            log.info("Processing chunk: {}", chunk.getItems());
            Thread.sleep(chunk.size());
        } catch (Exception ignored) {
            log.error(ExceptionUtils.getMessage(ignored));
        }
    }
}
