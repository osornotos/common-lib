package com.github.controller;

import com.github.service.JobService;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.springframework.batch.item.Chunk;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.github.constant.CommonConstant.CHUNK_SIZE;
import static com.github.constant.CommonConstant.TOTAL_SIZE;

@ExtendWith(SpringExtension.class)
class JobControllerTest {

    @InjectMocks
    private JobService jobService;

    @Test
    void testDoSomething() {
        var executor = Executors.newFixedThreadPool(4);

        List<Integer> items = new ArrayList<>();
        for (int i = 0; i < TOTAL_SIZE; i++) {
            items.add(i);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<List<Integer>> partition = Lists.partition(items, CHUNK_SIZE);
        for (List<Integer> integers : partition) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                Chunk<Integer> chunk = new Chunk<>(integers);
                jobService.doSomething(chunk);
            }, executor);
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
}
