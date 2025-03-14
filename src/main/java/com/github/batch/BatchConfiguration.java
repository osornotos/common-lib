package com.github.batch;

import com.github.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.*;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.github.constant.CommonConstant.CHUNK_SIZE;
import static com.github.constant.CommonConstant.TOTAL_SIZE;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
@Slf4j
public class BatchConfiguration {
    private final JobRepository jobRepository;

    private final PlatformTransactionManager transactionManager;
    private final JobService jobService;

    // Define your item reader (source of data)
    @Bean
    public ItemReader<Integer> reader() {
        List<Integer> items = new ArrayList<>();
        for (int i = 0; i < TOTAL_SIZE; i++) {
            items.add(i);
        }
        // Create a delegate that implements ItemStreamReader
        ItemStreamReader<Integer> delegate = new ItemStreamReader<>() {
            private Iterator<Integer> iterator;

            @Override
            public void open(ExecutionContext executionContext) {
                iterator = items.iterator();
            }

            @Override
            public Integer read() {
                if (iterator != null && iterator.hasNext()) {
                    return iterator.next();
                }
                return null;
            }

            @Override
            public void update(ExecutionContext executionContext) {
                // No need to update state
            }

            @Override
            public void close() {
                // No resources to close
            }
        };

        SynchronizedItemStreamReader<Integer> synchronizedReader = new SynchronizedItemStreamReader<>();
        synchronizedReader.setDelegate(delegate);

        return synchronizedReader;
    }

    // Define your item processor (business logic)
    @Bean
    public ItemProcessor<Integer, Integer> processor() {
        return item -> item;
    }

    // Define your item writer (destination)
    @Bean
    public ItemWriter<Integer> writer() {
        return jobService::doSomething;
    }

    // Define the job step
    @Bean
    public Step step1() {
        return new StepBuilder("step1", jobRepository)
                .<Integer, Integer>chunk(CHUNK_SIZE, transactionManager)  // Process 2 items at a time
                .reader(reader())
                .processor(processor())
                .writer(writer())
//                .taskExecutor(batchProcessingTaskExecutor())
                .taskExecutor(batchProcessingTaskExecutor())
                .build();
    }

    // Define the job
    @Bean
    public Job processJob() {
        return new JobBuilder("processJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(step1())
                .end()
                .build();
    }

    // Optional: Add a job completion listener
    @Bean
    public JobExecutionListener listener() {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                System.out.println("Job started at: " + jobExecution.getStartTime());
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                System.out.println("Job finished at: " + jobExecution.getEndTime());
                System.out.println("Status: " + jobExecution.getStatus());
            }
        };
    }

    // Optional: Configure a custom job launcher
    @Bean
    @Primary
    public JobLauncher jobLauncher(JobRepository jobRepository) throws Exception {
        TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
        launcher.setJobRepository(jobRepository);

        // Configure async execution with thread pool
        launcher.setTaskExecutor(batchProcessingTaskExecutor());
        launcher.afterPropertiesSet();
        return launcher;
    }

    @Bean
    public ThreadPoolTaskExecutor batchProcessingTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(20);
        executor.setThreadNamePrefix("Batch-Thread-");
        executor.initialize();
        return executor;
    }

//    @Bean
//    public PlatformTransactionManager transactionManager() {
//        return new JpaTransactionManager();
//    }
}
