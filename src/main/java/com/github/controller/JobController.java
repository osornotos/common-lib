package com.github.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/jobs")
public class JobController {
    private final JobLauncher jobLauncher;
    private final JobRepository jobRepository;

    @Qualifier("processJob")
    private final Job job;

    @PostMapping("/run")
    public ResponseEntity<Object> run() {
        List<String> jobNames = jobRepository.getJobNames();
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();
        try {
            JobExecution execution = jobLauncher.run(job, jobParameters);
            log.info("Job Execution Status: {}", execution.getStatus());
        } catch (Exception e) {
            log.error(ExceptionUtils.getMessage(e));
        }
        return ResponseEntity.ok().build();
    }
}
