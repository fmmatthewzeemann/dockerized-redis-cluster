package com.fostermoore.redis.chaos.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

@Configuration
@EnableRetry
public class RetryConfig {
    // Retry configuration is enabled via @EnableRetry
    // Individual retry configurations are specified on service methods
}