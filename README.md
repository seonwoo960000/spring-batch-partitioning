# Spring Boot 3 + Batch with Partitioning 
- Use `sql/ddl.sql` to create tables 
- Use `sql/dml.sql` to create dummy data 
- Set startDate and endDate as jobParameters e.g. `startDate=2023-01-01 endDate=2023-12-30`
- Set chunk size using `spring.batch.job.chunk-size` property
- Set pool size for thread pool which is used in partitioning by using `spring.batch.job.thread-pool-size` property
- Test job by using `ProductMonthlyAggregationJobTest`
