You have limited time to finish the one task using

python, django, drf, postgres-sql, redis, celery

the entire system is build as multi-tenant jobs processing services

consider that we are logically manage the tenant by one tenant master table and refernaces to other tables by foriegn key

the catch is to

manage job with the priority from 1 to 5 (1 is highest, 5 is lowest)
each tenant only execute 2 jobs at a time, rest of waiting to get queued
job have multiple status (wait, executed, completed successfully, fail, retried when appropriate, cancelled when appropriate)
maintain duplicate requests for scheduling the job during (network retries, timeouts, user retry, proxy retry)
Retry behaviour should be configurable for attempt maximum retry counts
there are temporary failure and final failure are also there, after attempting max retry consider as final failure and store to another table to manage dead letter queue

This michenism is required to developed not just limited to the system, need to consideration of

sometime app layer failed or restarted, django / celery worker failed or restarted, database failed or restarted but system should be operational
also consider that enterprise level where tons of tenants are there and millions of jobs are there, infra should be operational

there are several things may comes up during testing so required to generate those test cases as well to test every single scenario

consider one more important scenarios where few jobs are overlapping each other need to maintain and generate test cases for the same.

type of job could be

reports
data processing
synchronization
AI processing
notification
or any other type of operation

since we need to simulate the job consider non of these jobs could be process with the same time taken. few of may take seconds, few of may take miniutes

since is very huge requirements we go one by one, and make sure codebase or description must be humanized to can be maintain easily, define proper comments, consider dynamic programing and clean architecture, use SOLID principal, required design patterns as well, but do not generate code too much it self to create complexity at debugging level and management level

we will start the way

system design
database design
concurrancy
failure managment
django api structure
scalebility section
documentation

we have to develop everything locally and present to CTO locally, there are major questions are comming to use to be get explained we have to be ready

assume few scenarios at your level