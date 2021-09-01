# Cron jobs

This service has cron jobs that should be executed regularly. These are detailed below, along with recommended schedules. 

## Cleanup job

The cleanup job will delete any outbox records that have been successfully published more than 1 hour ago.

To run this job manually, in your dev environment you can run

    $ docker-compose exec app /go/bin/app --cleanup

When deployed, this cron should be executed by Kubernetes' scheduler. It is recommended that this is run hourly.

### Running the database optimize job

For both MySQL and Postgres, after records are deleted by the cleanup job above, disk space is not always released for new records. In order to free up the disk space, we need to run an optimization on the database. In MySQL this is `OPTIMIZE`, and in Postgres this is `VACUUM`.

You can run these manually, in your local dev environment:

    $ docker-compose exec app /go/bin/app --optimize

When executed in this way, the application will determine which optimization to run based on the configured database driver.

>_NOTE: If you have [routine vacuuming] enabled on Postgres then you do not need to run this job._

[routine vacuuming]: https://www.postgresql.org/docs/9.5/routine-vacuuming.html
