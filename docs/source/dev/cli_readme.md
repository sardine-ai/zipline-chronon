# Command line interface

## Plan

Describes the logical changes in your current repository versus the remote state for the target branch.

For example, if you add a column to a `GroupBy` and call `plan`, you'll see that change reflected as a column addition. If you then run `backfill` for the `GroupBy`, then call `plan` again, you would see no change to your `GroupBy` `backfill` plan, however, there would still be a column addition reflected in your `upload` plan.

`plan` is also always run against a particular environment. By default it is the branch which you are authoring on, however, it can also be set to `prod` in which case it will display the plan of your local changes against the prod environment, reflecting what would happen should you merge and deploy your change.

Note that `plan` does not fully describe any particular computation because it is agnostic of date ranges and other computation-specific arguments. To see a full compute plan for a given `backfill`, for example, you can call the `backfill` command with the `--plan` argument.

Usage:

```sh
zipline plan [OPTIONS]
```

Options:

```sh
--branch         the branch against which to compare the local state. Can be set to `prod` or a different `branch_id`. Defaults to the current branch if one is set, else `prod`.
```

## Backfill

Runs a backfill for the specified entity and date range. This produces computed values for the `Entity`s defined transformation. Commonly used with `Join` to produce point-in-time correct historical data of feature values. For `GroupBy`s it produces snapshots of values as of some boundary (usually midnight on each day in the range), and for `StagingQuery` it simply runs the provided SQL to produce output in the range.

Usage:

```sh
zipline backfill ENTITY_ID [OPTIONS]
```

- Valid entity types: `GroupBy`, `Join`, `StagingQuery`

Options:

```sh
--branch              the branch to backfill into. Defaults to the current branch if one is set, otherwise is a required argument. If set to `prod` then it will overwrite production tables - use with caution.
--start               start date for which you want data backfilled for this entity. Defaults to the configured start date.
--end                 end date for which you want data backfilled for this entity. Defaults to today - 2 days.
--force-recompute     recomputes the backfill for the specified entity, even if the date range is already present. For Joins, also recomputes all join_part intermediate tables. Defaults to false.
--info                visualizes the computation entailed in this backfill, but does not start compute. Useful to sanity check a job prior to running. If everything looks ok, then rerun the command but omit this flag to begin the job.
```

## Deploy

Populates the online serving index with data for the specified `Entity`. If run for a `Join`, it will run for all of the `GroupBy`s included in that join, as well as run the `Join` metadata upload job, which is required for fetching data for the `Join`.

For batch `GroupBy`s, this command will execute a batch upload. For streaming `GroupBy`s it will execute a batch upload and commence a streaming job.

After calling `Deploy` for any `Entity`, you can then call `fetch` to get values once the `Deploy` jobs are successful.

Usage:

```sh
zipline deploy ENTITY_ID [OPTIONS]
```

- Valid entity types: `GroupBy`, `Join`

Options:

```sh
--ds        The date to use for the batch upload. Defaults to 
--stream    Only applies to `GroupBy`s that use a streaming source. Runs the streaming job after the batch upload completes (or only runs the streaming job if the batch upload is already completed for the given `ds`).
--info      visualizes the computation entailed in this upload, but does not start compute. Useful to sanity check a job prior to running. If everything looks ok, then rerun the command but omit this flag to begin the job.
```


## Fetch

Fetches data for the given `Entity` and keys. Useful for testing online serving.

Usage:
```sh
zipline upload ENTITY_ID [OPTIONS]
```

- Valid entity types: `GroupBy`, `Join`

Options:

```sh
--keys    the keys to use in the fetch request, map of key name to value. Required argument.
```

## Info

Provides information about a given `Entity`, including upstream/downstream lineage and schema informatio.

Usage:
```sh
zipline info ENTITY_ID
```

- Valid entity types: `GroupBy`, `Join`, `StagingQuery`
