# Code tips

I wrote the tips or solutions on a variety of problems I encounted while learning this course.

## How to remove the file also from git history

It's not ideal circumstance, but in case you make mistake!

```bash
git filter-branch --index-filter 'git rm -rf --cached --ignore-unmatch path_to_file' HEAD
```

```bash
git push --force
```

## Storing Unix timestamp or Datetime in Data Warehouse

When dealing with date data stored as Unix timestamps in a data warehouse, there are a few considerations to keep in mind.

Storing the data as Unix timestamps in the data warehouse has the advantage of being more storage-efficient, as timestamps are represented as integers. This can be beneficial if you have a large volume of data and want to optimize storage space.

However, querying and analyzing data in Unix timestamp format can be less intuitive and require additional conversion steps. If you frequently need to perform date-based operations, such as filtering or aggregating data based on specific dates or time ranges, it may be more convenient to convert the Unix timestamps to a datetime data type during the ETL process and store them as such in the data warehouse.

By converting the Unix timestamps to datetime data type during the ETL process, you can take advantage of the built-in date functions and operators provided by the database system. This can simplify your queries and make them more readable and easier to understand.

Ultimately, the decision depends on your specific use case and requirements. If storage efficiency is a priority and you don't need to perform many date-based operations, storing the data as Unix timestamps may be a good choice. On the other hand, if you frequently work with dates and need to perform complex date-based queries, converting the timestamps to datetime during the ETL process can make your queries more straightforward.

## How to convert unix timestamp to timestamp in Redshift

```sql
SELECT 
timestamp with time zone 'epoch' + [column_with_unix_timestamp] * interval '1 second'
FROM [relevant_table]
```

### Redshift Error:  Check 'stl_load_errors' system table for details

```python
query = """select  starttime, raw_line, err_reason
from stl_load_errors
order by starttime desc
limit 5;
"""
%sql $query
```

### Redshift Error: Changing Data Type of The Column

```sql
ALTER TABLE [table_name] drop column [column_name];
ALTER TABLE [table_name] add column [column_name] [data_type];
```

Somehow, the following code didn't work.

```sql
ALTER TABLE [table_name] ALTER column [column_name] TYPE [data_type];
```

## How to zip files excluding some files

Suppose you want to exclude config files, you can run command on terminal as follows.

```bash
zip [zip_file_name].zip * -x "*.cfg"
```

Suppose you don't include subfolders in zip files

```bash
cd [folder directory to zip] && zip [zip_file_name].zip *
```

## Glue Job Bookmarks

It's the function in AWS Glue that prevents reprocessing of already-processedd data.
Job bookmarks help AWS Glue maintain state information and prevent the reprocessing of old data. With job bookmarks, you can process new data when rerunning on a scheduled interval.
[Read Detail](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuations.html)

## Cli Command to Remove All Objects in AWS S3 bucket

```bash
aws s3 rm s3://bucket-name --recursive
```
