# Information on and Guide to Common Issues for Weeky Parsers in Airflow

## General information on Airflow
Airflow is a platform that allows us to run, manage, and visualize our data pipelines. Within Airflow, we have **DAG**s (directed acyclic graphs) that are comprised of many tasks that run in a particular order - most tasks will either create, move, combine, delete, or QC data. <br>
You can find the Python code for all DAGs on the PatentsView GitHub [here](https://github.com/PatentsView/PatentsView-DB/tree/master/airflow/dags).

## Understanding and Running a DAG
When you click on a DAG, you will see an overview page for it. On the left side of the page, you will see a grid of squares.
This grid is telling you the current status of every task for the past 25 or so weeks. Each **column** is a specific week (or "run") of the DAG, and each **row** is a particular task that takes place during the run. <br>
<br>
The color of a task shows the status of a given task for a given week. 
* **White** means the task hasn't started yet
* **Light green** means the task is in progress
* **Dark green** means the task succeeded
* **Red** means the task has failed
* **Yellow** means the task cannot run because a previous task that it relies on has failed
<br>
If you click on one square in the grid, you'll see details and options for that task. 
If you click on *Log* (in between *Rendered Template* and *XCom*) you'll see the log messages for that task. **These are important to check to find out why a task failed.** The error messages in logs will tell you a lot about why a certain task failed, and where to begin degugging, if need be. <br>
<br>
There is also the **Graph**, which visualizes the order in which tasks are completed for a given DAG. Each task executes in a given order, starting on the left. If you look at a task in the middle, then all tasks that come before it are *upstream*, and those after it are *downstream*.
Note that the graph view will always default to showing you the most recent run of the DAG. <br>
<br>
If you want to try running a task again, then go back to the grid view, click on the task and respective week you want to rerun, and click *Clear* with *Downstream* and *Recursive* selected. Airflow will give you a warning message about the tasks you're about to clear to confirm.

## Logging Changes and Fixes
When a task fails, you may have to fix data in a table. When you do, it's important to make note of these changes so others can see what went wrong and what you changed. We keep text files outlining our changes in [this GitHub](https://github.com/PatentsView/daily-analysis-logs/tree/main). <br>
<br>
When you create a text file to note your changes, you may want to follow a consistent format, such as this: <br>
*qc_parse_text_data_correction.txt*
```
DAG: granted_patent_updater
Task: qc_parse_text_data
Week: 20230905

SQL query: 

SELECT count(*) FROM claims_2023 WHERE CHAR_LENGTH(`dependent`) != CHAR_LENGTH(TRIM(`dependent`))
Exception: upload_20230905.claims_2023.dependent needs trimming

1 row contains an extra space after its claim number.
uuid: '692cee48-4bcb-11ee-ae79-0a1e30f810d7'
dependent: 'claim 18 '

Updating dependent to remove trailing space.
```
It's best to write your text file as you work in MySQL. Once you're finished running any queries you've written, go back to the DAG in Airflow and rerun the task that failed. You may have to make more changes if the weeks fails at a different task - in which case, make a new text file for that task. <br>
<br>
Once all the tasks in a given week run successfully, you can upload your text files to the main branch in the daily-analysis-logs GitHub, into a new or existing folder that matches the week of the data.

# granted_patent_updater
## Description
This weekly parser handles new weekly data for patents. It runs every Tuesday at 9:00 UTC.

## Common issues

# pregrant_publication_updater_v2
## Description
This weekly parser handles new data for pregrant publications. It runs every Thursday at 9:00 UTC.

## Common issues
