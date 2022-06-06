# Project 2 - Data Modelling with Apache Cassandra
*Tobias Tröger, February 2022*

![](images/illu_cassandra_blog-147.png)

## 1. Project Description

The startup Sparkify wants to gain deeper insights into the user behavior of their subscribers.
As a prospective data engineer, my task is to model the available data using Apache Cassandra according to the specifications of the analysis team.
   
## 2. ETL Process  
  
Since there are no joins in Apache Cassandra, a new table is designed from the normalized data for each question.
The corresponding queries can be found in the `Project_1B_ Project_Template.ipynb`file.
  
## 3. Files

*Project_1B_ Project_Template.ipynb* - Notebook to set up the Cassandra nodes and run the queries against the normalized table

*event_datafile_new.csv* - normalized table -> derived from the data in the data_log folder



