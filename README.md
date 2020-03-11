# DATAWEREHOUSE FOR SONGPLAYS ANALYSIS

## Summary 
This project involves the data modeling of an OLAP database from the song and event datasets, they are modelled with this architecture in order to perform heavy analitycal queries. To achieve this we first choose to use a non-relational schema in the staging phase and a start schema in the delivery phase, because it makes it easy for bussiness user to make analisys in this type of schema, after that we choose the infraestructure which is a cluster in aws redshift  with 2 nodes of 160GB of ssd each. 

## How to run
To execute the project you first will need to create an AWS redshift cluster with minimum 2 nodes, after that you will need to fill the data asked in the dwh.cfg file in the sections of "CLUSTER","IAM_ROLE","AWS" and "DBTEST" the data of the "DBTEST" section should be filled with values without quotas. secondly you have to make sure you have python 2.7 or latest installed in your computer, after that you have to go to a terminal, navegate to create_tables.py file in the project run "python create_tables.py" the execution of this command will create all the structures of the staging and delivary tables, after executing the previus command is time to execute the ETL script that will populate our tables, for this we have to navegate to the etl.py file and execute `python etl.py` to fill the tables, finally to make sure everything is rigth we should lunch jupyter notebooks and go to the test.ipynb and execute the entire 
notebook. 

## Files

### create_tables.py 
This file represent the commands that need to be executed in order to create the table structure

### dwh.cfg
This is a configuration file where all  the properties are going to be set. 

### etl.py
This file represent the commands that need to be executed in order to create the ETL operations.

### sql_queries.py
This file represent the sql commands itselft needed to create the tables structure and perform the etl process.

### test.ipynb
This is a file containing the analytical queries needed to proof the correct execution of the ETL.

   
