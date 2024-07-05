# Final-project
## About

This project is providing a service to *Super Cafe* in which we, **Nubi**, will Extract, Transform and Load data provided by the client into a database. Next...

## Elevator pitch:
*FOR* the super cafe owner 

*WHO* need their data data organised by:
* Sales
* Performance
* Products
* Stores

*THE* NUBI service 

*IS* a data engineering service 

*THAT* creates structure, organisation and security to build clear reports using data visualisations

*UNLIKE* manual processing of data and having separate disjointed forms of data

*OUR SERVICE* automates data organisation and visualisation offering real time insights into sales and operations. 
Our service ensures customer privacy (names, card number, etc.)


## Team Members

* Haaris H
* Byron S
* Taiwo A
* Mujtaba M

## Database Schema Model

![database_schema_model](database_schema_model.png)


# Sprint 1 - ETL

In this sprint we complete the first stages of our project. By implementing ETL (Extract, Transform and Load) we stored data provided by the client into a Postgres SQL database.

To run ETL using files provided:

 1. Copy and Paste "Nubi_setup.sql" into adminer
 2. Run load.py

# Sprint 2

 In this sprint we have set up an S3 bucket to store our data. We then ran a Lambda function which consisted of a modfified version of our ETL from last sprint to run the process in the cloud. This ETL loads our data into Amazon Redshift where client data will be stored.

 To run ETL in AWS:

 1. Create S3 bcuket and Lambda function (Configuration details followed from class exercise)
 2. Paste Nubi_Redshift_Setup.sql into Redshift query to setup databse tables
 3. Run modified ETL function via Lambda to Extract from S3, Transform and Load into Redshift
 4. Data should appear in Redshift...