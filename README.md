#


## Overview
Data ingestion framework that has generic routines and procedure build on top of apache spark for ingesting data into data lake,

## Arguments
       The system accepts three argument in the below format
       <region>
       <data_source_id>
       <partition Location>
       
       Example : dev zeus 2020_7_23
       
       
## Main Class
    com.arjun.driver.Driver

## Prerequisitee

    - Create sql tables form metdata and batch framework
            DDLs can be found under 
            src\main\resource\ddl
            
    - Feed in approroate details in the tables for the source . Please refer data model
            
     -Modify the sql connection parameter before build
            Class : com.arjun.sqlconnection.SqlConnection
         
      - perform Maven build
            mvn package  

## Data model

TBD

## Maven Build
    mvn package
