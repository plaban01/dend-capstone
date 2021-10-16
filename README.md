# Data Engineering Capstone Project

In this project we will be analyzing the I94 immigration data from US National Tourism and Trade Office for immigrants in the United States. The high level idea of the project is to gather data from various sources and clean and transform them and load them into an analytics data model which could be used to analyse different patterns about immigrants.

The project follows the follow steps:

* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

## Step 1: Scope the Project and Gather Data
Following are the datasets used in the project:

* I94 Immigration Data: This data comes from the US National Tourism and Trade Office. [Link](https://travel.trade.gov/research/reports/i94/historical/2016.html)
* U.S. City Demographic Data: This data comes from OpenSoft. [Link](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
* Airport Code Table: This is a simple table of airport codes and corresponding cities. [Link](https://datahub.io/core/airport-codes#data)
* World Temperature Data: This dataset came from Kaggle. [Link](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)

The data dictionary for the I94 immigration data is also provided as input (I94_SAS_Labels_Descriptions.SAS)

### Architecture and tools used
![image](https://user-images.githubusercontent.com/4925968/137584666-7ddc4647-b129-4385-a70b-afeb1ab4c24f.png)

## Step 2: Explore and Assess the Data

## Step 3: Define the Data Model
The data is stored as star schema in Amazon Redshift. There are two fact tables and associated dimension tables. fact_immigration is the main fact table. Demographics is also modeled as fact as demographics information and can be aggregated (e.g. population count etc). Below is the ER diagram of the data model:
![image](https://user-images.githubusercontent.com/4925968/137584721-274cecac-2de1-4b10-bc5d-045da9434e91.png)
