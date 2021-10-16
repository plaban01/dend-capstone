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
![image](https://user-images.githubusercontent.com/4925968/137590277-027170d0-07df-469f-90a3-e980fdd0b47d.png)

## Step 2: Explore and Assess the Data

The metadata associated with the various codes used in I94 immigration data is present in the data dictionary which needs to be parsed and stored in a format that can be processed. Two scripts are added to process and cleanup the data:

* parse_sas_data_labels.py - This script reads the I94 data dictionary and extracts the country, states and ports information and writes them to csv files for further processing. Three files are written - i94_countries.csv, i94_states.csv and i94_ports.csv
* cleanup_and_process.py - The data files (i94_countries.csv, i94_states.csv, i94_ports.csv, us-cities-demographics.csv, airport-codes_csv.csv, GlobalLandTemperaturesByCity.csv) are uploaded to S3. The immigration data is processed using Spark. Following columns are processed:
  * arrival_date - Extracts datetime format from SAS timestamp
  * departure_date - Extracts datetime format from SAS timestamp
  * year - Year of arrival
  * month - Month of arrival
  * day - Day of arrival
  * travel_mode - The travel modes (Air, Sea, Land) are extracted from the travel codes using the defintions provided in the data dictionary
  * visa_category - Visa categories (Business, Travel, Student) are extracted from the codes using the definitions provided in the data dictionary
  * i94cit, i94res, i94bir - Data types are converted to int

  The immigration data is uploaded to S3 in parquet files partitioned by year, month and day

## Step 3: Define the Data Model

### Conceptual Data Model
The data has been modeled as star schema for data warehouse in Amazon Redshift. There are two fact tables and associated dimension tables. fact_immigration is the main fact table. Demographics is also modeled as fact as demographics information and can be aggregated (e.g. population count etc). 

Below is the ER diagram of the data model:
![image](https://user-images.githubusercontent.com/4925968/137584721-274cecac-2de1-4b10-bc5d-045da9434e91.png)

The data model will allow data to be aggregated across the following dimensions:
* Year and month
* Country of the immigrants
* State and city
* Mode of travel, visa type

### Mapping out Data Pipelines
Following steps are performed to pieline data into the above model:

![image](https://user-images.githubusercontent.com/4925968/137585276-54e5e5a8-0be8-4358-9d15-0bf7797423ed.png)

## Step 4: Run pipelines to model the data

Apache Airflow is chosen as the tool for data pipelines. Two DAGs are created:
* load_immigration_dimensions - This DAG will stage the data and populate the following dimension tables (dim_Country, dim_States, dim_Ports, dim_AirportTypes, dim_Airports) and the demographics table. This DAG is not scheduled. It is meant to be run once to setup all the static dimensional data.
![image](https://user-images.githubusercontent.com/4925968/137585545-735b234d-8965-4ca6-9a0b-0652482fd8c2.png)

* load_immigration_facts - This DAG will stage the immigration data, populate the dimensions (dim_TravelModes, dim_VisaTypes, dim_time) and finally move the data in the fact_immigration table. This DAG is scheduled to be run daily. 
![image](https://user-images.githubusercontent.com/4925968/137585564-e5d6bc93-9818-4318-9547-103f3eb766a2.png)

### Data Quality checks
Following data quality checks are performed to ensure correctness of the data pipelines:
* Dimension data quality checks - This check is run after all the dimension tables are loaded. It checks whether the row count is greater than zero for the tables - dim_Country, dim_States, dim_Ports and dim_Airports
* Fact immigration data quality checks - This check is run after the fact_immigration table task is completed. It checks whether data is loaded for that run by checking the row count of records for that day.

### Data Dictionary
#### dim_Country
Column        | Data Type     | Description
-----------   | -----------   | ------------
country_id    | int           | Primary key. Auto generate using IDENTITY
country_code  | varchar(3)    | 3 letter country code (NOT NULL UNIQUE) from I94 SAS data dictionary
country_name  | varchar(100)  | Name of the country from I94 SAS data dictionary

#### dim_States
Column        | Data Type     | Description
-----------   | -----------   | ------------
state_id      | int           | Primary key. Auto generate using IDENTITY
state_code    | varchar(2)    | 2 letter state code (NOT NULL UNIQUE) from I94 SAS data dictionary
state_name    | varchar(100)  | Name of the state from I94 SAS data dictionary

#### dim_Ports
Column        | Data Type     | Description
-----------   | -----------   | ------------
port_id       | int           | Primary key. Auto generate using IDENTITY
state_id      | int           | FOREIGN KEY references dim_States(state_id)
port_code     | varchar(3)    | 3 digit port code from I94 SAS data dictionary (NOT NULL UNIQUE)
port_name     | varchar(100)  | Name of the port from I94 SAS data dictionary (UNIQUE)

#### dim_TravelModes
Column          | Data Type     | Description
-----------     | -----------   | ------------
travel_mode_id  | int           | Primary key. Auto generate using IDENTITY
mode            | varchar(25)   | Sea, Air, Land etc (from I94 SAS data dictionary)

#### dim_VisaCategories
Column            | Data Type     | Description
-----------       | -----------   | ------------
visa_category_id  | int           | Primary key. Auto generate using IDENTITY
name              | varchar(50)   | Business, Pleasure, Student (from I94 SAS data dictionary)

#### dim_AirportTypes
Column          | Data Type     | Description
-----------     | -----------   | ------------
airport_type_id | int           | Primary key. Auto generate using IDENTITY
airport_type    | varchar(50)   | Airport names. From airport_codes_csv

#### dim_time
Column          | Data Type     | Description
-----------     | -----------   | ------------
ts              | int           | Primary key. SAS timestamp from I94 immigration data
year            | int           | Year
month           | int           | Month number
day             | int           | Day of the month

#### dim_Airports
Column          | Data Type     | Description
-----------     | -----------   | ------------
airport_id      | int           | Primary key. Auto generate using IDENTITY
airport_name    | varchar(200)  | Name of airport from airport_codes_csv
airport_type_id | int           | Type of airport. FOREIGN KEY to dim_AirportTypes(airport_type_id)
state_id        | int           | State where airport is located. FOREIGN KEY to dim_States(state_id)
continent       | varchar(50)   | Continent of the airport
elevation_ft    | int           | Elevation of the airport
municipality    | varchar(100)  | Municipality of the airport
gps_code        | varchar(25)   | GPS code of the airport
iata_code       | varchar(25)   | IATA code of the airport
local_code      | varchar(25)   | Local code of the airport
coordinates     | varchar(256)  | Coordinates of the airport

#### fact_Demographics
Column              | Data Type     | Description
-----------         | -----------   | ------------
demographics_id     | int           | Primary key. Auto generate using IDENTITY
city_id             | int           | City or Port. FOREIGN KEY REFERENCES dim_Ports(port_id)
median_age          | decimal       | Median age of the population
male_population     | int           | Male population
female_population   | int           | Female population
total_population    | int           | Total population
veterans            | int           | Veterans
foreign_born        | int           | Foreign born
avg_household_size  | decimal       | Average household size
race                | varchar(100)  | Race
count               | int           | Count

#### fact_Immigration
Column                  | Data Type     | Description
-----------             | -----------   | ------------
immigration_id          | int           | Primary key. Auto generate using IDENTITY
cicid                   | double        | Cicid of immigrant from I94 immigration data
citizenship_country_id  | int           | Country of citizenship. FOREIGN KEY REFERENCES dim_Country(country_id)
residence_country_id    | int           | Country of residence. FOREIGN KEY REFERENCES dim_Country(country_id)
port_id                 | int           | Port of entry. FOREIGN KEY REFERENCES dim_Ports(port_id)
arrival_date            | datetime      | Date of arrival
arrdate                 | double        | Date of arrival in SAS timestamp
departure_date          | datetime      | Date of departure
depdate                 | double        | Date of departure in SAS timestamp
travel_mode_id          | int           | Mode of travel (Air, Sea, Land). FOREIGN KEY REFERENCES dim_TravelModes(travel_mode_id)
age                     | integer       | Age of immigrant
gender                  | varchar(1)    | Gender of immigrant
visa_category_id        | int           | Category of visa (Business, travel, studen). FOREIGN KEY REFERENCES dim_VisaCategories(visa_category_id)
visatype                | varchar(10)   | Type of visa
airline                 | varchar(5)    | Airline used by the immigrant
fltno                   | varchar(10)   | Flight number of flight taken by the immigrant

## Step 5: Complete project write up

A combination of pandas and Apache Spark is used for processing and cleanup of the input data. Pandas is used to parse and extract the countries, states, port information from the I94 data SAS dictionary. Apache Spark is used for reading the immigration data, cleanup and add missing data. Since the size of immigration data is quite large, it is saved in parquet files partitioned by year, month and day. This will lead to smaller data processing size and faster data pipelines. All the processed data sets (with minimal cleanup) is uploaded to S3 to serve as the lake storage. The data here is close to raw data and can be used for different types of analysis.

The next step is to move the data into analytics tables in Redshift. The cleaner data model of the warehouse will allow us to answer variety of questions about immigration patterns. Apache Airflow is used as an orchestration tool for pipelining the data in the analytics tables. 
The main DAG to populate the fact_immigration table is scheduled to run on a daily basis.

How you would approach the problem differently under the following scenarios:
* The data was increased by 100x - For processing and cleanup of data we are using Apache Spark which can be scaled horizontally by adding more processing nodes.
* The data populates a dashboard that must be updated on a daily basis by 7am every day. - Since we are using Apache Airflow with daily DAG schedule this can be easily done. We can set the run time to 7AM in the dag schedule.
* The database needed to be accessed by 100+ people - The final analytics tables are created in Amazon Redshift which can easily scale horizontally and can handle more than 100+ user access.

## Steps to run the project

Prerequisites - Python 3.6 or higher, Apache Spark, Apache Airflow

* Run parse_sas_labels.py
* Run cleanup_and_process.py (Put aws access keys and secret in config.ini file)
* Create Redshift cluster in AWS
* Run the create_tables.sql script to create all the tables
* Setup Airflow. Copy DAGs in the Airflow DAG folder. Copy plugins in Airflow plugins folder
* Add connection variables in Airflow. One for "aws_credentials" with access key and secret. Another for redshift credentials.
* Trigger the load_immigration_dimensions DAG manually.
* Turn on load_immigration_facts DAG. This will be scheduled to run daily starting from 2016/1/1

## Example Queries (for 2016 1st Quarter)

### Which state and city had the maximum number of immigrants?
SELECT dp.port_name, ds.state_name, COUNT(*) as IMMIGRANT_COUNT  
FROM fact_immigration fi  
INNER JOIN dim_Ports dp on fi.port_id = dp.port_id  
INNER JOIN dim_States ds on dp.state_id = ds.state_id  
INNER JOIN dim_time dt on fi.arrdate = dt.ts  
WHERE dt.year = 2016 and dt.month IN (1, 2, 3)  
GROUP BY dp.port_id, dp.port_name, ds.state_id, ds.state_name ORDER BY IMMIGRANT_COUNT DESC  

![image](https://user-images.githubusercontent.com/4925968/137597037-9611ecd2-42a6-403d-ace0-5a2ba0b59062.png)

### What are the top countries where immigrants came from?
SELECT dc.country_name, COUNT(*) as COUNT  
FROM fact_immigration fi  
INNER JOIN dim_Country dc ON fi.residence_country_id = dc.country_id  
INNER JOIN dim_time dt on fi.arrdate = dt.ts  
WHERE dt.year = 2016 and dt.month IN (1, 2, 3)  
GROUP BY dc.country_id, dc.country_name  
ORDER BY COUNT DESC  

![image](https://user-images.githubusercontent.com/4925968/137597085-c3ac650d-cadf-4669-ae3c-9f33306a6b00.png)

### What is the most preferred purpose of travel ?
SELECT dv.name, COUNT(*)  
FROM fact_immigration fi  
INNER JOIN dim_VisaCategories dv on fi.visa_category_id = dv.visa_category_id  
INNER JOIN dim_time dt on fi.arrdate = dt.ts  
WHERE dt.year = 2016 and dt.month IN (1, 2, 3)  
GROUP BY dv.visa_category_id, dv.name  
ORDER BY COUNT DESC  

![image](https://user-images.githubusercontent.com/4925968/137598102-ae846888-3056-4aa1-9e1a-6f18e095be72.png)

### What is the total population and immigrant population of the cities where maximum immigrations are happening?
SELECT dp.port_name, fd.total_population, COUNT(*) as IMMIGRANT_COUNT  
FROM fact_immigration fi  
INNER JOIN dim_Ports dp on fi.port_id = dp.port_id  
INNER JOIN fact_demographics fd on fi.port_id = fd.city_id  
INNER JOIN dim_time dt on fi.arrdate = dt.ts  
WHERE dt.year = 2016 and dt.month IN (1, 2, 3)  
GROUP BY dp.port_id, dp.port_name, fd.total_population  
ORDER BY IMMIGRANT_COUNT DESC  

![image](https://user-images.githubusercontent.com/4925968/137597902-c0c12d61-1983-4e9e-92e2-f21a7b37c9a1.png)


