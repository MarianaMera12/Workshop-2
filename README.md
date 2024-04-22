# Workshop-002 

## By: Mariana Mera Gutierrez 

### ETL Automation for Spotify and Grammy Awards Data Analysis

This project focuses on the development of an ETL (Extract, Transform, Load) automation system to analyze music data and Grammy Award nominations. The main objective is to create an automated workflow to extract data from diverse sources, transform it for analysis and load it into a final destination for further use and visualization.

**The ETL automation process encompasses several key stages:**

1. **Data Extraction:** Collection of data from various sources such as CSV file and databases.

2. **Data Transformation:** Data cleaning, normalization and structuring to prepare data for analysis.

3. **Integration and Analysis:** Exploratory analysis and data integration to identify trends and patterns.

4. **Loading at Final Destination:** Storage of processed data in a database.

**Visualize data in an attractive and informative way:**

- Creation of interactive graphs and dashboards.
- Communicating findings in a clear and concise manner.

### Tools Technologies

- Python 
- Jupiter Notebook
- Apache Airflow
- Database PostgreSQL
- Power BI 

### Data

**Spotify Dataset**

The CSV file of [Spotify](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset) contains music data collected from the Spotify platform. This dataset includes information on various characteristics of the songs, such as their popularity, duration, musical genre, associated artists, among others.

**Grammy Dataset**

The CSV file of the [Grammy Awards](https://www.kaggle.com/datasets/unanimad/grammy-awards) contains data related to Grammy nominations and awards. This dataset provides information on award categories, nominated artists, years of nominations and awards, among other details.

### Repository content
The structure of the repository is as follows:

- `dags`: Folder where the transformations, the merge, database connection and the dag structure are located in `main.py`.

    - `pydrive`: This folder contains functions to authenticate to Google Drive and save this DataFrame as a CSV file in Google Drive using PyDrive API.

- `data`: Folder containing the CSV files 'spotify.csv' and 'grammy.csv'.

- `README.md`: This file you are reading now.

- `EDA_Notebooks`: This folder contains the 3 Notebooks that were made to understand the data set.
    - `01_EDA.ipynb`: Spotify Analysis.
    - `02_EDA.ipynb`: Grammy Awards Analysis.
    - `03_Merge.ipynb`: Merge analysis.
    

- `requirements.txt`: File that specifies the Python dependencies required to run the project.

- `docker-compose.yaml`: This Docker Compose configuration file defines services for running Apache Airflow with PostgreSQL and Redis, configuring specific environments and dependencies, including configurations for the Airflow webserver, scheduler, worker, triggerer, and CLI services, along with PostgreSQL and Redis database services.

- `Dockerfile`: This file specifies the configuration to create a runtime environment based on the *apache/airflow:2.8.4-python3.9* image, including the necessary dependencies for Apache Airflow and other packages specified in the `requirements.txt` file.  

### Requirements
- Install Docker : [Docker Downloads](https://docs.docker.com/engine/install/)
- Install Python : [Python Downloads](https://www.python.org/downloads/)
- Install PostgreSQL : [PostgreSQL Downloads](https://www.postgresql.org/download/)
- Install Power BI : [Power BI Desktop](https://www.microsoft.com/en-us/download/details.aspx?id=58494) 

### Environment variables

To connect to the database, you will need to set some environment variables.

- `DATABASE`: Replace with the name of the database you want to use.

- `USER`: Replaces with the database user name.

- `PASSWORD`: Replaces with the password of the database user.

- `HOST`: We use `host.docker.internal` to connect to a local PostgreSQL database from a Docker container.

- `PORT`: PostgreSQL server listening port.

### Credentials GoogleDrive

- `credentials_module.json`:To obtain the necessary credentials to connect to the PyDrive API I recommend you follow the instructions in this video [PyDrive API](https://www.youtube.com/watch?v=ZI4XjwbpEwU).


### Run this project

1. Clone the project
~~~
git clone https://github.com/MarianaMera12/Workshop-2.git 
~~~
2. Go to the project directory
~~~
cd Workshop-2
~~~
3. Create virtual environment for Python
~~~
python -m venv venv
~~~
4. Install libreries
~~~
pip install -r requirements.txt
~~~

### Run Docker 

**Requirement:** In Visual Studio Code have python and docker installed.

 - Python
   
 [![Captura-de-pantalla-2024-04-21-122151.png](https://i.postimg.cc/pLnBpp2M/Captura-de-pantalla-2024-04-21-122151.png)](https://postimg.cc/hXcmY4z2)
 - Docker
   
 [![dockervisual.png](https://i.postimg.cc/nhs3B3G5/dockervisual.png)](https://postimg.cc/SnhLp757)


5. After installing Docker, open Docker Desktop.  
[![Captura-de-pantalla-2024-04-21-124757.png](https://i.postimg.cc/nc8Fyh3w/Captura-de-pantalla-2024-04-21-124757.png)](https://postimg.cc/mhwWMBfQ)

Make sure you have your project folder ready on your system, then open Visual Studio Code and select your project folder from the 'File' > 'Open Folder' menu.
- Open the folder in visual
  
 [![wk2-parte1.png](https://i.postimg.cc/T3MR3PqJ/wk2-parte1.png)](https://postimg.cc/BXpWh4zj)

- First we build the image with the Dockerfile
  
[![wk2-parte2.png](https://i.postimg.cc/02Qzgr5W/wk2-parte2.png)](https://postimg.cc/QV2818T1)

- We lift the services
  
[![wk2-parte3.png](https://i.postimg.cc/g0Zxj71f/wk2-parte3.png)](https://postimg.cc/NyQsPds4)

### If you use another code editor

In the terminal: 

~~~
docker-compose up airflow-init
~~~
~~~
docker-compose up
~~~

6. In the browser we enter with : "http://localhost:8080/home"
[![wk2-parte4.png](https://i.postimg.cc/y6hCszYw/wk2-parte4.png)](https://postimg.cc/t7gMNw65)

### Power BI

7. We made the connection with Power BI to graph the transformed data.

 - Open a new window in Power BI
   
 [![Datos.png](https://i.postimg.cc/fRFZQdzD/Datos.png)](https://postimg.cc/RWc2cNJD)
 - Select in 'more'.
   
 [![datas2.png](https://i.postimg.cc/JzdJwpH0/datas2.png)](https://postimg.cc/w1L3LcVY)
 - Select PostgreSQL database
   
 [![datos3.png](https://i.postimg.cc/sfNRhSWS/datos3.png)](https://postimg.cc/K4BVX1Jv)
 - Insert your information and accept
   
 [![datos4.png](https://i.postimg.cc/j2G9kH6F/datos4.png)](https://postimg.cc/T5JCKW4V)
 - Select the table and download the information
   
[![Captura-de-pantalla-2024-04-21-123802.png](https://i.postimg.cc/J0kF3mJs/Captura-de-pantalla-2024-04-21-123802.png)](https://postimg.cc/JHRKmfp8)
Already the data is synchronized with Power BI and you can make your own dashboard.

### Dashboard

[![Workshop2-page-0002.jpg](https://i.postimg.cc/85BJ7fXc/Workshop2-page-0002.jpg)](https://postimg.cc/CB5Lt5gp)

[![Workshop2-page-0003.jpg](https://i.postimg.cc/65Q2Q3r6/Workshop2-page-0003.jpg)](https://postimg.cc/Z01K7JsX)


### Thank you for visiting our repository!

We hope this project will help you practice your Data Engineer skills. If you found it useful, give it a star!

Your comments and suggestions are welcome, please contribute to the project!

See you soon... 
