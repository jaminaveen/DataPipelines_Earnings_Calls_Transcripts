# DataPipelines_Earnings_Calls_Transcripts
![](https://github.com/jaminaveen/DataPipelines_Earnings_Calls_Transcripts/blob/master/Notched%20N%20Motto_RB.png)
INFO 7374 Product Grade Data Pipelines</br>
Shiqi Dai</br>
Naveen Jami</br>
Satwik Kashyap</br>
Sindhu Raghavendra</br>
Professor: Sri Krishnamurthy</br>

### Progress
   - [x] Web Scraping and storing csv
   - [x] Mapping paragraphs from raw html data(tuples - Introduction mappings, Q & A mappings, Conclusion mappings)
   - [x] Data Model
   - [x] Data Storage 
   - [x] Pre-processing
   - [ ] Sentiment API's module
   - [ ] Predictions and Analysis

### Contents
   - Project Documentation Link (Google Doc)
   - Codelabs Presentation
   - System Overview
   - Data Module Sequential Diagram
   - docker-compose instructions to install MongoDB and other Python Dependenices

### Project Documentation Link (Google Doc):
> included MongoDB Installation Guide

https://docs.google.com/document/d/1rFcNPuP9XiATgN7kJyd_60TYVKW-msw6CV0yMsOO_qc/edit?usp=sharing

### Codelabs Presentation:

https://codelabs-preview.appspot.com/?file_id=1rFcNPuP9XiATgN7kJyd_60TYVKW-msw6CV0yMsOO_qc#0

### System Overview

![](https://github.com/jaminaveen/DataPipelines_Earnings_Calls_Transcripts/blob/master/INFO7374%20Project1%20Pipeline.jpeg)


### Data Module Sequential Diagram
![](https://github.com/jaminaveen/DataPipelines_Earnings_Calls_Transcripts/blob/master/Data_Module_Sequence_Diagram.png)

### docker-compose instructions to install MongoDB and other Python Dependenices

1. Clone this repository

       git clone https://github.com/jaminaveen/DataPipelines_Earnings_Calls_Transcripts.git

2. Open Docker Quickstart Terminal

3. Change directory to this git clone folder

       cd "<local path>/DataPipelines_Earnings_Calls_Transcripts"

3. Docker Compose build (Only use for the first time)

       docker-compose build

4. Run the services and application (Note: If you are running it for the first time, Scraping will take about 30 min)
    
       docker-compose up

5. Optional - To check data in MongoDB GUI, install either MongoDB Compass Community IDE or Robo3T IDE and use these connection settings

       hostname - <docker-machine ip>
       port - 27017
    
### Two ways to find the docker ip

1. Use the command
       
       docker-machine ip
 
2. Also, we can find docker machine ip when we open the docker terminal at the beginning
 
  ![](https://github.com/jaminaveen/DataPipelines_Earnings_Calls_Transcripts/blob/master/dockermachine_ip.PNG)
       
    
    
