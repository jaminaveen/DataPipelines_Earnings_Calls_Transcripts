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
   - Steps to replicate the application

### Project Documentation Link (Google Doc):
> included MongoDB Installation Guide

https://docs.google.com/document/d/1rFcNPuP9XiATgN7kJyd_60TYVKW-msw6CV0yMsOO_qc/edit?usp=sharing

### Codelabs Presentation:

https://codelabs-preview.appspot.com/?file_id=1rFcNPuP9XiATgN7kJyd_60TYVKW-msw6CV0yMsOO_qc#0

### System Overview

![](https://github.com/jaminaveen/DataPipelines_Earnings_Calls_Transcripts/blob/master/INFO7374%20Project1%20Pipeline.jpeg)


### Data Module Sequential Diagram
![](https://github.com/jaminaveen/DataPipelines_Earnings_Calls_Transcripts/blob/master/Data_Module_Sequence_Diagram.png)

 
### Steps to replicate the application

1. [Set up Mongodb in the local system](https://docs.google.com/document/d/1rFcNPuP9XiATgN7kJyd_60TYVKW-msw6CV0yMsOO_qc/edit#heading=h.ium335l8jusv)

2. Install Python 3

3. Clone this repository

       git clone https://github.com/jaminaveen/DataPipelines_Earnings_Calls_Transcripts.git

4. Change working folder
    
       cd DataPipelines_Earnings_Calls_Transcripts/

5. Install dependencies 
   [requirements.txt](https://github.com/jaminaveen/DataPipelines_Earnings_Calls_Transcripts/blob/master/ECT/requirements.txt)
       
       run: pip install -r ECT/requirements.txt
 
6. Run main.py to extract, preprocess, store transcripts data into MongoDB
     
       python ECT/main.py
    
    
