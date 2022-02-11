#Written on Python 3.6 in AWS Lambda environment
#
#Input source files
#Restaurant data:
#https://raw.githubusercontent.com/ashraf356/cc4braininterview/main/restaurant_data.json
#Country Code:
#https://github.com/ashraf356/cc4braininterview/blob/main/Country-Code.xlsx?raw=true
#
#Input files have been downloaded and uploaded into S3 bucket named "cc4-source-json"
#Output files will be directed to S3 bucket named "cc4-destination" by default
#Program will not work if default specified buckets and files do not exist in your S3

import json
import boto3
import csv

s3 = boto3.client('s3')
s3a = boto3.resource('s3')

def lambda_handler(event, context):
    #replace desiredS3Source string with desired source bucket name, E.g "cc4-source-json"
    #replace inputFile string with source file in uploaded to above provided bucket
    desiredS3Source = "cc4-source-json"
    inputJsonFile = "cc4.json"
    inputCsvFile = "Country-Code.csv"
    
    #replace desiredDestination string with desired bucket location name, E.g "cc4-destination"
    #replace desiredCsvName for CSV output file name, E.g "output1.csv"
    #WARNING: existing file names in your S3 Bucket will be overriden
    desiredDestination = "cc4-destination"
    desiredCsvNameOne = "output1.csv"
    desiredCsvNameTwo = "output2.csv"

    #retrieve bucket and read in json data
    jsonData = bucketReader(desiredS3Source, inputJsonFile)
    data = jsonReader(jsonData)
    
    #retrieve bucket and read in csv data to dictionary
    csvData = bucketReader(desiredS3Source, inputCsvFile)
    countryCodeDict = csvReader(csvData)
    
    #2(i) Extract the following fields and store the data as .csv
    dataExtractionOne(data, countryCodeDict, desiredDestination, desiredCsvNameOne)
    
    #2(ii) Extract list of restaurants that have past event within the month of April 2017 and store the data as .csv
    dataExtractionTwo(data, desiredDestination, desiredCsvNameTwo)
    
    return 'Success!'

#Function to retrieve bucket
def bucketReader(bucketIn, keyIn):
    bucket = bucketIn
    key = keyIn
    try:
        #fetch file from s3 bucket
        response = s3.get_object(Bucket=bucket, Key=key)

        return response
        
    except Exception as e:
        print(e)
        raise e

#Function to read JSON file
def jsonReader(jsonDataIn):
    response = jsonDataIn
    try:
        #deserialize the content
        text = response["Body"].read().decode()
        data = json.loads(text)
    
        return data
        
    except Exception as e:
        print(e)
        raise e

#Function to read csv file
def csvReader(csvDataIn):
    response = csvDataIn
    try:
        text = response["Body"].read().decode().splitlines()
        data = csv.reader(text)
        outputDict = {rows[0]:rows[1] for rows in data}
    
        return outputDict
        
    except Exception as e:
        print(e)
        raise e

#Function to check and replaces empty strings with "NA"
def emptyCheck(inList):
    outList = []
    for item in inList:
        if not str(item).strip():
            outList.append("NA")
        else:
            outList.append(item)
    
    return outList

def dataExtractionOne(inputJson, inputDict, destinationBucket, outputFileName):
    data = inputJson
    filePath = '/tmp/' + outputFileName

    try:
        i = 0
        resultShownKey = 'results_shown'

        with open(filePath, "a", encoding="utf-8", newline='') as csvf:
            #Clear file and write header
            csvf.truncate(0)
            writer = csv.writer(csvf)
            header = ['Restaurant id', 'Restaurant name', 'Country name', 'City name', 'User rating votes', 'User aggregate_rating', 'Cuisines']
            writer.writerow(header)

            for records in data:

                #Filter out invalid records
                #"message": "API limit exceeded","code": 440,"status": ""
                if resultShownKey in records:

                    #iterate base on no. of results shown
                    resultsShown = data[i]['results_shown']
                    for j in range(0, resultsShown):

                        #Filter out results_shown = 0
                        if resultsShown != 0:

                            #Retrieve fields
                            #'Restaurant id', 'Restaurant name', 'Country name', 'City name', 'User rating votes', 'User aggregate_rating', 'Cuisines'
                            r_Name = data[i]['restaurants'][j]['restaurant']['name']
                            r_Id = data[i]['restaurants'][j]['restaurant']['R']['res_id']
                            r_CountryCODE = data[i]['restaurants'][j]['restaurant']['location']['country_id']
                            
                            #Retrieve Country names from loaded csv file
                            key = str(r_CountryCODE)
                            if key in inputDict:
                                r_CountryName = inputDict[key]

                            r_CityName = data[i]['restaurants'][j]['restaurant']['location']['city']
                            r_UserRatingVotes = data[i]['restaurants'][j]['restaurant']['user_rating']['votes']
                            r_UserAgg = float(data[i]['restaurants'][j]['restaurant']['user_rating']['aggregate_rating'])
                            r_Cuisines = data[i]['restaurants'][j]['restaurant']['cuisines']

                            csvLine = [r_Id, r_Name, r_CountryName, r_CityName, r_UserRatingVotes, float(r_UserAgg), r_Cuisines]
                            writer.writerow(csvLine)
                            #print(csvLine)
                    i += 1
                else:
                    continue
        #upload file from tmp to s3 key
        s3a.meta.client.upload_file(filePath, destinationBucket, outputFileName)
        
    except Exception as e:
        print(e)
        raise e
        
def dataExtractionTwo(inputJson, destinationBucket, outputFileName):
    data = inputJson
    filePath = '/tmp/' + outputFileName
    
    try:
        i = 0
        resultShownKey = 'results_shown'
        zomatoKey = 'zomato_events'
        photoKey = 'photos'
        april2017 = "2017-04"

        with open(filePath, "a", encoding="utf-8", newline='') as csvf:
            #Clear file and write header
            csvf.truncate(0)
            writer = csv.writer(csvf)
            header = ['Event id', 'Restaurant id', 'Restaurant name', 'Photo url', 'Event title', 'Event start date', 'Event end date']
            writer.writerow(header)

            for records in data:

                #Filter out invalid records
                #"message": "API limit exceeded","code": 440,"status": ""
                if resultShownKey in records:
                    #iterate base on no. of results shown
                    resultsShown = data[i]['results_shown']

                    for j in range(0, resultsShown):
                        zomatoKeyShown = data[i]['restaurants'][j]['restaurant']

                        if zomatoKey in zomatoKeyShown:
                            zomatoKeyLen = len(data[i]['restaurants'][j]['restaurant']['zomato_events'])

                            for k in range(0, zomatoKeyLen):
                                
                                #Filter out results_shown = 0
                                if resultsShown != 0:

                                    if zomatoKeyLen != 0:
                                        #Retrieve fields
                                        #'Event id', 'Restaurant id', 'Restaurant name', 'Photo url', 'Event title', 'Event start date', 'Event end date'
                                        r_Name = data[i]['restaurants'][j]['restaurant']['name']
                                        r_Id = data[i]['restaurants'][j]['restaurant']['R']['res_id']

                                        e_Id = data[i]['restaurants'][j]['restaurant']['zomato_events'][k]['event']['event_id']
                                        e_Title = data[i]['restaurants'][j]['restaurant']['zomato_events'][k]['event']['title']
                                        e_Start = data[i]['restaurants'][j]['restaurant']['zomato_events'][k]['event']['start_date']
                                        e_End = data[i]['restaurants'][j]['restaurant']['zomato_events'][k]['event']['end_date']
                                        
                                        photoKeyShown = data[i]['restaurants'][j]['restaurant']['zomato_events'][k]['event']

                                        if photoKey in photoKeyShown:
                                            photoKeyLen = len(data[i]['restaurants'][j]['restaurant']['zomato_events'][k]['event']['photos'])
                                            photoUrlList = ""

                                            for l in range(0, photoKeyLen):
                                                #Append ',' if multiple URLs
                                                if l == (photoKeyLen-1):
                                                    photoUrlList += (data[i]['restaurants'][j]['restaurant']['zomato_events'][k]['event']['photos'][l]['photo']['url'])
                                                else:
                                                    photoUrlList += ((data[i]['restaurants'][j]['restaurant']['zomato_events'][k]['event']['photos'][l]['photo']['url']) + ", ")

                                        e_PhotoUrl = photoUrlList

                                        #2(ii) If there are fields with empty value, please populate with "NA"
                                        if april2017 in e_Start and april2017 in e_End:
                                            csvLine = [e_Id, r_Id, r_Name, e_PhotoUrl, e_Title, e_Start, e_End]
                                            csvLineChecked = emptyCheck(csvLine)
                                            writer.writerow(csvLineChecked)
                        else:
                            continue
                    i += 1
                else:
                    continue
        #upload file from tmp to s3 key
        s3a.meta.client.upload_file(filePath, destinationBucket, outputFileName)
        
    except Exception as e:
        print(e)
        raise e
