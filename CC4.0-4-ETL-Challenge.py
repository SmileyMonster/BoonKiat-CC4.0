'''
Written on Python 3.8 in AWS Lambda environment

Lambda Layers required:
1. Custom layer .zip upload with pandas1.4.1(for Python 3.8) and pytz-2021.3
    Pandas link - https://files.pythonhosted.org/packages/20/53/5ad34b9d52f94e1ae8a4a410ead791e74e03d200ec64d9c3f61d83915ec4/pandas-1.4.1-cp38-cp38-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
    Pytz link   - https://files.pythonhosted.org/packages/d3/e3/d9f046b5d1c94a3aeab15f1f867aa414f8ee9d196fae6865f1d6a0ee1a0b/pytz-2021.3-py2.py3-none-any.whl
    
2. AWSLambda-Python38-SciPy1x

Input source files
Restaurant data:
https://raw.githubusercontent.com/ashraf356/cc4braininterview/main/restaurant_data.json

Input file have been downloaded and uploaded into S3 bucket named "cc4-source-json"
Output file will be directed to S3 bucket named "cc4-destination" by default
Program will not work if default specified buckets and files do not exist in your S3
'''

import json
import csv
import pandas as pd
import boto3

s3 = boto3.client('s3')
s3a = boto3.resource('s3')


'''
Main event driver
Args:
    **Lamda runtime provided fields
    param1: event object
    param2: context object 
Returns:
    'Success'
Raises:
    Exceptions as 'Failed'
'''
def lambda_handler(event, context):
    desiredS3Source = "cc4-source-json"
    inputJsonFile = "cc4.json" #restaurant-data.json
    
    desiredDestination = "cc4-destination"
    desiredOutputFileName = "output-ETL-Challenge.csv"
    
    #retrieve bucket and read in json data
    try:
        jsonData = bucketReader(desiredS3Source, inputJsonFile)
        data = jsonReader(jsonData)
        
        #Generate .csv file from json data with 'aggregate_rating' and 'rating text'
        dataExtraction(data, desiredDestination, desiredOutputFileName)
        
        #4 Eptional ETL challenge
        etlChallenge(desiredDestination, desiredOutputFileName)

    except Exception as err:
        raise err
        return 'Failed!'
        
    else:
        return 'Success!'



'''
Function to fetch input files from S3 bucket
Args:
    param1: String of S3 bucket containing input files
    param2: String of Input file name
Returns:
    Object of file from specified S3 bucket and file name
Raises:
    BucketNotFound: Invalid bucket name
    KeyNotFound: Invalid file name
'''
def bucketReader(bucketIn, keyIn):
    bucket = bucketIn
    key = keyIn
    try:
        response = s3.get_object(Bucket=bucket, Key=key)

    except Exception as e:
        print(f'ERROR: \n {e}')
        raise e

    else:
        return response



'''
Function to read JSON data from object
Args:
    param1: JSON file object
Returns:
    deserialized JSON data
Raises:
    JSONDecodeError: Invalid JSON file
'''
def jsonReader(jsonDataIn):
    response = jsonDataIn
    try:
        text = response["Body"].read().decode()
        data = json.loads(text)

    except Exception as e:
        print(f'ERROR: \n {e}')
        raise e
    
    else:
        return data


 
'''
Function to extract restaurant json file data into .csv readable
Args:
    param1: JSON file content (restaurant_data)
    param2: String of S3 Bucket name to store output files
    param3: String of output file name
Returns:
    .csv output file named (param4) into S3 bucket named (param3)
'''     
def dataExtraction(inputJson, destinationBucket, outputFileName):
    data = inputJson
    filePath = '/tmp/' + outputFileName

    try:
        i = 0
        resultShownKey = 'results_shown'

        with open(filePath, "a", encoding="utf-8", newline='') as csvf:
            #Clear file and write header
            csvf.truncate(0)
            writer = csv.writer(csvf)
            header = ['User aggregate_rating', 'Rating text']
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

                            r_RatingText = data[i]['restaurants'][j]['restaurant']['user_rating']['rating_text']
                            r_UserAgg = float(data[i]['restaurants'][j]['restaurant']['user_rating']['aggregate_rating'])

                            csvLine = [float(r_UserAgg), r_RatingText]
                            writer.writerow(csvLine)
                    i += 1

        #upload file from tmp to s3 key
        s3a.meta.client.upload_file(filePath, destinationBucket, outputFileName)
        
    except Exception as e:
        print(f'ERROR: \n {e}')
        raise e


   
'''
Function to analyse extracted .csv data and overwrite with results
Args:
    param1: String of S3 Bucket name to store output files
    param2: String of output file name
Returns:
    .csv output file named (param2) into S3 bucket named (param1)
'''        
def etlChallenge(destinationBucket, outputFileName):
    filePath = '/tmp/' + outputFileName
    
    data = pd.read_csv(filePath)
    df = pd.DataFrame(data)
    sortedDf = df.sort_values(['User aggregate_rating'], ascending=False)
    treshold = sortedDf.groupby('Rating text', sort=False).agg({'User aggregate_rating':['count', 'min', 'max']})
    print(treshold)
    
    with open(filePath, "a", encoding="utf-8", newline='') as csvf:
        #Clear file and overwrite
        csvf.truncate(0)
        treshold.to_csv(filePath, encoding='utf-8')

    s3a.meta.client.upload_file(filePath, destinationBucket, outputFileName)

