#Written on Python 3.10.0 in local offline environment
#Input source files
#Restaurant data:
#https://raw.githubusercontent.com/ashraf356/cc4braininterview/main/restaurant_data.json
#Country Code:
#https://github.com/ashraf356/cc4braininterview/blob/main/Country-Code.xlsx?raw=true 
#
#Input files have been downloaded and placed into the same directory
#Output files will be generated to the same directory named "DataExtraction.csv" and "DataExtraction2.csv"
#Program will not work if specified input files do not exist in the same directory
#Program will not work if "DataExtraction.csv" or "DataExtraction2.csv" is opened during code execution
#Program will overwrite existing "DataExtraction.csv" and "DataExtraction2.csv" if files exists in the same directory

import json
import csv
import operator

#main driver
def main():
    #Read in CSV file to Dictionary for country code
    countryCodeDict = csvReader('Country-Code.csv')
    #Read in raw json data
    jsonData = jsonReader('cc4.json')

    #2(i) Extract the following fields and store the data as .csv
    dataExtractionOne(jsonData, countryCodeDict)
    #Sort output file base on 'User aggregate_rating' column
    sortCsv("DataExtraction.csv")
    #2(ii) Extract list of restaurants that have past event within the month of April 2017 and store the data as .csv
    dataExtractionTwo(jsonData)

def sortCsv(fileIn):
    with open(fileIn, mode='r', encoding='utf-8') as csvfile:
        data = csv.reader(csvfile)
        data = sorted(data, key=operator.itemgetter(5), reverse=True)

    with open(fileIn, "a", encoding="utf-8", newline='') as csvf:
        #Clear file
        csvf.truncate(0)
        #Write sorted data back inside file
        writer = csv.writer(csvf)
        for rows in data:
            writer.writerow(rows)

#Function to read JSON file
def jsonReader(fileIn):
    with open(fileIn) as f:
        data = json.load(f)

    f.close()
    return data

#Function to read CSV file
def csvReader(fileIn):
    with open(fileIn, mode='r') as csvfile:
        reader = csv.reader(csvfile)
        outputDict = {rows[0]:rows[1] for rows in reader}

    csvfile.close()
    return outputDict


#Function to check and replaces empty strings with "NA"
def emptyCheck(inList):
    outList = []
    for item in inList:
        if not str(item).strip():
            outList.append("NA")
        else:
            outList.append(item)
    
    return outList

def dataExtractionOne(inputJson, inputDict):
    data = inputJson
    try:
        i = 0
        resultShownKey = 'results_shown'

        with open("DataExtraction.csv", "a", encoding="utf-8", newline='') as csvf:
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
                    i += 1
                else:
                    continue

    except IndexError as e:
        print("IndexError: " + str(e))
    except KeyError as e1:
        print("KeyError: " + str(e1))
    finally:
        csvf.close()

def dataExtractionTwo(inputJson):
    data = inputJson
    try:
        i = 0
        resultShownKey = 'results_shown'
        zomatoKey = 'zomato_events'
        photoKey = 'photos'
        april2017 = "2017-04"

        with open("DataExtraction2.csv", "a", encoding="utf-8", newline='') as csvf:
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
                
    except IndexError as e:
        print("IndexError: " + str(e))

    except KeyError as e1:
        print("KeyError: " + str(e1))

    finally:
        csvf.close()

#execute program
main()
