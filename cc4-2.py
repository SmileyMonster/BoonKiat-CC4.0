#Written on Python 3.10.0
#Input source files
#Restaurant data:
#https://raw.githubusercontent.com/ashraf356/cc4braininterview/main/restaurant_data.json
#Country Code:
#https://github.com/ashraf356/cc4braininterview/blob/main/Country-Code.xlsx?raw=true

import json
import csv

#Function to check and replaces empty strings with "NA"
def emptyCheck(inList):
    outList = []
    for item in inList:
        if not str(item).strip():
            outList.append("NA")
        else:
            outList.append(item)
    
    return outList

#Function to read CSV file
def csvReader(fileIn):
    with open(fileIn, mode='r') as csvfile:
        reader = csv.reader(csvfile)
        outputDict = {rows[0]:rows[1] for rows in reader}

    csvfile.close()
    return outputDict

def main():
    #Read in CSV file to Dictionary for country code
    countryCodeDict = csvReader('Country-Code.csv')

    #Read raw json from file
    with open('cc4.json') as f:
        data = json.load(f)

    try:
        i = 0
        resultShownKey = 'results_shown'
        zomatoKey = 'zomato_events'
        photoKey = 'photos'
        april2017 = "2017-04"

        #2(i) Extract list of restaurants that have past event within the month of April 2017 and store the data as .csv
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
        f.close()    
        csvf.close()

main()