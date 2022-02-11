#Written on Python 3.10.0
#Input source files
#Restaurant data:
#https://raw.githubusercontent.com/ashraf356/cc4braininterview/main/restaurant_data.json
#Country Code:
#https://github.com/ashraf356/cc4braininterview/blob/main/Country-Code.xlsx?raw=true

import json
import csv

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
        count = 1
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
                            if key in countryCodeDict:
                                r_CountryName = countryCodeDict[key]

                            r_CityName = data[i]['restaurants'][j]['restaurant']['location']['city']
                            r_UserRatingVotes = data[i]['restaurants'][j]['restaurant']['user_rating']['votes']
                            r_UserAgg = float(data[i]['restaurants'][j]['restaurant']['user_rating']['aggregate_rating'])
                            r_Cuisines = data[i]['restaurants'][j]['restaurant']['cuisines']

                            csvLine = [r_Id, r_Name, r_CountryName, r_CityName, r_UserRatingVotes, float(r_UserAgg), r_Cuisines]
                            writer.writerow(csvLine)

                            count += 1
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