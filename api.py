import requests
import json
import unittest
from bs4 import BeautifulSoup
import sqlite3
import unittest
import os
import csv
import matplotlib.pyplot as plt
# from matplotlib.ticker import FixedLocator, FormatStrFormatter
import numpy as np


def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn


list1 = ["Asia", "North America", "Africa", "Europe", "South America","Oceania"]
list2 = [1, 2, 3, 4, 5, 6]
list3 = []
list4 = []
list5 = []
list6 = []
list7 = []
list8 = []

def create_aqi_table(cur, conn):

    for country in dict_country_id.keys():

        base_url = f"https://api.waqi.info/feed/{country}/?token=8a23bbffcf70881ba9734d4547bf4010552222ad"

        resp = requests.get(base_url)

        location_info = json.loads(resp.text)

        if (location_info['status'] == 'ok'):
            # get api
            air_quality_index = location_info['data']['aqi']

            if air_quality_index == "-":
                if "pm25" not in location_info['data']['iaqi']:
                    continue
                else:
                    air_quality_index = location_info['data']['iaqi']["pm25"]["v"]


            # get pm25
            # if "o3" not in location_info['data']['iaqi']:
            #     continue

            # pm_25_value = location_info['data']['iaqi']["o3"]["v"]

            # make country name lower case 

            # insert data into the Tracks table

            location_name = country.lower()
        #     cur.execute('INSERT INTO AirQualityTable (Country_ID, AirQualityIndex) VALUES (?, ?)',
        #         (dict_return[location_name], air_quality_index))
            list7.append(dict_country_id[location_name])
            list8.append(air_quality_index)
        #         # commit the changes
    
        # conn.commit()


dict_continent_id = {}
def create_continent_table(cur, conn):

    dict_continent_id["Asia"] = 1
    dict_continent_id["North America"] = 2
    dict_continent_id["Africa"] = 3
    dict_continent_id["Europe"] = 4
    dict_continent_id["South America"] = 5
    dict_continent_id["Oceania"] = 6
    
    # for continent in dict_continent_id.keys():
    #     cur.execute('INSERT INTO ContinentIDTable (Continent_ID, Continent) VALUES (?, ?)',
    #             (dict_continent_id[continent], continent))
    # conn.commit()

dict_country_id = {}
def create_country_and_mortality_table(cur, conn):
# https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={part}&appid={API key}
    # https://worldpopulationreview.com/country-rankings/death-rate-by-country

    url = "https://statisticstimes.com/demographics/countries-by-death-rate.php"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')

    tag = soup.find('table', id='table_id1')

    tag1 = tag.find('tbody')

    tag2 = tag1.find_all('tr')
    
    count = 1
    for tds in tag2:
        tag3 = tds.find_all('td')

        # index into first td and get country name
        country_name = (tag3[0].text).strip()
        if (country_name == "United States of America"):
            country_name = "United States"
        if (country_name == "Russian Federation"):
            country_name = "Russia"
        if (country_name == "Iran (Islamic Republic of)"):
            country_name = "Iran"
        if (country_name == "Bolivia (Plurinational State of)"):
            country_name = "Bolivia"
        if (country_name == "Democratic Republic of the Congo"):
            country_name = "dr congo"
        if (country_name == "Viet Nam"):
            country_name = "vietnam"
        if (country_name == "Republic of Korea"):
            country_name = "South Korea"
        if (country_name == "China, Taiwan Province of China"):
            country_name = "taiwan"
        

        country_name_lower = country_name.lower()
        dict_country_id[country_name_lower] = count

        continent_name = (tag3[6].text).strip()

        # insert and ignore 

        list3.append(count)
        list4.append(country_name_lower)
        list5.append(dict_continent_id[continent_name])

        # cur.execute('INSERT INTO CountryIDTable (Country_ID, Country, Continent_ID) VALUES (?, ?, ?)',
        #     (count, country_name_lower, dict_continent_id[continent_name]))

        # index into second td and get country name
        mortality_rate = (tag3[4].text).strip()

        # cur.execute('INSERT INTO MortalityRateTable (Country_ID, MortalityRate) VALUES (?, ?)',
        #     (count, mortality_rate))
        list6.append(mortality_rate)

        count = count + 1

    # conn.commit()

# loop through each min(25, size of database left)
# only update count if the item is unique 


def add_to_table(printAll, cur, conn):
    cur.execute('SELECT count(Country_ID) FROM AirQualityTable')
    count = cur.fetchone()
    count = count[0]

    if count + 25 > len(list7):
        endVal = len(list7)
    else:
        endVal = count + 25

    if printAll == True:
        for i in range(0, len(list7)):
            cur.execute('INSERT INTO AirQualityTable (Country_ID, AirQualityIndex) VALUES (?, ?)',
                (list7[i], list8[i]))
        conn.commit()
        return


    for i in range(count, endVal):
        cur.execute('INSERT INTO AirQualityTable (Country_ID, AirQualityIndex) VALUES (?, ?)',
                (list7[i], list8[i]))
    conn.commit()


def add_to_table1(printAll, cur, conn):
    cur.execute('SELECT count(Country_ID) FROM MortalityRateTable')
    count = cur.fetchone()
    count = count[0]

    if count + 25 > len(list3):
        endVal = len(list3)
    else:
        endVal = count + 25

    if printAll == True:
        for i in range(0, len(list3)):
            cur.execute('INSERT INTO MortalityRateTable (Country_ID, MortalityRate) VALUES (?, ?)',
                (list3[i], list6[i]))
        conn.commit()
        return

    for i in range(count, endVal):
        cur.execute('INSERT INTO MortalityRateTable (Country_ID, MortalityRate) VALUES (?, ?)',
            (list3[i], list6[i]))
    conn.commit()


def add_to_table2(printAll, cur, conn):
    cur.execute('SELECT count(Country_ID) FROM CountryIDTable')
    count = cur.fetchone()
    count = count[0]

    if count + 25 > len(list3):
        endVal = len(list3)
    else:
        endVal = count + 25
    
    if printAll == True:
        for i in range(0, len(list3)):
            cur.execute('INSERT INTO CountryIDTable (Country_ID, Country, Continent_ID) VALUES (?, ?, ?)',
                (list3[i], list4[i], list5[i]))
        conn.commit()
        return

    for i in range(count, endVal):
         cur.execute('INSERT INTO CountryIDTable (Country_ID, Country, Continent_ID) VALUES (?, ?, ?)',
            (list3[i], list4[i], list5[i]))
    conn.commit()


def add_to_table3(printAll, cur, conn):
    cur.execute('SELECT count(Continent_ID) FROM ContinentIDTable')
    count = cur.fetchone()
    count = count[0]

    if count >= len(list2):
        return

    if count + 25 > len(list2):
        endVal = len(list2)
    else:
        endVal = count + 25

    if printAll == True:
        for i in range(0, len(list2)):
            cur.execute('INSERT INTO ContinentIDTable (Continent_ID, Continent) VALUES (?, ?)',
                (list2[i], list1[i]))
        conn.commit()
        return

    for i in range(count, endVal):
        cur.execute('INSERT INTO ContinentIDTable (Continent_ID, Continent) VALUES (?, ?)',
            (list2[i], list1[i]))
    conn.commit()

def join_tables(cur, conn):
    # CountryIDTable.Country ContinentIDTable.Continent MortalityRateTable.MortalityRate AirQualityTable.AirQualityIndex
    cur.execute('''SELECT CountryIDTable.Country, ContinentIDTable.Continent, MortalityRateTable.MortalityRate, AirQualityTable.AirQualityIndex
    FROM AirQualityTable  
    JOIN CountryIDTable ON CountryIDTable.Country_ID = AirQualityTable.Country_ID 
    JOIN ContinentIDTable ON CountryIDTable.Continent_ID = ContinentIDTable.Continent_ID
    JOIN MortalityRateTable ON AirQualityTable.Country_ID = MortalityRateTable.Country_ID
    ''')

    # cur.execute("SELECT CountryIDTable.Country ContinentIDTable.Continent MortalityRateTable.MortalityRate AirQualityTable.AirQualityIndex FROM AirQualityTable INNER JOIN MortalityRateTable ON AirQualityTable.Country_ID = MortalityRateTable.Country_ID INNER JOIN CountryIDTable ON CountryIDTable.Country_ID = AirQualityTable.Country_ID INNER JOIN ContinentIDTable ON CountryIDTable.Continent_ID = ContinentIDTable.Continent_ID")
    # cur.execute('''SELECT CountryIDTable.Country ContinentIDTable.Continent MortalityRateTable.MortalityRate AirQualityTable.AirQualityIndex FROM AirQualityTable INNER JOIN MortalityRateTable ON AirQualityTable.Country_ID = MortalityRateTable.Country_ID INNER JOIN CountryIDTable ON CountryIDTable.Country_ID = AirQualityTable.Country_ID INNER JOIN ContinentIDTable ON CountryIDTable.Continent_ID = ContinentIDTable.Continent_ID''')
    # cur.execute("SELECT CountryIDTable.Country ContinentIDTable.Continent MortalityRateTable.MortalityRate AirQualityTable.AirQualityIndex FROM covid_deaths INNER JOIN rainfall ON covid_deaths.country=rainfall.country_a INNER JOIN covid_confirmed ON covid_deaths.country=covid_confirmed.country_b")
    joined_data = cur.fetchall()
    # print(joined_data)
    return joined_data

def calculate_and_csv(joined_data):
    avg_mortality_rate_by_continent_list = []
    avg_aqi_rate_by_continent_list = []

    avg_mortality_rate_by_continent = {}
    avg_aqi_rate_by_continent = {}
    continent_count = {}

    for continent in joined_data:
        avg_mortality_rate_by_continent[continent[1]] = avg_mortality_rate_by_continent.get(continent[1], 0) + continent[2]
        avg_aqi_rate_by_continent[continent[1]] = avg_aqi_rate_by_continent.get(continent[1], 0) + continent[3]
        continent_count[continent[1]] = continent_count.get(continent[1], 0) + 1
    
    list_of_present_continents = []
    for items in list1:
        if items in avg_mortality_rate_by_continent:
            avg_mort_rate = avg_mortality_rate_by_continent[items] / continent_count[items]
            avg_mortality_rate_by_continent_list.append(avg_mort_rate)
            avg_aqi_rate = avg_aqi_rate_by_continent[items] / continent_count[items]
            avg_aqi_rate_by_continent_list.append(avg_aqi_rate)
            list_of_present_continents.append(items)


    header = ["Continent", "Average Mortality Rate (%)", "Average Air Quality Index"]
    outer = []
    for i in range(0, len(list_of_present_continents)):
        inner = [list_of_present_continents[i], avg_mortality_rate_by_continent_list[i], avg_aqi_rate_by_continent_list[i]]
        outer.append(inner)
    with open('continent-mortality-aqi.csv', 'w') as f:
      
    # using csv.writer method from CSV package
        write = csv.writer(f)
        
        write.writerow(header)
        write.writerows(outer)


    X_axis = np.arange(6)
    
    plt.bar(X_axis - 0.2, avg_mortality_rate_by_continent_list, 0.4, label = 'Average Mortality Rate (%)' )
    plt.bar(X_axis + 0.2, avg_aqi_rate_by_continent_list, 0.4, label = 'Average Air Quality Index')
    
    plt.xticks(X_axis, list_of_present_continents)
    plt.xlabel("Continent")
    plt.title("Average Mortality Rate and Average Air Quality Index per Continent")
    plt.legend()
    plt.savefig("mortality-aqi-continent.png")
    plt.close()
          

    goodIndex = 0
    moderateIndex = 0
    unhealthyForSensitiveGroupsIndex = 0
    unhealthyIndex = 0 
    veryUnhealthyIndex = 0 
    hazardousIndex = 0 

    for aqi in list8:
        if aqi >= 0 and aqi <= 50:
            goodIndex = goodIndex + 1
        if aqi >= 51 and aqi <= 100:
            moderateIndex = moderateIndex + 1
        if aqi >= 101 and aqi <= 150:
            unhealthyForSensitiveGroupsIndex = unhealthyForSensitiveGroupsIndex + 1
        if aqi >= 151 and aqi <= 200:
            unhealthyIndex = unhealthyIndex + 1
        if aqi >= 201 and aqi <= 300:
            veryUnhealthyIndex = veryUnhealthyIndex + 1
        if aqi >= 301:
            hazardousIndex = hazardousIndex + 1
    
    level_of_health_concern_list = []
    level_of_health_concern_list.append(goodIndex)
    level_of_health_concern_list.append(moderateIndex)
    level_of_health_concern_list.append(unhealthyForSensitiveGroupsIndex)
    level_of_health_concern_list.append(unhealthyIndex)
    level_of_health_concern_list.append(veryUnhealthyIndex)
    level_of_health_concern_list.append(hazardousIndex)

    level_of_health_concern_catergory_list = ["Good", "Moderate", "Unhealthy for \n Sensitive Groups", "Unhealthy", "Very Unhealthy", "Hazardous"]

    header = ["Air Quality Index Level of Health Concern", "Number of Countries"]
    outer = []
    for i in range(0, len(level_of_health_concern_catergory_list)):
        inner = [level_of_health_concern_catergory_list[i], level_of_health_concern_list[i]]
        outer.append(inner)
    with open('aqi-across-countries.csv', 'w') as f:
      
    # using csv.writer method from CSV package
        write = csv.writer(f)
        
        write.writerow(header)
        write.writerows(outer)

    

    X_axis = np.arange(6)
    
    plt.bar(X_axis, level_of_health_concern_list, 0.4, label = 'Number of Countries in Distinct AQI Levels' )
    
    plt.xticks(X_axis, level_of_health_concern_catergory_list, rotation = 45)
    # plt.plot(range(100))

    # plt.axes().xaxis.set_major_locator(FixedLocator(range(0, 100, 10)))
    # plt.axes().xaxis.set_minor_locator(FixedLocator(range(5, 100, 10)))
    # plt.axes().xaxis.set_minor_formatter(FormatStrFormatter("%d"))
    # plt.axes().tick_params(which='major', pad=20, axis='x')
    plt.xlabel("Air Quality Index Levels")
    plt.ylabel("Number of Countries")
    plt.title("Number of Countries in Distinct Air Quality Index Levels")
    plt.legend()
    plt.savefig("aqi-index-countries-count.png", bbox_inches = 'tight')
    plt.close()


if os.path.exists("mortality-aqi-continent.png"):
    os.remove("mortality-aqi-continent.png")
if os.path.exists("aqi-index-countries-count.png"):
    os.remove("aqi-index-countries-count.png")

def main():

    cur, conn = setUpDatabase('air_quality_vs_mortality_rate.db')

    printAll = False

    acct = input("Enter RESET to clear the database or CONTINUE to add 25 more values to the tables (if availible): ")
    if acct == "reset" or acct == "Reset" or acct == "RESET":
        cur.execute('DROP TABLE IF EXISTS MortalityRateTable')
        cur.execute('DROP TABLE IF EXISTS CountryIDTable')
        cur.execute('DROP TABLE IF EXISTS ContinentIDTable')
        cur.execute('DROP TABLE IF EXISTS AirQualityTable')
        acct1 = input("Enter ALL to print all the data or 25 to print 25 lines of code: ")

        if acct1 == "ALL" or acct1 == "All" or acct1 == "all":
            printAll = True
        else:
            printAll = False

    cur.execute('CREATE TABLE IF NOT EXISTS MortalityRateTable (Country_ID INTEGER, MortalityRate INTEGER)')
    cur.execute('CREATE TABLE IF NOT EXISTS CountryIDTable (Country_ID INTEGER, Country TEXT, Continent_ID INTEGER)')
    cur.execute('CREATE TABLE IF NOT EXISTS ContinentIDTable (Continent_ID INTEGER, Continent TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS AirQualityTable (Country_ID INTEGER, AirQualityIndex INTEGER)')

    create_continent_table(cur, conn)
    create_country_and_mortality_table(cur, conn)
    create_aqi_table(cur, conn)

    add_to_table(printAll, cur, conn)
    add_to_table1(printAll, cur, conn)
    add_to_table2(printAll, cur, conn)
    add_to_table3(printAll, cur, conn)

    joined_data = join_tables(cur, conn)
    calculate_and_csv(joined_data)




if __name__ == "__main__":
    main()