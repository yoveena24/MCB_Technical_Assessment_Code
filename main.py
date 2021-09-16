"""
Title: Software Engineer Data Practical Test Paper
Author: Yoveena Vencatasamy
Last modified date: 16.08.2021
"""
import json
import sqlite3
from sqlite3 import Error
import pandas as pd
from pycountry_convert import country_name_to_country_alpha2
import requests


#Task 1
def create_connection(path):
    """
    This function creates connection to sqlite in path given
    :param path:
    :return:
    """
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    """
    This function performs a DB query on the given connection
    :param connection:
    :param query:
    :return:
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
    except Error as e:
        print(f"The error '{e}' occurred")

def execute_read_query(connection, query):
    """
    This function performs a select query on the DB connection
    and returns the result as a list
    :param connection:
    :param query:
    :return:
    """
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")

def create_table(connection):
    """
    This function creates a table with columns based on the data file given
    :param connection:
    :return:
    """
    create_HR_table = """
    CREATE TABLE IF NOT EXISTS happiness_report (
      year TEXT,
      country TEXT,
      country_url TEXT,
      region_code INTEGER,
      region TEXT,
      rank_per_region INTEGER,
      overall_rank INTEGER,
      happiness_score REAL,
      happiness_status TEXT,
      gdp_per_capita REAL,
      family REAL,
      social_support REAL,
      healthy_life_expectancy REAL,
      freedom_life_choices REAL,
      generosity REAL,
      corruption REAL,
      PRIMARY KEY (country, year)
    );
    """
    execute_query(connection, create_HR_table)
    return "Task 1: Table created in sqlite database. Columns have been adjusted to implement Task 3"

#Task 2
def readfile(connection,path, filenames,years):
    """
    This function consumes data files and loads them in happiness_report table
    :param connection:
    :param path:
    :param filenames:
    :param years:
    :return:
    """
    for i in range(len(filenames)):
        df = pd.read_csv(path + "\\" + filenames[i])
        for j in range(df.shape[0]):
            if (years[i] == '2018') or (years[i] == '2019'):
                data = (years[i], df.iloc[j][0], None, None, None, None, None, df.iloc[j][1], None, df.iloc[j][2], None, df.iloc[j][3], df.iloc[j][4], df.iloc[j][5],df.iloc[j][6], df.iloc[j][7])
            else:
                data = (years[i], df.iloc[j][0],None, None, None, None, None, df.iloc[j][1], None, df.iloc[j][4], df.iloc[j][5], None, df.iloc[j][6], df.iloc[j][7],df.iloc[j][8], df.iloc[j][9])

            insert_data = "INSERT INTO happiness_report (year, country, country_url, region_code,region, rank_per_region, overall_rank, happiness_score, happiness_status, gdp_per_capita, family, social_support, healthy_life_expectancy, freedom_life_choices, generosity, corruption) VALUES (?,?,?,?,?,?, ?, ?, ?, ?,?,?,?,?,?,?)"
            cursor = connection.cursor()
            cursor.execute(insert_data,data)
            connection.commit()
    return "Task 2: Automated data pipeline created"

#Task 3
def getJsondata(connection, path, filename):
    """
    This function consumes json file given and loads the data in happiness_report table
    :param connection:
    :param path:
    :param filename:
    :return:
    """
    f = open(path + "\\" + filename)
    data = json.load(f)
    for i in data:
        select_country_json = 'SELECT year,country,happiness_score from happiness_report where country = "'+ i['country'] + '"'
        list_country_json = execute_read_query(connection, select_country_json)
        if len(list_country_json) > 0:
            update_data = """UPDATE happiness_report SET country_url = ? , region_code = ? , region = ? WHERE country = ?"""
            if i['region'] == "":
                data = (i['image_url'],i['region-code'], 'Nan', i['country'] )
            else:
                data = (i['image_url'], i['region-code'], i['region'].upper(), i['country'])
            cursor = connection.cursor()
            cursor.execute(update_data,data)
            connection.commit()

    # update happiness_status
    select_country_yr = 'SELECT year,country,happiness_score from happiness_report'
    list_country_yr = execute_read_query(connection, select_country_yr)
    for j in list_country_yr:
        happiness_score = j[2]
        if happiness_score > 5.6:
            status = "Green"
        elif happiness_score >= 2.6 and happiness_score <= 5.6:
            status = "Amber"
        else:
            status = "Red"
        upd_happiness_status = """UPDATE happiness_report SET happiness_status = ? WHERE country = ? and year = ?"""
        cursor = connection.cursor()
        cursor.execute(upd_happiness_status, (status, j[1], j[0]))
        connection.commit()

    # update countries region to Nan where countries not available in json file
    update_remaning = """UPDATE happiness_report SET region = 'Nan' WHERE region is NULL"""
    cursor = connection.cursor()
    cursor.execute(update_remaning)
    connection.commit()
    # Closing file
    f.close()

def getRankings (connection):
    """
    This function updates happiness_report table with rank per region per year and overall rank per year
    :param connection:
    :return:
    """
    select_overall_rank = 'select year, country, happiness_score, RANK() OVER ( PARTITION BY year ORDER BY happiness_score DESC) overall_rank from happiness_report'
    select_rank_region = "select * from (select year, country, region, happiness_score, RANK() OVER ( PARTITION BY year,region ORDER BY happiness_score DESC) rank_per_region from happiness_report) where region!='Nan'"
    overall_rank = execute_read_query(connection, select_overall_rank)
    rank_per_region = execute_read_query(connection, select_rank_region)
    for row in overall_rank:
        update_data = """UPDATE happiness_report SET overall_rank = ? WHERE country = ? and year = ?"""
        cursor = connection.cursor()
        cursor.execute(update_data, (row[3], row[1], row[0]))
        connection.commit()

    for row in rank_per_region:
        update_data = """UPDATE happiness_report SET rank_per_region = ? WHERE country = ? and year = ?"""
        cursor = connection.cursor()
        cursor.execute(update_data, (row[4], row[1], row[0]))
        connection.commit()

def output_csv_parquet (connection,path, filename):
    """
    This function returns the required information for Task 3 in both csv and parquet format
    :param connection:
    :param path:
    :param filename:
    :return:
    """
    db_df = pd.read_sql_query("SELECT * FROM happiness_report", connection)

    # put data to csv file
    db_df.to_csv(path + "\\" + filename +".csv", index=False)

    #put data to parquet format
    db_df.to_parquet(path + "\\" + filename +".parquet")

    return "Task 3: Modelling record in csv and parquet format created"

# Task 4
# summary extract in json
def create_extract(connection,path,filename):
    """
    This function creates a json extract with the information required in Task 4
    :param connection:
    :param path:
    :param filename:
    :return:
    """
    select_countries = "select distinct country from happiness_report"
    list_countries = execute_read_query(connection, select_countries)
    json_data = {}
    json_data['extract']=[]
    for i in list_countries:
        select_h_rank = "select country, min(overall_rank), max(overall_rank), max(happiness_score), min(happiness_score) from happiness_report where country = '" + i[0] +"'"
        query_res = execute_read_query(connection,select_h_rank)
        json_data['extract'].append({
            'country': query_res[0][0],
            'highest_rank': query_res[0][1],
            'lowest_rank': query_res[0][2],
            'highest_happiness_score' : query_res[0][3],
            'lowest_happiness_score': query_res[0][4]
        })
    with open(path + "\\" + filename, 'w') as outfile:
        json.dump(json_data, outfile,ensure_ascii=False, indent=4)
    return "Task 4: Extract in json format created"

# Task 5
def create_dataset(connection,path,filename):
    """
    This function creates a dataset to be used for creating a small data visualisation dashboard
    :param connection:
    :param path:
    :param filename:
    :return:
    """
    db_df = pd.read_sql_query("SELECT year, country, happiness_score, happiness_status FROM happiness_report", connection)
    # put data to csv file
    db_df.to_csv(path + "\\" + filename , index=False)
    return "Task 5: Dataset containing details created"


# Task 6
def callAPI(connection):
    """
    This function adds 3 new columns to happiness_report and calls world bank data api
    to get capital city, longitude, latitude
    :param connection:
    :return:
    """
    addColumn1 = "ALTER TABLE happiness_report ADD COLUMN capital_city TEXT"
    addColumn2 = "ALTER TABLE happiness_report ADD COLUMN longitude REAL"
    addColumn3 = "ALTER TABLE happiness_report ADD COLUMN latitude REAL"
    cursor = connection.cursor()
    cursor.execute(addColumn1)
    cursor.execute(addColumn2)
    cursor.execute(addColumn3)
    connection.commit()

    # convert to alpha2 country codes
    select_countries = "select distinct country from happiness_report"
    list_countries = execute_read_query(connection, select_countries)
    alpha2_code = []
    for i in list_countries:
        try:
            cn_a2_code = country_name_to_country_alpha2(i[0])
        except:
            cn_a2_code = 'Unknown'
        alpha2_code.append((i[0],cn_a2_code))

    for j in alpha2_code:
        if j[1] != 'Unknown':
            response = requests.get("http://api.worldbank.org/v2/country/"+ j[1].lower()+"?format=json")
            data = (response.json()[1][0]['capitalCity'], response.json()[1][0]['longitude'],response.json()[1][0]['latitude'],j[0])
            upd_happiness_status = """UPDATE happiness_report SET capital_city = ?, longitude = ?, latitude = ? WHERE country = ?"""
            cursor = connection.cursor()
            cursor.execute(upd_happiness_status,data)
            connection.commit()
    return "Task 6: Three new columns capital_city, longitude, latitude added"

def runfunctions(connection, path, filenames,years):
    """
    This function calls all implemented functions to perform the required tasks of the assessment
    :param connection:
    :param path:
    :param filenames:
    :param years:
    :return:
    """
    print(create_table(connection))
    print(readfile(connection, path, filenames, years))
    getJsondata(connection, path, "countries_continents_codes_flags_url.json")
    getRankings(connection)
    print(output_csv_parquet(connection, path, "modelling_record"))
    print(create_extract(connection,path,"extract.json"))
    print(create_dataset(connection,path,"dataset.csv"))
    print(callAPI(connection))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    connection = create_connection("D:\\MCB\\connection.db")
    filenames = ["HR_2016.csv","happiNess_report_2017.csv","2018.csv","report_2019.csv"]
    years = ['2016','2017','2018','2019']
    path = "D:\MCB\Technical_Assessment_-_SE_(DATA)_-_Sept_2021\Data Files"
    runfunctions(connection,path,filenames,years)

    # get column names
    # cursor = connection.execute("SELECT * from happiness_report")
    # names = list(map(lambda x: x[0], cursor.description))
    # print(names)

    # delete_comment = "DROP TABLE happiness_report"
    # execute_query(connection, delete_comment)

    # select_users = "SELECT * from happiness_report limit 10"
    # users = execute_read_query(connection, select_users)
    # for user in users:
    #     print(user)



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
