# -*- coding: utf-8 -*-
"""
Created on Tue Jan  4 10:46:29 2022

@author: nishi
"""
import re
from io import StringIO
import os
import requests
import pandas as pd
import datetime
# from datetime import  timedelta
# %% file dwnld


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


start_date = datetime.date(2020, 1, 1)
end_date = datetime.date(2021, 12, 31)


for date in daterange(start_date, end_date):
    # check for friday
    if date.weekday() == 4:
        # print(date.strftime("%Y%m%d"))
        report_date = date.strftime("%Y%m%d")
        URL = f"https://marketdata.theocc.com/weekly-volume-reports?reportDate={report_date}&reportType=options&reportClass=equity&format=csv"

        res = requests.get(URL)
        if res.text != 'Report Date is invalid.\r\n':
            with open(f'data\OCC_weekly_{report_date}.txt', 'w') as f:
                f.write(res.text)
                print(report_date)

# %%


# for file in os.listdir(r"C:\Users\nishi\OneDrive - University of St Andrews\Lecture Notes\Research StARIS\data\raw_OCC"):
directory = os.fsencode(
    r"C:\Users\nishi\OneDrive - University of St Andrews\Lecture Notes\Research StARIS\data\raw_OCC")


dfs = []
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    # print(filename)
    with open(f'data\\raw_OCC\{filename}', 'r') as f:
        report_date = re.findall(r'\d+', filename)[0]

        # porc = ''

        lines = f.readlines()
        data = dict(
            calls=lines[14:28],
            puts=lines[30:44],
            comb=lines[46:60])

        cols = lines[10].split(',')

        for key, vals in data.items():
            rows = lines[10]
            for line in vals:
                rows += line

            df = pd.read_csv(StringIO(rows))
            df['Total Contracts'] = df['Total Contract'].apply(
                lambda x: int(x.replace(',', '')))

            df['porc'] = key
            df['ds'] = datetime.datetime.strptime(report_date, "%Y%m%d")
        dfs.append(df)
        # df.to_csv('data\OCC_Weekly-2020Jan-2021Dec.csv',
        #               mode='a')
        # rows = [line.split(',') for line in lines]
        # pd.Data
OCC_weekly_df = pd.concat(dfs)

OCC_weekly_df.to_csv('data\OCC_Weekly-2020Jan-2021Dec.csv', index=False)
