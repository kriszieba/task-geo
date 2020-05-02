# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 15:07:03 2020

@author: Kristoffer Jan Zieba
"""

import io
import pandas as pd
import requests
import datetime
from datetime import date

url_deaths = ('https://raw.githubusercontent.com/elinlutz/gatsby-map/master/src/data/time_series/time_series_deaths-deaths.csv')
url_cases = ('https://raw.githubusercontent.com/elinlutz/gatsby-map/master/src/data/time_series/time_series_confimed-confirmed.csv')
 
country_subdivisions_df = pd.DataFrame({
        'subdivision_category': ['county', 'county', 'county', 'county', 
                                 'county', 'county', 'county', 'county',
                                 'county', 'county', 'county', 'county',
                                 'county', 'county', 'county', 'county',
                                 'county', 'county', 'county', 'county',
                                 'county'],
        '3166-2_iso': ['SE-K', 'SE-W', 'SE-I', 'SE-X', 'SE-N', 'SE-Z', 'SE-F',
                       'SE-H', 'SE-G', 'SE-BD', 'SE-M', 'SE-AB', 'SE-D',
                       'SE-C', 'SE-S', 'SE-AC', 'SE-Y', 'SE-U', 'SE-O', 'SE-T',
                       'SE-E'],
        'subdivision_name': ['Blekinge län [SE-10]', 'Dalarnas län [SE-20]',
                             'Gotlands län [SE-09]', 'Gävleborgs län [SE-21]',
                             'Hallands län [SE-13]', 'Jämtlands län [SE-23]',
                             'Jönköpings län [SE-06]', 'Kalmar län [SE-08]',
                             'Kronobergs län [SE-07]', 'Norrbottens län [SE-25]',
                             'Skåne län [SE-12]', 'Stockholms län [SE-01]',
                             'Södermanlands län [SE-04]', 'Uppsala län [SE-03]', 
                             'Värmlands län [SE-17]', 'Västerbottens län [SE-24]',
                             'Västernorrlands län [SE-22]', 'Västmanlands län [SE-19]',
                             'Västra Götalands län [SE-14]', 'Örebro län [SE-18]',
                             'Östergötlands län [SE-05]'],
        'Display_Name': ['Blekinge', 'Dalarna', 'Gotland', 'Gävleborg',
                         'Halland', 'Jämtland', 'Jönköping', 'Kalmar län',
                         'Kronoberg', 'Norrbotten', 'Skåne', 'Stockholm',
                         'Sörmland', 'Uppsala', 'Värmland', 'Västerbotten',
                         'Västernorrland', 'Västmanland', 'Västra Götaland',
                         'Örebro', 'Östergötland']})

def sw_covidata():
    """Data Source for the Swedish COVID-19 Data.
    Arguments:
        None
    Returns:
        pandas.DataFrame
    """
    dataset_deaths, dataset_cases = sw_covidata_connector()
    return sw_covidata_formatter(dataset_deaths, dataset_cases)


def sw_covidata_connector():
    """Extracts data from www.coronakartan.se's Github repository.
    Description:
        - Downloads the URL's data in a Unicode CSV Format
        - Unicode CSV Format: UTF-8
    Returns:
        2 datasets (DataFrame with CSV Data)
    """
    
    urlData_deaths = requests.get(url_deaths).content
    dataset_deaths = pd.read_csv(io.StringIO(urlData_deaths.decode('utf-8')))
    urlData_cases = requests.get(url_cases).content
    dataset_cases = pd.read_csv(io.StringIO(urlData_cases.decode('utf-8')))

    return dataset_deaths, dataset_cases


def sw_covidata_formatter(dataset_deaths, dataset_cases):
    """Formatter for SW COVID-19 Data.
    Arguments:
        2 datasets(pandas.DataFrame): Data as returned by sw_covidata_connector.
    Description:
        - Drops unnecessary rows for this dataset (totals, FHM_Deaths_Today,
        At_Hospital, At_ICU, Hospital_Total, ICU_Capacity_2017, FHM_ICU_Est, 
        Region_Deaths, FHM_Deaths_Today, Diff). FHM = Public Health Agency of 
        Sweden
        -Changes structure of the downloaded datasets (one row: one region) to 
        a new structure (one row: one date-one region) by pandas.melt  
        - Rename column titles,
        - Add a country column (Sweden) and geographical/political subdivisions
    Returns:
        dataset(pandas.DataFrame)
    """
    
    # Returns column names that refer to dates
    def get_date_cols(df):
        date_cols = []
        for c in df:
           try:
                datetime.datetime.strptime(c, '%Y-%m-%d')
                date_cols.append(c)
           except  ValueError:
               pass
        return date_cols
    
    def transf_dataset(raw_dataset):
        df = raw_dataset.copy()
        # drop lines: unknown, total, todays total
        df.drop([21,22,23], axis=0, inplace=True)
        # Rename column name "Today" to date 
        df.rename(columns={'Today': date.today().isoformat()}, inplace=True)
        # Add columns about geographical/political subdivisions
        df = df.merge(country_subdivisions_df, on=['Display_Name'])
        cols = ['Display_Name', 'Lat', 'Long', 
                'subdivision_category', '3166-2_iso', 'subdivision_name']
        dataset_transf = pd.melt(
                df, value_vars=get_date_cols(df), var_name='date', 
                value_name='number', id_vars=cols)
        return dataset_transf
        
    deaths = transf_dataset(dataset_deaths)
    deaths.rename(columns={'number':'deaths'}, inplace=True)
    cases = transf_dataset(dataset_cases)
    cases.rename(columns={'number':'cases'}, inplace=True)
    
    dataset = pd.merge(
            deaths, cases, on=['Display_Name', 'Lat', 'Long', 
                               'subdivision_category', '3166-2_iso', 
                               'subdivision_name', 'date'])
    dataset['date'] = pd.to_datetime(dataset['date'] , format='%Y-%m-%d')
    dataset['country'] = 'Sweden'
    dataset.rename(
            columns={'Display_Name': 'subdivision_name_dataset', 
                     'Lat': 'latitude', 'Long': 'longitude'}, inplace=True)
    dataset = dataset[['country', 'subdivision_category', 'subdivision_name', 
                       'subdivision_name_dataset', '3166-2_iso', 'latitude', 
                       'longitude', 'date', 'cases', 'deaths']]
    
    return dataset