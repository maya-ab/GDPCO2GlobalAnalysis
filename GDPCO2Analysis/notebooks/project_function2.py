import pandas as pd
import numpy as np

#Method Chaining 
def load_and_process_emissions2(url_or_path_to_csv_file):

    # Load data and deal with missing data and remove data that is not useful
    dataEmissions = pd.read_csv(url_or_path_to_csv_file, skiprows=4) 

    dataEmissions = (
        dataEmissions
        .drop(dataEmissions.columns[[1,2,3,4,5,6,7,8,9,10,11,12,
                                     13 ,14,15,16,17,18,19,20,21,
                                     22,23, 24,25,26,27,28,29,30,31,32,33]], axis=1)
        .drop(dataEmissions.index[[1,3, 7, 36, 49, 61, 62,63, 64, 65, 73, 
                             74, 95, 98, 102, 103, 104, 105, 107, 110, 
                             128, 134, 135, 136, 139, 140, 142, 153, 156, 161,
                             181, 183, 191, 198, 215, 218, 230, 231, 236, 
                             238, 240, 241, 249, 259]])
        .melt(id_vars = ["Country Name"])
        .rename(columns={'variable': 'Year',
                         'value': 'CO2 emissions'})
        .dropna(subset=['CO2 emissions'])  
        
    )
    
    dataEmissions['Year'] = dataEmissions['Year'].astype(int)
    dataEmissions.to_csv(r'../data/processed/modifiedEmissions.csv', index=False)

    return dataEmissions 


def load_and_process_GDP2(url_or_path_to_csv_file):
    dataGDP = pd.read_csv(url_or_path_to_csv_file)
    
    dataGDP = (
        dataGDP
        .rename(columns={dataGDP.columns[0]: 'Country Name'})
    
    )
    
    dataGDP['Year'] = dataGDP['Year'].astype(int)
    dataGDP.to_csv(r'../data/processed/modifiedGDP.csv', index=False)
    
    return dataGDP 
     
        
def load_and_process_GDP_Emissions2(emissionsUrl, gdpUrl):
    
    dataEmissions = pd.read_csv(emissionsUrl)
    dataGDP = pd.read_csv(gdpUrl)
    dataGDP = dataGDP.dropna(subset=['Code'])
    
    gdp2019 = dataGDP.loc[(dataGDP['Year'] == 2019) ]
    gdp2019Top = gdp2019.sort_values(by='GDP per capita, PPP (constant 2017 international $)', ascending=False).head(15)
    gdp2019Top = gdp2019Top['Country Name'].reset_index(drop=True)
    gdp2019Top
    
    gdp2019Bottom = dataGDP.loc[(dataGDP['Year'] == 2019) ]
    gdp2019Bottom = gdp2019Bottom.sort_values(by='GDP per capita, PPP (constant 2017 international $)', ascending=True).head(15)
    gdp2019Bottom = gdp2019Bottom['Country Name'].reset_index(drop=True)
    gdp2019Bottom
    
    def isTop15(row):
        if row['Country Name'] in gdp2019Top:
            return "Top 15"
        if row['Country Name'] in gdp2019Bottom:
            return "Bottom 15"
        else:
            return "Neither" 
    
   #Drop empty cells and merge the two dfs  
    mergeData = (pd.merge(dataGDP, dataEmissions, 
                         on=['Country Name', 'Year'],
                         how = 'left')
                 .dropna(subset=['CO2 emissions'])
                )
    #Create new column
    mergeData['GDP - PPP Ranking'] = mergeData.apply(isTop15, axis=1)

    
    mergeData.to_csv(r'../data/processed/CO2AndGDPAnalysis.csv', index=False)

    return mergeData