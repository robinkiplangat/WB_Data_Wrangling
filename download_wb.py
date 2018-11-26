
# coding: utf-8

import pandas as pd 
import wbdata
import os

#   Access WB Data
countries = {"Burkina Faso":"BF", "Congo, Dem. Rep.":"CD", "Ethiopia":"ET", "Kenya":"KE", "Nigeria":"NG", 
          "Senegal":"SN", "Tanzania":"TZ", "Uganda":"UG", "South Africa":"ZA", "Zambia":"ZM"}

country_code = list({v for (k,v) in countries.items()})
country_name = list({k for (k,v) in countries.items()})

def collect():
    # generate a dict from the indicators file
    takwimu_indicators = pd.read_csv('key/takwimu_indicators.csv',
                                     index_col=0, squeeze=True).to_dict()
    # Gather indicator data on the selected countries
    data = wbdata.get_dataframe(takwimu_indicators, 
                                 country=country_code, convert_date=False)
    return data.to_csv('data/takwimu_worldbank_data.csv')

# def process_wb_data():
#     for x in country_name:
#         name = x+'.csv'
#         directory = 'data/'+x
#         if not os.path.exists(directory):
#             os.makedirs(directory)
#         data.loc[x].to_csv(directory+'/'+name)

#  Structure into Hurumap format

data = pd.read_csv('data/takwimu_worldbank_data.csv')
columns = ['geography','date','male','female']
melted_columns = ['geography','geo_version','gender','total']

#   population
def population(): 
    df = data[['country', 'date','Population Male', 'PopulationFemale' ]].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    population = df[melted_columns]
    
    return population

#   basic services
def basic_services():
    df = data[['country', 'date','access to basic services - Electricity','access to basic services - Water' ]].dropna(axis=0)
    df = df[df['date']== df['date'].max()]
    df.columns = ['geography','date','electricity','water']
    df = df.melt(id_vars=['geography','date'], value_vars=['electricity','water'],
            var_name='service', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    basic_services = df[['geography','geo_version','service','total']]
   
    return basic_services

#   youth unemployment
def youth_unemployment():

    df = data[['country', 'date','Youth unemployment-Male','Youth unemployment - Female' ]].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    youth_unemployment = df[melted_columns]

    return youth_unemployment

#   Life expectancy
def life_expectancy():

    df = data[['country','date','Life expectancy-Male','Life expectancy-Female']].dropna(axis=0) 
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    life_expectancy = df[melted_columns]
    return life_expectancy

#   infant & Under-5 motality (per 1000)
def infant_under_5_mortality():
    df = data[['country', 'date','Infant Mortality','Under 5 Mortality rates']].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    infant_under_5_mortality = df[melted_columns]

    return infant_under_5_mortality


#   Prevalence of HIV
def hiv_prevalence():
    df = data[['country', 'date','Prevalence of HIV, male (% ages 15-24)','Prevalence of HIV, female (% ages 15-24)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    hiv_prevalence = df[melted_columns]

    return hiv_prevalence

#   Primary completion rate
def primary_completion():
    df = data[['country', 'date','Primary completion rate, male (%)','Primary completion rate, female (%)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    primary_completion = df[melted_columns]

    return primary_completion

#   Employment to population ratio
def employment_to_population():
    df = data[['country', 'date','Employment to population ratio male (%)','Employment to population ratio female (%)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    employment_to_population = df[melted_columns]

    return employment_to_population

#   Physicians ,Nurses and Mid wives per 1000
def health_staff():

    df = data[['country', 'date','Physicians per 1000','Nurses and Mid wives']].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = ['geography','date','physician','nurses_and_midwives' ]
    df = df.melt(id_vars=['geography','date'], value_vars=['physicians','nurses_and_midwives'],
            var_name='health_staff', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    health_staff = df[['geography','geo_version','health_staff','total']]

    return health_staff

#   Account ownership
def acc_ownership():
    df = data[['country', 'date','Account ownership,male (% of population ages 15+)','Account ownership,female (% of population ages 15+)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    acc_ownership = df[melted_columns]
    return acc_ownership

#   School enrollment, primary
def primary_school_enrollment():

    df = data[['country', 'date','School enrollment, primary, male (% gross)','School enrollment, primary, female (% gross)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    primary_school_enrollment = df[melted_columns]

    return primary_school_enrollment

#   Secondary school enrolment
def secondary_school_enrollment():

    df = data[['country', 'date','Secondary school enrolment - Male (% gross)','Secondary school enrolment - Female (% gross)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    secondary_school_enrollment = df[melted_columns]

    return secondary_school_enrollment

#     Literacy rate
def literacy_rate():
    df = data[['country', 'date','Literacy rate - Male','Literacy rate - Female']].dropna(axis=0)
    df = df[df['date']== df['date'].max()] # most recent values
    df.columns = columns
    df = df.melt(id_vars=['geography','date'], value_vars=['male','female'],
            var_name='gender', value_name='total').sort_values('geography')
    df = df.rename(columns={"date": "geo_version"})
    literacy_rate = df[melted_columns]
    return literacy_rate

#   export datasets to csv
def save_to_csv():  
    population().to_csv('data/population.csv',index=False)
    basic_services().to_csv('data/basic_services.csv',index=False)
    youth_unemployment().to_csv('data/youth_unemployment.csv',index=False)
    life_expectancy().to_csv('data/life_expectancy.csv',index=False)
    infant_under_5_mortality().to_csv('data/infant_under_5_mortality.csv',index=False)
    hiv_prevalence().to_csv('data/hiv_prevalence.csv',index=False)
    primary_completion().to_csv('data/primary_completion.csv',index=False)
    employment_to_population().to_csv('data/employment_to_population.csv',index=False)
    health_staff().to_csv('data/health_staff.csv',index=False)
    acc_ownership().to_csv('data/acc_ownership.csv',index=False)   
    primary_school_enrollment().to_csv('data/primary_school_enrollment.csv',index=False)
    secondary_school_enrollment().to_csv('data/secondary_school_enrollment.csv',index=False)
    literacy_rate().to_csv('data/literacy_rate.csv',index=False)

    
    
if __name__ == "__main__":
    save_to_csv()

