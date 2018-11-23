
# coding: utf-8

import pandas as pd 
import wbdata
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('key/takwimu-001-c43ff4f1160f.json', scope)
gc = gspread.authorize(credentials)

# ## Access WB Data
countries = {"Burkina Faso":"BF", "Congo, Dem. Rep.":"CD", "Ethiopia":"ET", "Kenya":"KE", "Nigeria":"NG", 
          "Senegal":"SN", "Tanzania":"TZ", "Uganda":"UG", "South Africa":"ZA", "Zambia":"ZM"}

country_code = list({v for (k,v) in countries.items()})

def collect():
    # generate a dict from the indicators file
    takwimu_indicators = pd.read_csv('key/takwimu_indicators.csv',
                                     index_col=0, squeeze=True).to_dict()
    # Gather indicator data on the selected countries
    wb_data = wbdata.get_dataframe(takwimu_indicators, 
                                 country=country_code, convert_date=False)
    return wb_data    

## Structure into Hurumap format

data = pd.read_csv('data/takwimu_wb_data.csv')
# takwimu_Sheet = gc.create('Takwimu_WB spreadsheet')
# takwimu_Sheet.share('robyne.kiplangat@gmail.com', perm_type='user', role='owner')

takwimu_Sheet = gc.open('Takwimu_WB_spreadsheet')

takwimu_Sheet.worksheets()

#      population
def population():
    
    df = data[['country', 'date','Population Male', 'PopulationFemale' ]].dropna(axis=0)
    df = df[df['date']== df['date'].max()]
    df.columns = ['name','date','male','female']

    df = df.melt(id_vars=['name','date'], value_vars=['male','female'],
            var_name='sex', value_name='total')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})

    population = df[['geo_level','geo_code','name',
                                   'geo_version','sex','total']].sort_values('name')
    try:
        sheet = takwimu_Sheet.worksheet('population')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='population', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('population')

            population = gd.set_with_dataframe(sheet, population, include_index=False,
                          include_column_header=True,resize=False)
            return population

    return population

#     basic services
def basic_services():
    df = data[['country', 'date','access to basic services - Electricity','access to basic services - Water' ]].dropna(axis=0)
    df = df[df['date']== df['date'].max()]
    df.columns = ['name','date','electricity','water']

    df = df.melt(id_vars=['name','date'], value_vars=['electricity','water'],
            var_name='service', value_name='total')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})

    basic_services = df[['geo_level','geo_code','name',
                         'geo_version','service','total']].sort_values('name')

    try:
        sheet = takwimu_Sheet.worksheet('basic_services')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='basic_services', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('basic_services')

            basic_services = gd.set_with_dataframe(sheet, basic_services, include_index=False,
                          include_column_header=True,resize=False)
            return basic_services

    return basic_services

#     youth unemployment
def youth_unemployment():

    df = data[['country', 'date','Youth unemployment-Male','Youth unemployment - Female' ]].dropna(axis=0)
    df = df[df['date']== df['date'].max()]
    df.columns = ['name','date','male','female']


    df = df.melt(id_vars=['name', 'date'], value_vars=['male','female'],
            var_name='sex', value_name='total')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})
    youth_unemployment = df[['geo_level','geo_code','name',
                             'geo_version','sex','total']].sort_values('name')

    try:
        sheet = takwimu_Sheet.worksheet('youth_unemployment')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='youth_unemployment', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('youth_unemployment')

            youth_unemployment = gd.set_with_dataframe(sheet, youth_unemployment, include_index=False,
                          include_column_header=True,resize=False)
            return youth_unemployment

    return youth_unemployment

#     Life expectancy
def life_expectancy():

    df = data[['country', 'date','Life expectancy-Male','Life expectancy-Female']].dropna(axis=0)
    df = df[df['date']== df['date'].max()]
    df.columns = ['name','date','male','female']


    df = df.melt(id_vars=['name','date'], value_vars=['male','female'],
            var_name='sex', value_name='age')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})

    life_expectancy = df[['geo_level','geo_code','name',
                          'geo_version','sex','age']].sort_values('name')

    try:
        sheet = takwimu_Sheet.worksheet('life_expectancy')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='life_expectancy', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('life_expectancy')

            life_expectancy = gd.set_with_dataframe(sheet, life_expectancy, include_index=False,
                          include_column_header=True,resize=False)
            return life_expectancy

    return life_expectancy

#     infant & Under-5 motality (per 1000)
def infant_under_5_mortality():
    df = data[['country', 'date','Infant Mortality','Under 5 Mortality rates']].dropna(axis=0)
    df = df[df['date']== df['date'].max()]

    df.columns = ['name','date','infant','under_5']

    df = df.melt(id_vars=['name','date'], value_vars=['infant','under_5'],
            var_name='mortality', value_name='rate')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})
    infant_under_5_mortality = df[['geo_level','geo_code','name',
                                   'geo_version','mortality','rate']].sort_values('name')
    try:
        sheet = takwimu_Sheet.worksheet('infant_under_5_mortality')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='infant_under_5_mortality', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('infant_under_5_mortality')

            infant_under_5_mortality = gd.set_with_dataframe(sheet, infant_under_5_mortality, include_index=False,
                          include_column_header=True,resize=False)
            return infant_under_5_mortality

    return infant_under_5_mortality


#     Prevalence of HIV
def hiv_prevalence():
    df = data[['country', 'date','Prevalence of HIV, male (% ages 15-24)','Prevalence of HIV, female (% ages 15-24)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()]

    df.columns = ['name','date','male','female']

    df = df.melt(id_vars=['name','date'], value_vars=['male','female'],
            var_name='sex', value_name='rate')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})
    hiv_prevalence = df[['geo_level','geo_code','name',
                         'geo_version','sex','rate']].sort_values('name')

    try:
        sheet = takwimu_Sheet.worksheet('hiv_prevalence')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='hiv_prevalence', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('hiv_prevalence')

            hiv_prevalence = gd.set_with_dataframe(sheet, hiv_prevalence, include_index=False,
                          include_column_header=True,resize=False)
            return hiv_prevalence

    return hiv_prevalence

#     Primary completion rate
def primary_completion():
    df = data[['country', 'date','Primary completion rate, male (%)','Primary completion rate, female (%)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()]

    df.columns = ['name','date','male','female']

    df = df.melt(id_vars=['name','date'], value_vars=['male','female'],
            var_name='sex', value_name='rate')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})
    primary_completion = df[['geo_level','geo_code','name',
                             'geo_version','sex','rate']].sort_values('name')

    try:
        sheet = takwimu_Sheet.worksheet('primary_completion')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='primary_completion', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('primary_completion')

            primary_completion = gd.set_with_dataframe(sheet, primary_completion, include_index=False,
                          include_column_header=True,resize=False)
            return primary_completion



    return primary_completion

#     Employment to population ratio
def employment_to_population():
    df = data[['country', 'date','Employment to population ratio male (%)','Employment to population ratio female (%)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()]

    df.columns = ['name','date','male','female']

    df = df.melt(id_vars=['name','date'], value_vars=['male','female'],
            var_name='sex', value_name='rate')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})
    employment_to_population = df[['geo_level','geo_code','name',
                                   'geo_version','sex','rate']].sort_values('name')

    try:
        sheet = takwimu_Sheet.worksheet('employment_to_population')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='employment_to_population', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('employment_to_population')

            employment_to_population = gd.set_with_dataframe(sheet, employment_to_population, include_index=False,
                          include_column_header=True,resize=False)
            return employment_to_population

    return employment_to_population

#     Physicians ,Nurses and Mid wives per 1000

def health_staff():

    df = data[['country', 'date','Physicians per 1000','Nurses and Mid wives']].dropna(axis=0)
    df = df[df['date']== df['date'].max()]

    df.columns = ['name','date','physicians','nurses and mid wives']

    df = df.melt(id_vars=['name','date'], value_vars=['physicians','nurses and mid wives'],
            var_name='role', value_name='rate')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})
    health_staff = df[['geo_level','geo_code','name',
                                   'geo_version','role','rate']].sort_values('name')


    try:
        sheet = takwimu_Sheet.worksheet('health_staff')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='health_staff', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('health_staff')

            health_staff = gd.set_with_dataframe(sheet, health_staff, include_index=False,
                          include_column_header=True,resize=False)
            return health_staff


    return health_staff

#     Account ownership
def acc_ownership():
    df = data[['country', 'date','Account ownership,male (% of population ages 15+)','Account ownership,female (% of population ages 15+)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()]

    df.columns = ['name','date','male','female']

    df = df.melt(id_vars=['name','date'], value_vars=['male','female'],
            var_name='sex', value_name='rate')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})
    acc_ownership = df[['geo_level','geo_code','name',
                                   'geo_version','sex','rate']].sort_values('name')

    try:
        sheet = takwimu_Sheet.worksheet('acc_ownership')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='acc_ownership', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('acc_ownership')

            acc_ownership = gd.set_with_dataframe(sheet, acc_ownership, include_index=False,
                          include_column_header=True,resize=False)
            return acc_ownership



    return acc_ownership

#     School enrollment, primary
def primary_school_enrollment():

    df = data[['country', 'date','School enrollment, primary, male (% gross)','School enrollment, primary, female (% gross)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()]

    df.columns = ['name','date','male','female']

    df = df.melt(id_vars=['name','date'], value_vars=['male','female'],
            var_name='sex', value_name='rate')
    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})
    primary_school_enrollment = df[['geo_level','geo_code','name',
                                   'geo_version','sex','rate']].sort_values('name')

    try:
        sheet = takwimu_Sheet.worksheet('primary_school_enrollment')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='primary_school_enrollment', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('primary_school_enrollment')

            primary_school_enrollment = gd.set_with_dataframe(sheet, primary_school_enrollment, include_index=False,
                          include_column_header=True,resize=False)
            return primary_school_enrollment



    return primary_school_enrollment


#     Secondary school enrolment
def secondary_school_enrollment():

    df = data[['country', 'date','Secondary school enrolment - Male (% gross)','Secondary school enrolment - Female (% gross)']].dropna(axis=0)
    df = df[df['date']== df['date'].max()]

    df.columns = ['name','date','male','female']

    df = df.melt(id_vars=['name','date'], value_vars=['male','female'],
            var_name='sex', value_name='rate')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})
    secondary_school_enrollment = df[['geo_level','geo_code','name',
                                   'geo_version','sex','rate']].sort_values('name')

    try:
        sheet = takwimu_Sheet.worksheet('secondary_school_enrollment')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='secondary_school_enrollment', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('secondary_school_enrollment')

            secondary_school_enrollment = gd.set_with_dataframe(sheet, secondary_school_enrollment, include_index=False,
                          include_column_header=True,resize=False)
            return secondary_school_enrollment


    return secondary_school_enrollment


#     Literacy rate
def literacy_rate():
    df = data[['country', 'date','Literacy rate - Male','Literacy rate - Female']].dropna(axis=0)
    df = df[df['date']== df['date'].max()]

    df.columns = ['name','date','male','female']

    df = df.melt(id_vars=['name','date'], value_vars=['male','female'],
            var_name='sex', value_name='rate')

    df['geo_code'] = df['name'].map(countries)
    df['geo_level'] = "country"
    df = df.rename(columns={"date": "geo_version"})
    literacy_rate = df[['geo_level','geo_code','name',
                                   'geo_version','sex','rate']].sort_values('name')

    try:
        sheet = takwimu_Sheet.worksheet('literacy_rate')

    except gspread.exceptions.WorksheetNotFound:
            takwimu_Sheet.add_worksheet(title='literacy_rate', rows="100", cols="20")
            sheet = gc.open('Takwimu_WB_spreadsheet').worksheet('literacy_rate')

            literacy_rate = gd.set_with_dataframe(sheet, literacy_rate, include_index=False,
                          include_column_header=True,resize=False)
            return literacy_rate
    return literacy_rate

# export datasets to csv

def save_to_csv():  
    population().to_csv('huru/population.csv')
    basic_services().to_csv('huru/basic_services.csv')
    youth_unemployment().to_csv('huru/youth_unemployment.csv')
    life_expectancy().to_csv('huru/life_expectancy.csv')
    infant_under_5_mortality().to_csv('huru/infant_under_5_mortality.csv')
    hiv_prevalence().to_csv('huru/hiv_prevalence.csv')
    primary_completion().to_csv('huru/primary_completion.csv')
    employment_to_population().to_csv('huru/employment_to_population.csv')
    health_staff().to_csv('huru/health_staff.csv')
    acc_ownership().to_csv('huru/acc_ownership.csv')   
    primary_school_enrollment().to_csv('huru/primary_school_enrollment.csv')
    secondary_school_enrollment().to_csv('huru/secondary_school_enrollment.csv')
    literacy_rate().to_csv('huru/literacy_rate.csv')
    
#     Save to sheet
def process_to_sheet():
    population()
    basic_services()
    youth_unemployment()
    life_expectancy()
    infant_under_5_mortality()
    hiv_prevalence()
    primary_completion()
    employment_to_population()
    health_staff()
    acc_ownership() 
    primary_school_enrollment()
    secondary_school_enrollment()
    literacy_rate()

if __name__ == "__main__":
    data = collect()
    save_to_csv()