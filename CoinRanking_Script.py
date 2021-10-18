# Importing the required libararies
import json # for working with JSON data
import requests # for sending request to the API endpoint/url
import statistics # for statistical calculations
import pandas as pd # for working with dataframe/tabular data
from bs4 import BeautifulSoup # for parsing html tags
from datetime import datetime # for working with datetime


# Make request to the API...
response = requests.get('https://api.coinranking.com/v1/public/coin/1/history/30d', timeout=10)
rjson = response.json()

# Use tool like: https://jsoneditoronline.org to inspect the returned json to know which node contains the required data...
# In this case the data is in: 'data' >> 'history'. That is: rjson['data']['history'] which is a list of dictionaries...

# Lets save it into a dataframe for manipulation.
df = pd.DataFrame(rjson['data']['history'])
# Now lets update the dataframe table with the schema definitions...

# **********************************************************************
# **********************************************************************
# Schema 1: The general work flow is the create a helper function and apply it to a new column...

# Date------------------
def date_func(x):
    x = int(str(x)[:-3]) # Reduce digits to 10 to avoid OSError - so convert to str and slice out
    dt = datetime.fromtimestamp(x).strftime('%Y-%m-%d T%H:%M:%S')
    return dt

df['Date'] = df['timestamp'].apply( lambda x: date_func(x) )



# Price------------------
def price_func(x):
    return round(float(x), 2) # Convert to float and round to 2 decimal

df['Price'] = df['price'].apply( lambda x: price_func(x) )



# Direction------------------
# Create a helper column by shifting price column down by 1 step
df['Direction_temp1'] = df['Price'].shift(1)

def direction_func(x, y):
    if x - y > 0:
        return 'Down'
    if x - y < 0:
        return 'Up'
    if x - y == 0:
        return 'Same'

df['Direction'] = df.apply( lambda x : direction_func(x['Direction_temp1'], x['Price']), axis=1 )


# Change------------------
def change_func(x, y):
    return x - y

df['Change'] = df.apply( lambda x : change_func(x['Direction_temp1'], x['Price']), axis=1 )



# dayOfWeek------------------
def day_of_week_func(x):
    x = int(str(x)[:-3]) # Reduce digits to 10 to avoid OSError - so convert to str and slice out
    dt = datetime.fromtimestamp(x).strftime('%A')
    return dt

df['dayOfWeek'] = df['timestamp'].apply( lambda x : day_of_week_func(x) )



# highSinceStart------------------
def high_since_start(x):
    if x == 'Up':
        return 'true'
    elif x == 'Down':
        return 'false'
    else:
        return ''

df['highSinceStart'] = df['Direction'].apply( lambda x : high_since_start(x) )




# lowSinceStart------------------
def low_since_start(x):
    if x == 'Down':
        return 'true'
    elif x == 'Up':
        return 'false'
    else:
        return ''

df['lowSinceStart'] = df['Direction'].apply( lambda x : low_since_start(x) )


# **********************************************************************
# **********************************************************************
# Schema 2: As in above, the new entries are: dailyAverage, dailyVariance, volatilityAlert

# dailyAverage------------------
# Create helper column...
df['Date_temp1'] = df['Date'].apply( lambda x : x.split(' T')[0] )
# Group the data by Date_temp1 col and cal average...
date_group = df.groupby('Date_temp1')
df_average = date_group.apply(lambda x: x['Price'].mean())
# pd.DataFrame(df2, columns=['Mean']).reset_index()
df_average = df_average.to_frame(name="Mean").reset_index()
# Map the average to the main df...
# make a mapping dictionary from average df...
avg_dict = dict(zip(df_average['Date_temp1'], round(df_average['Mean'], 2)))
# create new column based on the map column and dictionary
df['dailyAverage'] = df['Date_temp1'].map(avg_dict)




# dailyVariance------------------
# Population variance n
# dailyVariance = statistics.pvariance( df['Price'].to_list() )
# Sample variance n-1
# dailyVariance = statistics.variance( df['Price'].to_list() )

# Use the day Group df above to cal dailyVariance...
df_variance = date_group.apply( lambda x: round(statistics.variance(x['Price'].to_list()), 2) )
df_variance = df_variance.to_frame(name="Variance").reset_index()
vari_dict = dict(zip(df_variance['Date_temp1'], df_variance['Variance'] ))
df['dailyVariance'] = df['Date_temp1'].map(vari_dict)



# volatilityAlert------------------
# Standard Deviation... 
sd = round( statistics.stdev( df['Price'].to_list() ), 2) # Or: df['Price'].std()
second_sd = df['Price'].mean() + 2*sd

# median = df['Price'].median()
# percentage = (sd/median) * 100
# benchmark = 95 / 100

'''
The normal distribution is commonly associated with the 68-95-99.7 rule which you can see in the image above. 
    68% of the data is within 1 standard deviation (σ) of the mean (μ), 
    95% of the data is within 2 standard deviations (σ) of the mean (μ), and 
    99.7% of the data is within 3 standard deviations (σ) of the mean (μ).
- https://towardsdatascience.com/understanding-the-68-95-99-7-rule-for-a-normal-distribution-b7b7cbf760c2

2nd standard devation above = mean + 2*standard deviation
2nd standard deviation below = mean - 2*standard deviation
- https://www.wyzant.com/resources/answers/27347/i_need_to_find_one_two_and_three_standards_deviations_above_the_mean_over_14_88_and_one_two_and_three_below_this_mean
- https://www.cuemath.com/questions/how-to-find-how-many-standard-deviation-away-from-mean/

- "volatilityAlert:" true/false if any price that day is outside 2 standard deviations.
'''
def volatility_alert_func(x):
    if x > second_sd:
        return 'true'
    else:
        return 'false'
    
df['volatilityAlert'] = df['Price'].apply( lambda x: volatility_alert_func(x) )



# **********************************************************************
# **********************************************************************
# Filter only time at: 00:00:00
# Drop dups by date...
df_drop_dup = df.drop_duplicates(subset='Date_temp1', keep="first")


# Filter the needed fields into df_schemas as follow...
df_schema1 = df_drop_dup[['Date', 'Price', 'Direction', 'Change', 'dayOfWeek', 'highSinceStart', 'lowSinceStart']]
df_schema2 = df_drop_dup[['Date', 'Price', 'dailyAverage', 'dailyVariance', 'volatilityAlert']]


# Get current time to save json file...
current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

# Convert table to schemas...
df_schema1.to_json(f'schema_1_{current_time}.json', orient='records')
print(df_schema1.to_json(orient='records'))

df_schema2.to_json(f'schema_2_{current_time}.json', orient='records')
print(df_schema2.to_json(orient='records'))

