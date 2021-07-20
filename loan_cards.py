import datetime
from pymongo import MongoClient
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import requests
import json
import ast
from pprint import pprint
#pd.option_context('display.max_rows', None, 'display.max_columns', None)

# connect to MongoDB, change the << MONGODB URL >> to reflect connection string
client = MongoClient('mongodb://127.0.0.1:27017/')
database = client["letsmd"]
card_leads = database["loan_card_leads"]

### Input start date and end date
startDate = z.input("From","2019-06-06 00:00:00")
endDate = z.input("To","2019-06-06 23:59:59")
additional_columns = z.checkbox("Additional Columns", [("borrower_details","Borrower Details"), ("dialer_details","Dialer Details")])
stage = z.checkbox("Stage", [("new","New"), ("info_call","Info_call"), ("send_to_credit","Send_to_credit"), ("payment","Payment"), ("activate","Activate"), ("verification","Verification")])
payment_status = z.checkbox("Payment Status", [("payment_received","Payment Received"), ("payment_not_received","Payment Not Received")])
query = {}
query["created_at"] = {
    u"$gte": datetime.datetime.strptime(startDate, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(hours=5, minutes=30),
    u"$lte": datetime.datetime.strptime(endDate, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(hours=5, minutes=30)
}
stage = list(stage)
query["stage"] = {"$in" : stage}
query["channel_id"] = "5c98a775aa0eb33bff0dcdb3"
projection = {}
projection["created_at"] = 1.0
projection["counter"] = 1.0
projection["loanCardCustomerObject.first_name"] = 1.0
projection["loanCardCustomerObject.last_name"] = 1.0
projection["loanCardCustomerObject.mobile"] = 1.0
projection["stage"] = 1.0
projection["state"] = 1.0
projection["state_timestamp"] = 1.0
projection["loan_card_id"] = 1.0
projection["utm_data"] = 1.0
projection["_id"] = 0.0

# Adds additional columns to query if borrower details is checked by user
if 'borrower_details' in additional_columns:
    projection["loanCardCustomerObject.dob"] = 1.0
    projection["loanCardCustomerObject.city"] = 1.0
    projection["loanCardCustomerObject.pincode"] = 1.0
    projection["loanCardCustomerObject.area_type"] = 1.0
    projection["loanCardCustomerObject.company"] = 1.0
    projection["loanCardCustomerObject.designation"] = 1.0
    projection["loanCardCustomerObject.education"] = 1.0
    projection["loanCardCustomerObject.experience"] = 1.0
    projection["loanCardCustomerObject.gender"] = 1.0
    projection["loanCardCustomerObject.marital_status"] = 1.0
    projection["loanCardCustomerObject.residence_type"] = 1.0
    projection["loanCardCustomerObject.salary"] = 1.0
    projection["loanCardCustomerObject.employment_type"] = 1.0

# Converts the mongodb query output to dataframe
df_0 = json_normalize(list(card_leads.find(query, projection = projection)))

# Adds column field in dataframe if output of query had no values in particular column
if 'loanCardCustomerObject.first_name' not in df_0:
    df_0['lloanCardCustomerObject.first_name'] = ''
if 'loanCardCustomerObject.last_name' not in df_0:
    df_0['loanCardCustomerObject.last_name'] = ''
if 'created_at' not in df_0:
    df_0['created_at'] = ''
if 'state' not in df_0:
    df_0['state'] = ''
if 'state_timestamp' not in df_0:
    df_0['state_timestamp'] = ''
if 'loan_card_id' not in df_0:
    df_0['loan_card_id'] = ''
if 'utm_data' not in df_0:
    df_0['utm_data'] = ''
if 'loanCardCustomerObject.dob' not in df_0:
    df_0['loanCardCustomerObject.dob'] = ''
if 'loanCardCustomerObject.city' not in df_0:
    df_0['loanCardCustomerObject.city'] = ''
if 'loanCardCustomerObject.pincode' not in df_0:
    df_0['loanCardCustomerObject.pincode'] = ''
if 'loanCardCustomerObject.area_type' not in df_0:
    df_0['loanCardCustomerObject.area_type'] = ''
if 'loanCardCustomerObject.company' not in df_0:
    df_0['loanCardCustomerObject.company'] = ''
if 'loanCardCustomerObject.designation' not in df_0:
    df_0['loanCardCustomerObject.designation'] = ''
if 'loanCardCustomerObject.education' not in df_0:
    df_0['loanCardCustomerObject.education'] = ''
if 'loanCardCustomerObject.experience' not in df_0:
    df_0['loanCardCustomerObject.experience'] = ''
if 'loanCardCustomerObject.gender' not in df_0:
    df_0['loanCardCustomerObject.gender'] = ''
if 'loanCardCustomerObject.marital_status' not in df_0:
    df_0['loanCardCustomerObject.marital_status'] = ''
if 'loanCardCustomerObject.residence_type' not in df_0:
    df_0['loanCardCustomerObject.residence_type'] = ''
if 'loanCardCustomerObject.salary' not in df_0:
    df_0['loanCardCustomerObject.salary'] = ''
if 'loanCardCustomerObject.employment_type' not in df_0:
    df_0['loanCardCustomerObject.employment_type'] = ''
    
# Converts time from GMT to IST
df_0['created_at'] += datetime.timedelta(hours=5, minutes=30)

# Rename columns by keeping only text after last dot
df_0.columns = [x.rsplit('.', 1)[-1] for x in df_0.columns]

# Create list of age from dob and created_at
df_0['age'] = pd.to_datetime(df_0['created_at']) - pd.to_datetime(df_0['dob'])
df_0['age'] = df_0['age'].apply(lambda x: x if pd.isnull(x) else int(x.days / 356.25))

# Converts loan_card_id to string
df_0['loan_card_id'] = df_0['loan_card_id'].astype(str)

# Combine first name and last name to get full name
df_0['name'] = df_0['first_name'] + " " + df_0['last_name']

# collect card price from loan_cards collection
card_details = database["loan_cards"]
projection_1 = {}
projection_1["price"] = 1.0
df_1 = pd.DataFrame(list(card_details.find({},projection = projection_1)))
df_1['_id'] = df_1['_id'].astype(str)

# Merge two dataframes into one dataframe
df = df_0.merge(df_1, left_on = 'loan_card_id', right_on ='_id', how='left')

# segregate utm source, medium, campaign, content/ term from utm_data
utm_data = list(df['utm_data'])
utm_source = []
utm_medium = []
utm_campaign = []
utm_content_term = []
for item in utm_data:
    if item.isspace():
        utm_source.append('')
        utm_medium.append('')
        utm_campaign.append('')
        utm_content_term.append('')
    elif item:
        if 'utm_source' in ast.literal_eval(item):
            utm_source.append(ast.literal_eval(item).get('utm_source'))
        else:
            utm_source.append('')
        if 'utm_medium' in ast.literal_eval(item):
            utm_medium.append(ast.literal_eval(item).get('utm_medium')+',')
        else:
            utm_medium.append('')
        if 'utm_campaign' in ast.literal_eval(item):
            utm_campaign.append(ast.literal_eval(item).get('utm_campaign')+',')
        else:
            utm_campaign.append('')
        if 'utm_content' in ast.literal_eval(item):
            utm_content_term.append(ast.literal_eval(item).get('utm_content')+',')
        elif 'utm_term' in ast.literal_eval(item):
            utm_content_term.append(ast.literal_eval(item).get('utm_term')+',')
        else:
            utm_content_term.append('')
    else:
        utm_source.append('')
        utm_medium.append('')
        utm_campaign.append('')
        utm_content_term.append('')

df['utm_source'] = utm_source
df['utm_campaign'] = utm_campaign
df['utm_medium'] = utm_medium
df['utm_content_term'] = utm_content_term

# Finds timestamp of payment received from state_timestamp array
state_timestamp = list(df['state_timestamp'])
time_of_payment = []
for item in state_timestamp:
    if not isinstance(item, list):
        time_of_payment.append('')
    elif not item:
        time_of_payment.append('')
    else:
        c = 0
        for dict_item in item:
            if "converted_state_payment_received_at" in dict_item:
                time_of_payment.append(dict_item["converted_state_payment_received_at"].replace(microsecond=0))
                c += 1
                break
        if c == 0:
            time_of_payment.append('')
df['time_of_payment'] = time_of_payment
df['time_of_payment'] = pd.to_datetime(df['time_of_payment'], errors='coerce')
df['time_of_payment'] += datetime.timedelta(hours=5, minutes=30)

# Rename columns by keeping only text after last dot
df.columns = [x.rsplit('.', 1)[-1] for x in df.columns]

# Drops irrelevant columns
df.drop(['loan_card_id','_id','utm_data','first_name','last_name','dob'], axis=1, inplace=True)


# collect call info from dialer collection
call_info = database["dialer"]
number_list = df['mobile'].tolist()
# query_2 = {}
# query_2["number"] = {"$in" : number_list}
# projection_2 = {}
# projection_2["_id"] = 0
# projection_2["number"] = 1
# projection_2["call_log.Sub Disposition"] = 1
# projection_2["call_log.Start Time"] = 1
# projection_2["call_log.Latest_Disposition"] = 1

# dialer_df = pd.DataFrame(list(call_info.find(query_2 ,projection = projection_2)))
dialer_df = pd.DataFrame(list(call_info.aggregate([
    {
        "$match": {"number" : {"$in" : number_list}}
    },
    {
    "$project":
      {
         "_id": 0,
         "number": 1,
         "call_log.Sub Disposition" : 1,
         "call_log.Start Time" : 1,
         "call_log.Latest_Disposition" : 1

      }
    }])))

call_log_df = pd.DataFrame(dialer_df['call_log'].values.tolist())
for column in call_log_df:
    name = column
    dialer_df[[str(column)+'_call_time',str(column)+'_sub_disposition','latest disposition']] = call_log_df[column].apply(pd.Series)
# z.show(dialer_df)

dialer_df = dialer_df[ ['latest disposition'] + [ col for col in dialer_df.columns if col != 'latest disposition' ] ]


# Merge two dataframes into one dataframe
# df = df.merge(dialer_df, left_on = 'mobile', right_on ='number', how='left')


# Show columns with borrower details based on user's choice
if 'borrower_details' in additional_columns:
    if 'dialer_details' in additional_columns:
        df = df[['counter', 'created_at', 'name', 'mobile', 'stage', 'state', 'time_of_payment', 'price', 'utm_source', 'utm_campaign', 'utm_medium', 'utm_content_term', 'city', 'pincode', 'area_type', 'age', 'gender', 'marital_status', 'education', 'employment_type', 'experience', 'company', 'designation', 'salary']]
        df = df.merge(dialer_df, left_on = 'mobile', right_on ='number', how='left')
    else:
        df = df[['counter', 'created_at', 'name', 'mobile', 'stage', 'state', 'time_of_payment', 'price', 'utm_source', 'utm_campaign', 'utm_medium', 'utm_content_term', 'city', 'pincode', 'area_type', 'age', 'gender', 'marital_status', 'education', 'employment_type', 'experience', 'company', 'designation', 'salary']]
else:
    if 'dialer_details' in additional_columns:
        df = df[['counter', 'created_at', 'name', 'mobile', 'stage', 'state', 'time_of_payment', 'price', 'utm_source', 'utm_campaign', 'utm_medium', 'utm_content_term']]
        df = df.merge(dialer_df, left_on = 'mobile', right_on ='number', how='left')
    else:
       df = df[['counter', 'created_at', 'name', 'mobile', 'stage', 'state', 'time_of_payment', 'price', 'utm_source', 'utm_campaign', 'utm_medium', 'utm_content_term']] 

if 'payment_received' not in payment_status:
    df = df[df.time_of_payment.isnull()]
    
if 'payment_not_received' not in payment_status:
    df = df[df.time_of_payment.notnull()]

df.drop(['call_log','number'], axis=1, inplace=True)

# Relace NaN with blank string for display
df.fillna('',inplace=True)    

# Set max no. of rows to be displayed to 10000
z.max_result = 10000

# Display final dataframe
z.show(df)

  