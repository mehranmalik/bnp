import sys
from pandas import read_csv
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

def main():
    filename = sys.argv[1]
    df = read_csv(filename, header=None)
    df.columns = ['time','message_identifier','sender','recipients','topic','mode']
    
    print('(1/3) Processing Question 1..........')
    top_senders = fn_question_1(df, 10)
    
    print('(2/3) Processing Question 2..........')
    fn_question_2(df, top_senders, '2000-06-01','2001-06-01')
    
    print('(3/3) Processing Question 3..........')
    fn_question_3(df, top_senders, '2000-06-01','2001-06-01')

def fn_question_1(df, number_of_top_senders):
    """
    Algorithm
    Convert UNIX time to date time
    Create separate dataframe of senders list and use groupby to find the count  
    Create a separate dataframe for receipients list and split the list in separate columns
    move all the receipients columns in one column using stack function
    Group the receipients using groupby and calculate count  
    Merge both dataframes, using inner, left and right joins
    returns Top X senders list
    
    :param pandas dataframe with raw data
    :param Number of top senders to be returned by function
    :return list of top senders
    """
    df['Date_Time'] = df['time'].apply(change_time_format)
    
    df_sender= df['sender'].reset_index()
    df_sender = df_sender.groupby(['sender']).count().reset_index()
    df_sender.rename(columns={'index':'sent_count'},inplace=True)
    
    df_recipients = df['recipients'].str.split(pat='|',expand=True)
    df_recipients = df_recipients.stack().reset_index()
    df_recipients.columns = ['level_0','level_1','recipient']
    df_recipients = df_recipients.groupby(['recipient']).count().reset_index()
    df_recipients = df_recipients[['recipient','level_0']]
    df_recipients.rename(columns={'level_0':'received_count'},inplace=True)
    
    df_inner = pd.merge(df_sender,df_recipients,how='inner',left_on='sender',right_on='recipient')
    df_inner.drop(columns=['recipient'],inplace=True)
    df_inner.rename(columns={'sender':'person'},inplace=True)   
   
    df_left_only = pd.merge(df_sender,df_recipients,how='left',left_on='sender',right_on='recipient', indicator=True) \
            .query("_merge == 'left_only'") \
            .drop('_merge',1)
    df_left_only.drop(columns=['recipient'],inplace=True)
    df_left_only.rename(columns={'sender':'person'},inplace=True)
    
    df_right_only = pd.merge(df_sender,df_recipients,how='right',left_on='sender',right_on='recipient', indicator=True) \
            .query("_merge == 'right_only'") \
            .drop('_merge',1)
    df_right_only.drop(columns=['sender'],inplace=True)
    df_right_only.rename(columns={'recipient':'person'},inplace=True)
    new_order = ['person','sent_count','received_count']
    df_right_only = df_right_only[new_order]  
    
    df1 = df_inner.append([df_left_only,df_right_only])
    df1.fillna(0,inplace=True)
    df1.sort_values(by='sent_count',ascending=False,inplace=True)
    top_senders = df1['person'].head(int(number_of_top_senders)).tolist()
    df1.to_csv('question1.csv')
    
    return top_senders

def fn_question_2(df, top_senders, from_date, to_date):
    """
    Algorithm
    Get the list of top senders from previous function
    Create dataframe with senders list,date time.
    Group the dataset by sender, data time and calculate count 
    Filter the dataset based on senders list and date range
    Use pivot _table to aggregate the dataset and plot a graph
    
    :param pandas dataframe with raw data
    :param list of top senders
    :param From data, Duration for which data has to be analysed
    :param To date, Duration for which data has to be analysed
    """
    from_date = pd.to_datetime(from_date).date()
    to_date = pd.to_datetime(to_date).date() 
    df['Date'] = df['Date_Time'].apply(convert_time_to_date)    
    df_sender_date= df[['sender','Date']].reset_index()
    df_sender_date = df_sender_date.groupby(['sender','Date']).count().reset_index()   
    df2 = df_sender_date[df_sender_date.sender.isin(top_senders)] 
    df2 = df2.loc[(df2['Date']>from_date) & (df2['Date']<to_date)]
    df2.set_index('Date',inplace=True)
    df2.rename(columns={'index':'sent_count'},inplace=True)   
    df2 = df2.reset_index()
    df2.set_index('Date',inplace=True)
    
    df2.pivot_table(index='Date', columns='sender', values='sent_count',\
                            aggfunc='sum', fill_value=0).plot.area(stacked=True,\
                            figsize=(40,20),rot=90,ylim=(0,80),fontsize=20)
    plt.legend(loc=2, prop={'size': 20})
    plt.savefig('question2.png')

def fn_question_3(df, top_senders, from_date, to_date):
    """
    Algorithm
    Get the list of top senders from fn_question_1 function
    Create dataframe with senders list, date time.
    Split the receipients list into separate columns, and add those columns to previous dataframe
    Stack the list of receipients and then index by sender   
    Filter the dataset based on whether receipients are in top senders list and date range
    Group the dataset by receipient and data time and calculate count 
    Use pivot _table to aggregate the dataset and plot a graph
    
    :param pandas dataframe with raw data
    :param list of top senders
    :param From data, Duration for which data has to be analysed
    :param To date, Duration for which data has to be analysed
    """
    from_date = pd.to_datetime(from_date).date()
    to_date = pd.to_datetime(to_date).date() 
    df['Date_Time'] = df['time'].apply(change_time_format)
    df_sender = df[['sender','Date_Time']]
    df_recipients = df['recipients'].str.split(pat='|',expand=True)
    
    for i in range(0,len(df_recipients.columns)):
        df_sender[str(i)] = df_recipients[i]
    
    df_sender.set_index(['sender','Date_Time'],inplace=True)
    df3 = df_sender.stack().reset_index()  
    df3['Date'] = df3['Date_Time'].apply(convert_time_to_date)
    df3.drop(columns=['level_2','Date_Time'],inplace=True)
    df3.rename(columns={0:'recipient'},inplace=True) 
    df3 = df3[df3.recipient.isin(top_senders)]
    df3 = df3.loc[(df3['Date']>from_date) & (df3['Date']<to_date)]   
    df3 = df3.groupby(['recipient','Date']).count().reset_index()   
    df3.set_index('Date',inplace=True)
    
    df3.pivot_table(index='Date', columns='recipient', values='sender',\
                     aggfunc='sum', fill_value=0).plot.area(stacked=True,\
                     figsize=(40,20),rot=90,ylim=(0,80),fontsize=20)
    plt.legend(loc=2, prop={'size': 20})
    plt.savefig('question3.png')


def change_time_format(ts):
    ts = str(ts)
    return datetime.fromtimestamp(int(ts[0:9]))

def convert_time_to_date(ts):
    dateObj = ts.date()
    return dateObj

def convert_Date_to_string(ts):
    dateObj = str(ts)
    return dateObj

main()