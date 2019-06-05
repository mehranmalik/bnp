from pandas import read_csv
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt


df = read_csv('C:\\Users\\mehran.malik\\Documents\\bnp\\data\\data.csv', header=None)
df.columns = ['time','message_identifier','sender','recipients','topic','mode']

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

df_0 = pd.merge(df_sender,df_recipients,how='inner',left_on='sender',right_on='recipient')
df_0.drop(columns=['recipient'],inplace=True)
df_0.rename(columns={'sender':'person'},inplace=True)

df_1 = pd.merge(df_sender,df_recipients,how='left',left_on='sender',right_on='recipient', indicator=True) \
        .query("_merge == 'left_only'") \
        .drop('_merge',1)
df_1.drop(columns=['recipient'],inplace=True)
df_1.rename(columns={'sender':'person'},inplace=True)

df_2 = pd.merge(df_sender,df_recipients,how='right',left_on='sender',right_on='recipient', indicator=True) \
        .query("_merge == 'right_only'") \
        .drop('_merge',1)
df_2.drop(columns=['sender'],inplace=True)
df_2.rename(columns={'recipient':'person'},inplace=True)
new_order = ['person','sent_count','received_count']
df_2 = df_2[new_order]

df1 = df_0.append([df_1,df_2])
df1.fillna(0,inplace=True)
df1.sort_values(by='sent_count',ascending=False,inplace=True)

df1.to_csv('C:\\Users\\mehran.malik\\Documents\\bnp\\question1.csv')

#######################################################################
#######################################################################
#######################################################################
#######################################################################

df['Date'] = df['Date_Time'].apply(convert_time_to_date)

df_sender_date= df[['sender','Date']].reset_index()
df_sender_date = df_sender_date.groupby(['sender','Date']).count().reset_index()

top_senders = ['jeff dasovich','sara shackleton','pete davis','chris germany'
              ,'notes','vince kaminski','matthew lenhart','debra perlingiere'
              ,'gerald nemec','announcements']

df_sender_2 = df_sender_date[df_sender_date.sender.isin(top_senders)]

date1 = pd.to_datetime('1999-06-18').date()
date2 = pd.to_datetime('2001-06-17').date()

df_sender_2 = df_sender_2.loc[(df_sender_2['Date']>date1) & (df_sender_2['Date']<date2)]
df_sender_2.set_index('Date',inplace=True)
df_sender_2.rename(columns={'index':'sent_count'},inplace=True)

df_sender_2 = df_sender_2.reset_index()
df_sender_2.set_index('Date',inplace=True)

#plt.figure()
#plt.xlabel("")
#plt.ylim(0,80)
df_sender_2.pivot_table(index='Date', columns='sender', values='sent_count',\
                        aggfunc='sum', fill_value=0).plot.area(stacked=True,\
                        figsize=(40,20),rot=90,ylim=(0,80),fontsize=20)
plt.legend(loc=2, prop={'size': 20})
plt.savefig('C:\\Users\\mehran.malik\\Documents\\bnp\\question2.png')


#######################################################################
#######################################################################
#######################################################################

df = read_csv('C:\\Users\\mehran.malik\\Documents\\bnp\\data\\data.csv', header=None)
df.columns = ['time','message_identifier','sender','recipients','topic','mode']

df['Date_Time'] = df['time'].apply(change_time_format)
df_sender = df[['sender','Date_Time']]
df_recipients = df['recipients'].str.split(pat='|',expand=True)

for i in range(0,len(df_recipients.columns)):
    df_sender[str(i)] = df_recipients[i]

df_sender.set_index(['sender','Date_Time'],inplace=True)
df_1 = df_sender.stack().reset_index()

df_1['Date'] = df_1['Date_Time'].apply(convert_time_to_date)
df_1.drop(columns=['level_2','Date_Time'],inplace=True)
df_1.rename(columns={0:'recipient'},inplace=True)

df_2 = df_1[df_1.recipient.isin(top_senders)]
df_2 = df_2.loc[(df_2['Date']>date1) & (df_2['Date']<date2)]

df_3 = df_2.groupby(['recipient','Date']).count().reset_index()

df_3.set_index('Date',inplace=True)

df_3.pivot_table(index='Date', columns='recipient', values='sender',\
                 aggfunc='sum', fill_value=0).plot.area(stacked=True,\
                 figsize=(40,20),rot=90,ylim=(0,80),fontsize=20)
plt.legend(loc=2, prop={'size': 20})
plt.savefig('C:\\Users\\mehran.malik\\Documents\\bnp\\question3.png')

#df_3.plot()
#df_1.columns



#######################################################################
#######################################################################
#######################################################################
#######################################################################


def change_time_format(ts):
    ts = str(ts)
    return datetime.fromtimestamp(int(ts[0:9]))
    #ts = datetime.fromtimestamp(int(ts[0:9])) 
    #dateObj = ts.date()
    #return dateObj

def convert_time_to_date(ts):
    dateObj = ts.date()
    return dateObj

def convert_Date_to_string(ts):
    dateObj = str(ts)
    return dateObj

