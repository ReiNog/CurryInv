# Functions to calculate business days using the brazilian holidays
import pandas as pd
from pandas.tseries.offsets import BDay

db_path = 'D:\Investiments\Databases\Indexes\BR_holidays.csv'
br_holidays = pd.read_csv(db_path, delimiter=';')

br_holidays['event_date'] = pd.to_datetime(br_holidays['event_date'],format='%Y-%m-%d')

# List of holidays that affect the brazilian stock exchange B3 - used to find the days the B3 is open for trade and settlement
b3_holidays = br_holidays.loc[(br_holidays['city_name'] == 'Sao Paulo')]
list_b3_holidays = b3_holidays['event_date'].values

# List of bank holidays - used to calculate business days for interest rates (CDI, Selic)
br_holidays = br_holidays.loc[(br_holidays['city_name'] == 'Brazilian Real')]
list_br_holidays = br_holidays['event_date'].values

def next_br_bday (st_date, num_days=1):
    # Calculates the date that is num_days business days after (num_days > 0)
    
    if num_days < 0:
        raise Exception('num-days must be positive')

    next_date = st_date
    
    while (num_days > 0):   
        next_date = next_date + pd.Timedelta(1, unit='D')
        
        while ((next_date in list_br_holidays) or (next_date.dayofweek == 5) or (next_date.dayofweek == 6)):
            next_date = next_date + pd.Timedelta(1, unit='D')
            
        num_days = num_days - 1
    
    return(next_date)  

def prev_br_bday (st_date, num_days=-1):
    # Calculates the date that is num_days business days before (num_days < 0) st_date

    if num_days > 0:
        num_days = -num_days

    next_date = st_date
    
    while (num_days < 0):   
        next_date = next_date + pd.Timedelta(-1, unit='D')
        
        while ((next_date in list_br_holidays) or (next_date.dayofweek == 5) or (next_date.dayofweek == 6)):
            next_date = next_date + pd.Timedelta(-1, unit='D')
            
        num_days = num_days + 1
    
    return(next_date)

def num_br_bdays (st_date, end_date):
    # Calculates the number of business days between st_date and end_date
    
    if not is_br_bday(st_date):
        st_date = next_br_bday(st_date)
    
    if not is_br_bday(end_date):
        end_date = next_br_bday(end_date)

    num_days = len(pd.bdate_range(start=st_date, end=end_date,freq='C', holidays=list_br_holidays))-1

    return(num_days)

def is_br_bday(ref_date):
    # Returns if ref_date is a business day in Brazil

    if ((ref_date in list_br_holidays) or (ref_date.dayofweek == 5) or (ref_date.dayofweek == 6)):
        return(False)
    else:
        return(True)

def list_of_br_bdays(st_date, end_date):
    # Returns a list with all business days in Brazil between st_date (inclusive) and end_date (inclusive)

    list_br_bdays = pd.bdate_range(start=st_date, end=end_date,freq='C', holidays=list_br_holidays)

    return(list_br_bdays)

def next_b3_bday (st_date, num_days=1):
    # Calculates the date that is num_days business days after (num_days > 0) considering B3's calendar
    
    if num_days < 0:
        raise Exception('num-days must be positive')

    next_date = st_date
    
    while (num_days > 0):   
        next_date = next_date + pd.Timedelta(1, unit='D')
        
        while ((next_date in list_b3_holidays) or (next_date.dayofweek == 5) or (next_date.dayofweek == 6)):
            next_date = next_date + pd.Timedelta(1, unit='D')
            
        num_days = num_days - 1
    
    return(next_date)  

def is_b3_bday(ref_date):
    # Returns if ref_date is a business day for B3

    if ((ref_date in list_b3_holidays) or (ref_date.dayofweek == 5) or (ref_date.dayofweek == 6)):
        return(False)
    else:
        return(True)

def list_of_b3_bdays(st_date, end_date):
    # Returns a list with all B3 business days between st_date (inclusive) and end_date (inclusive)

    list_br_bdays = pd.bdate_range(start=st_date, end=end_date,freq='C', holidays=list_br_holidays)

    return(list_br_bdays)
    
