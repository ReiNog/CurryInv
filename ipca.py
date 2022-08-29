# This module contains the set of functions that work with the IPCA inflation index.
# IPCA is the most used inflation index in Brazil. It is calculated and published by IBGE. It is published between the 8th and 11th of the following month.
# IPCA is published both as a monthly percentage rate and a index number. We update our database getting the monthly percentage rate from the BCB's API.

import os
import pandas as pd
import ibge
import ir_calc as ir
import br_workdays as wd


def load_ipca(db_path='D:\Investiments\Databases\Indexes\IPCA.csv'):
    # Reads the IPCA database to dataframe
    try:
        ipca = pd.read_csv(db_path, delimiter=';', dtype={'Num_IPCA':float, 'IPCA':float, 'Accum':float}, index_col='TradeDate')
        ipca.index = pd.to_datetime(ipca.index,format='%Y-%m-%d')
        ipca.sort_index()
    except OSError as err:
        raise OSError(err)
    except Exception as err:
        raise Exception(err)

    return(ipca)

def update_ipca_db(db_path='D:\Investiments\Databases\Indexes\IPCA.csv'):
    # Updates the IPCA database with all the rates published after the last update

    try:
        ipca = load_ipca(db_path)
    except Exception as exp:
        raise Exception(exp)

    # Get the IPCA values published since the last update
    try:
        novos_ipca = ibge.get_ipca_from_ibge(ipca.last_valid_index(),pd.to_datetime("today"))
    except Exception as exp:
        raise Exception(exp)
    
    if novos_ipca.first_valid_index() == ipca.last_valid_index():
        ipca.drop(ipca.last_valid_index(), inplace=True)

    # Appends new rates to the dataframe
    ipca = pd.concat([ipca, novos_ipca])

    # Includes the month following the date of the last IPCA rate available, to calculate the last available Accum
    new_date = ipca.last_valid_index() + pd.DateOffset(months=1)
    new_row = pd.DataFrame(data={'Num_IPCA':[0.0],'IPCA':[0.0],'Accum':[1.00]},index=[new_date])
    ipca = pd.concat([ipca, new_row])

    ipca.sort_index()
    
    # Calculates the cumulative return
    ipca['Accum'] = (1 + ipca.IPCA.shift(1)).cumprod()
    ipca.loc[ipca.first_valid_index()].Accum = 1.00
    ipca['Accum'].round(decimals=8)

    # Saves the last version of the IPCA database in a different file
    new_path = db_path[:len(db_path)-4] + ipca.last_valid_index().strftime('%Y%m%d') + '.csv'
    if not os.path.exists(new_path):
        os.rename(db_path,new_path)

    # Saves the updated series to the csv file
    ipca.to_csv(db_path, sep=';',header=['Num_IPCA','IPCA','Accum'], index_label='TradeDate')

def calc_first_accrual(start_date, end_date, reset_day, accrual_type):
    # Calculates the parameters to be used in the accrual of the first rate -> may be necessary to calculate a pro-rata accrual        
    if start_date.day < reset_day:
        month_m0 = pd.to_datetime((start_date - pd.DateOffset(months=1)).strftime('%Y%m01'))
        month_m1 = pd.to_datetime(start_date.strftime('%Y%m01'))
        first_date = pd.to_datetime(start_date.strftime('%Y%m')+"{0:0>2}".format(reset_day))
    else:
        month_m0 = pd.to_datetime(start_date.strftime('%Y%m01'))
        month_m1 = pd.to_datetime((start_date + pd.DateOffset(months=1)).strftime('%Y%m01'))
        first_date = pd.to_datetime(month_m1.strftime('%Y%m')+"{0:0>2}".format(reset_day))

    if first_date > end_date:
        first_date = end_date

    if accrual_type == 'cd':
        tot_days = month_m1 - month_m0
        num_days = first_date - start_date
    elif accrual_type == 'bd':
        tot_days = wd.num_br_bdays(month_m0,month_m1)
        num_days = wd.num_br_bdays(start_date, first_date)
    else:
        raise Exception('Accrual type must be cd (calendar days), or bd (business days)')

    return {'month_m0': month_m0, 'month_m1': month_m1, 'first_date': first_date, 'tot_days': tot_days, 'num_days': num_days}

def calc_last_accrual(start_date, end_date, reset_day, accrual_type):
    # Calculates the parameters to be used in the accrual of the last rate -> may be necessary to calculate a pro-rata accrual        
    if end_date.day <= reset_day:
        month_m0 = pd.to_datetime((end_date - pd.DateOffset(months=1)).strftime('%Y%m01'))
        month_m1 = pd.to_datetime(end_date.strftime('%Y%m01'))
        last_date = pd.to_datetime((end_date - pd.DateOffset(months=1)).strftime('%Y%m')+"{0:0>2}".format(reset_day))
    else:
        month_m0 = pd.to_datetime(end_date.strftime('%Y%m01'))
        month_m1 = pd.to_datetime((end_date + pd.DateOffset(months=1)).strftime('%Y%m01'))
        last_date = pd.to_datetime(month_m0.strftime('%Y%m')+"{0:0>2}".format(reset_day))

    if last_date < start_date:
        last_date = start_date
        
    if accrual_type == 'cd':
        tot_days = month_m1 - month_m0
        num_days = end_date - last_date
    elif accrual_type == 'bd':
        tot_days = wd.num_br_bdays(month_m0,month_m1)
        num_days = wd.num_br_bdays(last_date, end_date)
    else:
        raise Exception('Accrual type must be cd (calendar days), or bd (business days)')

    return {'month_m0': month_m0, 'month_m1': month_m1, 'last_date': last_date, 'tot_days': tot_days, 'num_days': num_days}

def ipca_accum (ipca, start_date, end_date, reset_day=0, accrual_type='cd'):
    # Returns the cumulative return of the IPCA rate between start_date (inclusive) and end_date (exclusive)
    # Source for the formula: https://www.b3.com.br/data/files/F6/26/EA/D2/F051F610AF4EF0F6AC094EA8/Caderno%20de%20Formulas%20-%20Debentures%20Cetip%2021.pdf
    

    if (start_date < ipca.first_valid_index()) or (start_date > ipca.last_valid_index()) or (end_date < ipca.first_valid_index()) or (end_date > ipca.last_valid_index()):
        raise Exception('Dates out of available range of IPCA dates')
    
    if start_date > end_date:
        raise Exception('Start Date must be older than End Date')

    if accrual_type != 'cd' and accrual_type != 'bd':
        raise Exception('Accrual type must be cd (calendar days), or bd (business days)')

    if reset_day == 0:
        reset_day = end_date.day

    first_accrual = calc_first_accrual(start_date, end_date, reset_day, accrual_type)
    last_accrual = calc_last_accrual(start_date, end_date, reset_day, accrual_type)
    
    try:
        # First accrual may be pro-rata
        ipca_accum = (ipca.loc[first_accrual['month_m1']].Accum / ipca.loc[first_accrual['month_m0']].Accum) ** (first_accrual['num_days'] / first_accrual['tot_days'])
        # Intermediate accruals are never pro-rata
        ipca_accum = ipca_accum * (ipca.loc[last_accrual['month_m0']].Accum / ipca.loc[first_accrual['month_m1']].Accum)
        # Last accrual can be pro-rata
        ipca_accum = ipca_accum * (ipca.loc[last_accrual['month_m1']].Accum / ipca.loc[last_accrual['month_m0']].Accum) ** (last_accrual['num_days'] / last_accrual['tot_days'])
    except Exception as exp:
        raise Exception(exp)

    return(ipca_accum)


