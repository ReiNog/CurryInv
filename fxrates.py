# This module contains the set of functions that work with the FX rates.
# For the BRL/USD FX rate we use the BCB API as source
 
import os
import pandas as pd
import bacen as bc
import ir_calc as ir
import br_workdays as wd

def load_brlusd(db_path='D:\Investiments\Databases\Indexes\BRLUSD.csv'):
    # Reads the BRLUSD database to dataframe
    try:
        df_brlusd = pd.read_csv(db_path, delimiter=';', dtype={'BRLUSD':float}, index_col='TradeDate')
        df_brlusd.index = pd.to_datetime(df_brlusd.index,format='%Y-%m-%d')
        df_brlusd.sort_index()
    except OSError as err:
        raise OSError(err)
    except Exception as err:
        raise Exception(err)

    return(df_brlusd)

def update_brlusd_db(db_path='D:\Investiments\Databases\Indexes\BRLUSD.csv'):
    # Updates the BRLUSD database with all the rates published after the last update

    df_brlusd = load_brlusd(db_path)

    # Get the BRLUSD values published since the last update
    start_date_str = df_brlusd.last_valid_index().strftime('%d/%m/%Y')
    end_date_str = pd.to_datetime("today").strftime("%d/%m/%Y")

    try:
        novos_brlusd = bc.get_bacen_data('BRLUSD', start_date_str, end_date_str)
    except Exception as err:
        raise Exception(err)

    if novos_brlusd.first_valid_index() == df_brlusd.last_valid_index():
        df_brlusd.drop(df_brlusd.last_valid_index(), inplace=True)

    novos_brlusd.rename(columns={'valor':'BRLUSD'}, inplace=True)

    # Appends new rates to the dataframe
    df_brlusd = pd.concat([df_brlusd, novos_brlusd])

    df_brlusd.sort_index()

    # Saves the last version of the BRLUSD database
    new_path = db_path[:len(db_path)-4] + df_brlusd.last_valid_index().strftime('%Y%m%d') + '.csv'
    if not os.path.exists(new_path):
        os.rename(db_path,new_path)

    # Saves the updated series to the csv file
    df_brlusd.to_csv(db_path, sep=';',header=['BRLUSD'], index_label='TradeDate')

def brlusd_accum (df_brlusd, start_date, end_date):
    # Returns the cumulative return of the BRLUSD rate between start_date (inclusive) and end_date (exclusive)
    # If start_date is not a business day in Brazil, considers the following business day as start_date
    # If end_date is not a business day in Brazil, considers the following business day as end_date

    if not wd.is_br_bday(start_date):
        start_date = wd.next_br_bday(start_date)

    if not wd.is_br_bday(end_date):
        end_date = wd.next_br_bday(end_date)

    if (start_date < df_brlusd.first_valid_index()) or (end_date > df_brlusd.last_valid_index()):
        raise Exception('Dates out of available range of BRLUSD dates')

    try:
        cum_ret = df_brlusd.loc[end_date].BRLUSD / df_brlusd.loc[start_date].BRLUSD
    except Exception as err:
        print(err)
    return(cum_ret)
