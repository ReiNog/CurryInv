# This module contains the set of functions that work with the Selic interest rate
# The Selic interest rate is the monetary policy interest rate, i.e, the key tool used by the Banco Central do Brasil (BCB or Bacen) in the implementation of the monetary policy. 
# The Selic rate, or 'over Selic', is the Brazilian federal funds rate. Precisely, Selic rate is the weighted average interest rate of the overnight interbank operations
#  — collateralized by federal government securities — carried out at the Special System for Settlement and Custody (Selic). 
# Selic rate is expressed as a percentage per annum, based on a two hundred fifty-two (252) business days year, as published by BCB in its website (http://www.bcb.gov.br)
import os
import pandas as pd
import bacen as bc
import ir_calc as ir
import br_workdays as wd


def load_selic(db_path='D:\Investiments\Databases\Indexes\Selic.csv'):
    # read Selic database to dataframe
    try:
        df_selic = pd.read_csv(db_path, delimiter=';', dtype={'Rate':float, 'Accum':float}, index_col='TradeDate')
        df_selic.index = pd.to_datetime(df_selic.index,format='%Y-%m-%d')
        df_selic.sort_index()
    except OSError as err:
        raise OSError(err)
    except Exception as err:
        raise Exception(err)

    return(df_selic)

def update_selic_db(db_path='D:\Investiments\Databases\Indexes\Selic.csv'):

    df_selic = load_selic(db_path)

    # Get the Selic values published since the last update.
    start_date_str = df_selic.last_valid_index().strftime('%d/%m/%Y')
    end_date_str = pd.to_datetime("today").strftime("%d/%m/%Y")

    try:
        novos_selic = bc.get_bacen_data('Selic', start_date_str, end_date_str)
    except Exception as err:
        raise Exception(err)


    if novos_selic.first_valid_index() == df_selic.last_valid_index():
        df_selic.drop(df_selic.last_valid_index(), inplace=True)

    novos_selic.rename(columns={'valor':'Rate'}, inplace=True)

    # Appends new rates to the dataframe
    df_selic = pd.concat([df_selic, novos_selic])

    # Includes the workday following the date of the last CDI rate available, to calculate the last available Accum
    new_date = wd.next_br_bday(df_selic.last_valid_index())
    new_row = pd.DataFrame(data={'Rate':[0.0],'Accum':[1.0]},index=[new_date])
    df_selic = pd.concat([df_selic, new_row])

    df_selic.sort_index()

    # Calculates the cumulative Selic for the whole period
    df_selic = ir.calc_accum_r252(df_selic)

    # Saves the last version of the Selic database
    new_path = db_path[:len(db_path)-4] + df_selic.last_valid_index().strftime('%Y%m%d') + '.csv'
    if not os.path.exists(new_path):
        os.rename(db_path,new_path)

    # Saves the updated series to the csv file
    df_selic.to_csv(db_path, sep=';',header=['Rate','Accum'], index_label='TradeDate')

def selic_accum (df_selic, start_date, end_date, percent=1):
    # Returns the cumulative return of the Selic rate between start_date (inclusive) and end_date (exclusive)
    # If percent <> 1, applies percent to the daily effective rate
    # If start_date is not a business day in Brazil, considers the following business day as start_date
    # If end_date is not a business day in Brazil, considers the following business day as end_date

    if not wd.is_br_bday(start_date):
        start_date = wd.next_br_bday(start_date)

    if not wd.is_br_bday(end_date):
        end_date = wd.next_br_bday(end_date)

    if (start_date < df_selic.first_valid_index()) or (end_date > df_selic.last_valid_index()):
        raise Exception('Dates out of available range of Selic dates')

    try:
        if percent == 1:
            cum_ret = df_selic.loc[end_date].Accum / df_selic.loc[start_date].Accum
        else:
            df_selic_tmp = df_selic.loc[start_date : end_date]
            df_selic_tmp = ir.calc_accum_r252(df_selic_tmp, percent)
            cum_ret = df_selic_tmp.loc[end_date].Accum / df_selic_tmp.loc[start_date].Accum
    except Exception as err:
        print(err)

    return(cum_ret)
