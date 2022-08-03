# This module contains the set of functions that work with the CDI interest rate
# CDI rate means the Brazilian interbank deposit (Certificado de Depósito Interbancário) rate, which is an average of interbank overnight rates in Brazil.
# CDI rate is expressed as a percentage per annum, based on a two hundred fifty-two (252) business days year, as published by B3 in its daily report available at B3’s website (http://www.b3.com.br)

import os
import pandas as pd
import bacen as bc
import ir_calc as ir
import br_workdays as wd


def load_cdi(db_path='D:\Investiments\Databases\Indexes\CDI.csv'):
    # Reads the CDI database to dataframe
    try:
        df_cdi = pd.read_csv(db_path, delimiter=';', dtype={'Rate':float, 'Accum':float}, index_col='TradeDate')
        df_cdi.index = pd.to_datetime(df_cdi.index,format='%Y-%m-%d')
        df_cdi.sort_index()
    except OSError as err:
        raise OSError(err)
    except Exception as err:
        raise Exception(err)

    return(df_cdi)

def update_cdi_db(db_path='D:\Investiments\Databases\Indexes\CDI.csv'):
    # Updates the CDI database with all the rates published after the last update

    df_cdi = load_cdi(db_path)

    # Get the CDI values published since the last update
    start_date_str = df_cdi.last_valid_index().strftime('%d/%m/%Y')
    end_date_str = pd.to_datetime("today").strftime("%d/%m/%Y")

    try:
        novos_cdi = bc.get_bacen_data('CDI', start_date_str, end_date_str)
    except Exception as err:
        raise Exception(err)

    # if novos_cdi.first_valid_index().strftime('%d/%m/%Y') == start_date_str:
    #    novos_cdi.drop(novos_cdi.first_valid_index(), inplace=True)

    if novos_cdi.first_valid_index() == df_cdi.last_valid_index():
        df_cdi.drop(df_cdi.last_valid_index(), inplace=True)

    novos_cdi.rename(columns={'valor':'Rate'}, inplace=True)

    # Appends new rates to the dataframe
    df_cdi = pd.concat([df_cdi, novos_cdi])

    # Includes the workday following the date of the last CDI rate available, to calculate the last available Accum
    new_date = wd.next_br_bday(df_cdi.last_valid_index())
    new_row = pd.DataFrame(data={'Rate':[0.0],'Accum':[1.0]},index=[new_date])
    df_cdi = pd.concat([df_cdi, new_row])

    df_cdi.sort_index()

    # Calculates the cumulative CDI for the whole period
    df_cdi = ir.calc_accum_r252(df_cdi)

    # Saves the last version of the CDI database
    new_path = db_path[:len(db_path)-4] + df_cdi.last_valid_index().strftime('%Y%m%d') + '.csv'
    if not os.path.exists(new_path):
        os.rename(db_path,new_path)

    # Saves the updated series to the csv file
    df_cdi.to_csv(db_path, sep=';',header=['Rate','Accum'], index_label='TradeDate')

def cdi_accum (df_cdi, start_date, end_date, percent=1):
    # Returns the cumulative return of the CDI rate between start_date (inclusive) and end_date (exclusive)
    # If percent <> 1, applies percent to the daily effective rate
    # If start_date is not a business day in Brazil, considers the following business day as start_date
    # If end_date is not a business day in Brazil, considers the following business day as end_date

    if not wd.is_br_bday(start_date):
        start_date = wd.next_br_bday(start_date)

    if not wd.is_br_bday(end_date):
        end_date = wd.next_br_bday(end_date)

    if (start_date < df_cdi.first_valid_index()) or (end_date > df_cdi.last_valid_index()):
        raise Exception('Dates out of available range of CDI dates')

    try:
        if percent == 1:
            cum_ret = df_cdi.loc[end_date].Accum / df_cdi.loc[start_date].Accum
        else:
            df_cdi_tmp = df_cdi.loc[start_date : end_date]
            df_cdi_tmp = ir.calc_accum_r252(df_cdi_tmp, percent)
            cum_ret = df_cdi_tmp.loc[end_date].Accum / df_cdi_tmp.loc[start_date].Accum
    except Exception as err:
        print(err)
    return(cum_ret)


