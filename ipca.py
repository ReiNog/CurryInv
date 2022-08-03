# This module contains the set of functions that work with the IPCA inflation index.
# IPCA is the most used inflation index in Brazil. It is calculated and published by IBGE. It is published between the 8th and 11th of the following month.
# IPCA is published both as a monthly percentage rate and a index number. We update our database getting the monthly percentage rate from the BCB's API.

import os
import pandas as pd
import bacen as bc
import ir_calc as ir
import br_workdays as wd


def load_ipca(db_path='D:\Investiments\Databases\Indexes\IPCA.csv'):
    # Reads the IPCA database to dataframe
    try:
        df_ipca = pd.read_csv(db_path, delimiter=';', dtype={'Num_IPCA':float, 'IPCA':float, 'Accum':float}, index_col='TradeDate')
        df_ipca.index = pd.to_datetime(df_ipca.index,format='%Y-%m-%d')
        df_ipca.sort_index()
    except OSError as err:
        raise OSError(err)
    except Exception as err:
        raise Exception(err)

    return(df_ipca)

def update_ipca_db(db_path='D:\Investiments\Databases\Indexes\IPCA.csv'):
    # Updates the IPCA database with all the rates published after the last update

    df_ipca = load_ipca(db_path)

    # Get the IPCA values published since the last update
    datas = pd.period_range(start = df_ipca.last_valid_index(), end = pd.to_datetime("today"), freq='M').strftime("%Y%m").to_list()
    datas = '|'.join(datas)
    
    api_url = f'https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/{datas}/variaveis/2266?localidades=N1[all]'
    req = pd.read_json(api_url)
    
    if not req.empty:
        # if req is empty, there is no new IPCA rates to add

        values = req['resultados'][0][0]['series'][0]['serie']
    
        novos_ipca = pd.DataFrame(values.items(), columns=['TradeDate', 'Num_IPCA'])
        novos_ipca['TradeDate'] = pd.to_datetime(novos_ipca['TradeDate'], format='%Y%m')
        novos_ipca.set_index('TradeDate', inplace=True)

        api_url = f'https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/{datas}/variaveis/63?localidades=N1[all]'
        req = pd.read_json(api_url)
    
        values = req['resultados'][0][0]['series'][0]['serie']
        novos_ipca2 = pd.DataFrame(values.items(), columns=['TradeDate', 'IPCA'])
        novos_ipca2['TradeDate'] = pd.to_datetime(novos_ipca2['TradeDate'], format='%Y%m')
        novos_ipca2.set_index('TradeDate', inplace=True)
        novos_ipca2['IPCA'] = novos_ipca2['IPCA'] / 100

        novos_ipca = pd.concat([novos_ipca, novos_ipca2], axis=1)

        if novos_ipca.first_valid_index() == df_ipca.last_valid_index():
            df_ipca.drop(df_ipca.last_valid_index(), inplace=True)

        # Appends new rates to the dataframe
        df_ipca = pd.concat([df_ipca, novos_ipca])

        # Includes the month following the date of the last IPCA rate available, to calculate the last available Accum
        new_date = df_ipca.last_valid_index() + pd.DateOffset(months=1)
        new_row = pd.DataFrame(data={'Num_IPCA':[0.0],'IPCA':[0.0],'Accum':[1.00]},index=[new_date])
        df_ipca = pd.concat([df_ipca, new_row])

        df_ipca.sort_index()
    
        # Calculates the cumulative return
        df_ipca['Accum'] = (1 + novos_ipca.IPCA.shift(1)).cumprod()
        df_ipca.loc[df_ipca.first_valid_index()].Accum = 1.00
        df_ipca['Accum'].round(decimals=8)

        # Saves the last version of the IPCA database in a different file
        new_path = db_path[:len(db_path)-4] + df_ipca.last_valid_index().strftime('%Y%m%d') + '.csv'
        if not os.path.exists(new_path):
            os.rename(db_path,new_path)

        # Saves the updated series to the csv file
        df_ipca.to_csv(db_path, sep=';',header=['Num_IPCA','IPCA','Accum'], index_label='TradeDate')

def ipca_accum (df_ipca, start_date, end_date):
    # Returns the cumulative return of the IPCA rate between start_date (inclusive) and end_date (exclusive)
    # If start_date.day() != end_data.day() the first IPCA will be caculated in a pro-rata basis

    if (start_date < df_ipca.first_valid_index()) or (end_date > df_ipca.last_valid_index()):
        raise Exception('Dates out of available range of CDI dates')

    if start_date.day < end_date.day:
        # First pro-rata accrual uses the IPCA from the month previous to start_date
        prev_month = pd.to_datetime((start_date - pd.DateOffset(months=1)).strftime('%Y%m01'))
        curr_month = pd.to_datetime(start_date.strftime('%Y%m01'))
        first_acrual_date = pd.to_datetime(start_date.strftime('%Y%m')+end_date.strftime('%d'))
        tot_days = curr_month - prev_month
        num_days = first_acrual_date - start_date
        ipca_accum = (df_ipca.loc[curr_month].Accum / df_ipca.loc[prev_month].Accum) ** (num_days / tot_days)
        last_month = pd.to_datetime(end_date.strftime('%Y%m01'))
        ipca_accum = ipca_accum * (df_ipca.loc[last_month].Accum / df_ipca.loc[curr_month].Accum)
    elif start_date.day > end_date.day:
        # First pro-rata accrual uses the IPCA from start_date's month
        curr_month = pd.to_datetime(start_date.strftime('%Y%m01'))
        next_month = pd.to_datetime((start_date + pd.DateOffset(months=1)).strftime('%Y%m01'))
        first_acrual_date = pd.to_datetime(next_month.strftime('%Y%m')+end_date.strftime('%d'))
        tot_days = next_month - curr_month
        num_days = first_acrual_date - start_date
        ipca_accum = (df_ipca.loc[next_month].Accum / df_ipca.loc[curr_month].Accum) ** (num_days / tot_days)
        last_month = pd.to_datetime(end_date.strftime('%Y%m01'))
        ipca_accum = ipca_accum * (df_ipca.loc[last_month].Accum / df_ipca.loc[next_month].Accum)
    else:
        # There is no need to calculate pro-rata
        curr_month = pd.to_datetime(start_date.strftime('%Y%m01'))
        last_month = pd.to_datetime(end_date.strftime('%Y%m01'))
        ipca_accum = df_ipca.loc[last_month].Accum / df_ipca.loc[curr_month].Accum

    return(ipca_accum)


