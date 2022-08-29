import pandas as pd

def call_ibge_api(series_id, date_list, date_column_name='TradeDate', value_column_name='Value'):
    api_url = f'https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/{date_list}/variaveis/{series_id}?localidades=N1[all]'
        
    try:
        req = pd.read_json(api_url)
    except Exception as ex:
        raise Exception(ex)
    
    if not req.empty:
        # if req is empty, there is no new data to add
        values = req['resultados'][0][0]['series'][0]['serie']
        new_values = pd.DataFrame(values.items(), columns=[date_column_name, value_column_name])
        new_values[date_column_name] = pd.to_datetime(new_values[date_column_name], format='%Y%m')
        new_values.set_index(date_column_name, inplace=True)
    else:
        new_values = pd.DataFrame(data=[])

    return(new_values)

def get_ipca_from_ibge(start_date, end_date):
    # Gets the values of the IPCA, for the period start_date to end_date, from the IBGE api

    date_list = pd.period_range(start = start_date, end = end_date, freq='M').strftime("%Y%m").to_list()
    date_list = '|'.join(date_list)

    try:
        # First gets the IPCA index numbers - series_id = '2266'
        new_values = call_ibge_api('2266', date_list, 'TradeDate', 'Num_IPCA')
        #Then gets the IPCA monthly rates - series_id = '63'
        new_values2 = call_ibge_api('63', date_list, 'TradeDate', 'IPCA')
        if new_values2['IPCA'].dtype != float and new_values2['IPCA'].dtype != int:
            new_values2['IPCA'] = float(new_values2['IPCA'])
        new_values2['IPCA'] = new_values2['IPCA'] / 100
        new_values = pd.concat([new_values, new_values2], axis=1)
    except Exception as ex:
        raise Exception(ex)
    
    
    return(new_values)