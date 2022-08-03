import pandas as pd

def get_bacen_data(series_name, start_date_str, end_date_str):
    # Gets the values of the series with series_name, for the period start_date to end_date, from the Brazilian Central Bank api

    # Mapping the series_name to the series_id used by BCB.
    seriesmap = {'BRLUSD': 1, 'CDI' : 4389, 'IPCA' : 433, 'Selic': 1178}
    try:
        series_id = seriesmap[series_name]
    except KeyError as err:
        raise KeyError(err)
    except:
        raise('Unknown error')

    try:
        url = f'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados?formato=json&dataInicial={start_date_str}&dataFinal={end_date_str}'
        df_series = pd.read_json(url)
        df_series['data'] = pd.to_datetime(df_series['data'], dayfirst=True)
        df_series.set_index('data', inplace=True)
    except:
        raise TypeError('Error in fetching data from BCB api')

    # The IPCA, Selic and CDI rates are published as percentage. We store and work whith them already divided by 100.
    if (series_id == 433 or series_id == 1178 or series_id == 4389):
        df_series['valor'] = df_series['valor'] / 100
        
    return df_series