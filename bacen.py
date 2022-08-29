import pandas as pd

def get_bacen_data(series_name, start_date, end_date):
    # Gets the values of the series with series_name, for the period start_date to end_date, from the Brazilian Central Bank api

    # Mapping the series_name to the series_id used by BCB.
    seriesmap = {'BRLUSD': 1, 'CDI' : 4389, 'IPCA' : 433, 'Selic': 1178}
    try:
        series_id = seriesmap[series_name]
    except KeyError as err:
        raise KeyError(err)
    except:
        raise Exception(err)

    try:
        url = f'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados?formato=json&dataInicial={start_date:%d/%m/%Y}&dataFinal={end_date:%d/%m/%Y}'
        series = pd.read_json(url)
        series['data'] = pd.to_datetime(series['data'], dayfirst=True)
        series.set_index('data', inplace=True)
    except:
        raise TypeError('Error in fetching data from BCB api')

    # The IPCA, Selic and CDI rates are published as percentage. We store and work whith them already divided by 100.
    if (series_id == 433 or series_id == 1178 or series_id == 4389):
        series['valor'] = series['valor'] / 100
        
    return series