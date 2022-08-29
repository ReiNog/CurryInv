import pandas as pd

def calc_accum_r252(rate252, percentage=1):
    # Calculates the cumulative return for the Rate in df_rate252 using a given percentage of the rate
    # Rate is an exponential annual rate base 252 (workdays)
    # df_rate252 must have dates in the index and a column named Rate. The index must be sorted in ascending order.

    # For each day d: Cum_Return_d = ( 1 + (Rate_d-1 * Percentage) ^ (1/252)) * Cum_Return_d-1

    with pd.option_context('mode.chained_assignment', None):
        rate252['Accum'] = (((((1 + rate252.Rate.shift(1)) ** (1/252) - 1) * percentage)) + 1).cumprod()

    # The Cum_Perc of the first day gets NaN, it is necessary to make it = 1.0000

    rate252.loc[rate252.first_valid_index()].Accum = 1.0000

    return(rate252)