# TODO: research ml repo has these in scrape form.
span_margins = {
    'cl': 5_000,
    'ec': 1_000,
    'mbt': 2_000,
    'ng': 5_000,
    'zs': 4_000,
    'mnq': 2_000,
    'mes': 1_500,
    'nq': 20_000,
    'es': 15_000,
}

# Made these huge on purpose...
overnight_margins = {
    'cl': 20_000,
    'ec': 5_000,
    'mbt': 10_000,
    'ng': 15_000,
    'zs': 10_000,
    'mnq': 4_000,
    'mes': 3_500,
    'nq': 40_000,
    'es': 35_000,
}


def dollars_to_pct(df, margins, true_pct=True):
    """
    NOTE: we can pass SPAN margins, or anything really in here...
    this IS dependent on the product being IN the column name.
    """
    df = df.copy(deep=True)
    multiplier = 100.0 if true_pct else 1.0
    for c in df.columns:
        for product, v in margins.items():
            if f'{product}_' in c:
                # This is a bit harsh, but accurate --
                # df[c] = (df[c] / v).fillna(0.0)  * multiplier

                # more 'smooth' would likely be -- do a + v, cumsum(), pct_change()
                df[c] = (df[c].cumsum() + v).pct_change().fillna(0.0) * multiplier
                break
    return df


# to convert skfolios summary (if using dollar value) to dollar value, away from % terms.
def convert_summary_percentage(cell):
    """
    Usage:
    summary_df = population.summary()
    summary_df = summary_df.map(convert_percentage)
    """
    if isinstance(cell, str) and cell.endswith('%'):
        return float(cell.replace('%', '')) / 100
    else:
        return cell


if __name__ == '__main__':
    import pandas as pd

    re = pd.read_csv('incubating_raw.csv', parse_dates=['date']).set_index('date')
    # re, map = read_ts_dataflow("C:\\TS_Logs\\*.txt", encode=False)
    rdf = re.dropna(thresh=int(re.shape[1] * .5)).fillna(0.0)

    rdf_pct_raw = dollars_to_pct(rdf, span_margins, true_pct=False)
    rdf_pct = dollars_to_pct(rdf, overnight_margins, true_pct=True)
