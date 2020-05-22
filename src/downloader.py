"""
Script to convert the downloaded data from DGS
"""

import sys
import datetime
import argparse
import shutil
from pathlib import Path
import pandas as pd

BASEPATH = '~/git/covid-19-pt-data'

mapper = {'Fev':'Feb', 'Abr':'Apr', 'Mai':'May', 'Ago':'Aug','Set':'Sep',
          'Out':'Oct', 'Dez':'Dec'}

def modify_dates(txt):
    m,d = txt.split('-')
    return mapper.get(m, m) + '-' + d


def preprocess(datapath):
    data = pd.read_csv(datapath)
    if "Distrito" in data.columns:
        return preprocess_districts(data)
    else:
        return preprocess_global(data)


def get_data_col(df):
    for name in ["Data (mm-dd)", "Data"]:
        if name in df.columns:
            return name


def preprocess_districts(df):
    filename = "mortalidade_distritos_2020.csv"
    previous = pd.read_csv(Path(BASEPATH).expanduser() / filename, parse_dates=["data"])
    datacol = get_data_col(df)
    df[datacol] = df[datacol].apply(modify_dates)
    df[datacol] = "2020" + '-' + df[datacol]
    df["data"] = pd.to_datetime(df[datacol])
    df = df[["data", "Distrito", "Óbitos"]]
    # if some date was removed from previous, keep that date (this happens sometimes 
    # with the source data)
    removed_entries = set(map(tuple, previous[["data", "Distrito"]].values.tolist())) - \
                      set(map(tuple, df[["data", "Distrito"]].values.tolist()))
    for data, distrito in removed_entries:
        df = df.append({"data": data, "Distrito": distrito, 
                        "Óbitos": previous[previous.data.eq(data) & previous.Distrito.eq(distrito)]["Óbitos"].values[0]},
                        ignore_index=True)
        print("Reincluded: ", data, distrito)
    df = df.sort_values(["Distrito", "data"])
    return df, filename


def preprocess_global(data):
    filename = "mortalidade.csv"
    df1 = data
    datacol = get_data_col(df1)
    df1 = df1.set_index(datacol).unstack().to_frame('total').reset_index()
    df1 = df1.rename(columns={'level_0': 'ano'})
    df1[datacol] = df1[datacol].apply(modify_dates)
    df1[datacol] = df1.ano + '-' + df1[datacol]
    df1 = df1[df1.total.notnull()]
    df1['data'] = pd.to_datetime(df1[datacol])
    df1['total'] = df1['total'].astype(int)
    return df1[['data', 'total']], filename


def search_download(include_today=False):
    dpaths = list(Path('~/Downloads').expanduser().glob('Dados_SICO_*.csv'))
    if len(dpaths) == 0:
        print('No file found in Downloads')
        sys.exit()
    elif len(dpaths) > 1:
        print('Multiple files found in Downloads')
        sys.exit()
    data, outputname = preprocess(dpaths[0])
    if not include_today:
        if data['data'].max().date() == datetime.date.today():
            print('Last date is today! Removing')
            data = data[data['data'] != data['data'].max()]
    outputpath = Path(BASEPATH).expanduser() / outputname
    data.to_csv(outputpath, index=False)
    print('Updated mortalidade file')
    archivepath = Path(BASEPATH).expanduser() / 'archive' / outputname[:-4] /dpaths[0].name
    shutil.move(dpaths[0], archivepath)
    print('Archived the file in ', archivepath)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rank query/docs')
    parser.add_argument('--include-today', action='store_true',
                        help="Include current day")
    args = parser.parse_args()
    search_download(args.include_today)
