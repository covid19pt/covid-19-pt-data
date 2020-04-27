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
    df1 = pd.read_csv(datapath)
    df1 = df1.set_index('Data (mm-dd)').unstack().to_frame('total').reset_index()
    df1 = df1.rename(columns={'level_0': 'ano'})
    df1['Data (mm-dd)'] = df1['Data (mm-dd)'].apply(modify_dates)
    df1['Data (mm-dd)'] = df1.ano + '-' + df1['Data (mm-dd)']
    df1 = df1[df1.total.notnull()]
    df1['data'] = pd.to_datetime(df1['Data (mm-dd)'])
    df1['total'] = df1['total'].astype(int)
    return df1[['data', 'total']]

def search_download(include_today=False):
    dpaths = list(Path('~/Downloads').expanduser().glob('Dados_SICO_*.csv'))
    if len(dpaths) == 0:
        print('No file found in Downloads')
        sys.exit()
    elif len(dpaths) > 1:
        print('Multiple files found in Downloads')
        sys.exit()
    data = preprocess(dpaths[0])
    if not include_today:
        if data['data'].max().date() == datetime.date.today():
            print('Last date is today! Removing')
            data = data[data['data'] != data['data'].max()]
    outputpath = Path(BASEPATH).expanduser() / 'mortalidade.csv'
    data.to_csv(outputpath, index=False)
    print('Updated mortalidade file')
    archivepath = Path(BASEPATH).expanduser() / 'archive' / dpaths[0].name
    shutil.move(dpaths[0], archivepath)
    print('Archived the file in ', archivepath)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rank query/docs')
    parser.add_argument('--include-today', action='store_true',
                        help="Include current day")
    args = parser.parse_args()
    search_download(args.include_today)
