import requests
import os
import re
import csv
import time
import sys
import numpy as np

def batch_process(year, index, batch_size):
    """
    Batch processing API calls. A batch is a subset of index of pages to be retrieved.
    
    Parameters
    ----------
    year : Numeric
    index : np.ndarray
    batch_size: Numeric
    """
    n = len(index)
    
    for ii in np.arange(0, n, batch_size):
        # batch is subset of index
        batch = index[ii:ii+batch_size]
        print(f'running batch: {batch[0]}-{batch[-1]}')
        
        # get data for batch
        data = get_data(year, batch)
        
        # write to csv
        if ii == 0:
            write_data(data, write_header=True)
        else:
            write_data(data, write_header=False)
        
        
def tweak_record(dd):
    """
    """
    return {'id' : dd['id'] if dd['id'] is not None else np.nan,
            'ocid' : dd['ocid'] if dd['ocid'] is not None else np.nan, 
            'metodo' : dd['method'].title() if dd['method'] is not None else np.nan, 
            'tipo' : dd['internal_type'] if dd['internal_type'] is not None else np.nan, 
            'loc' : dd['locality'].title() if dd['locality'] is not None else np.nan, 
            'prov' : dd['region'].title() if dd['region'] is not None else np.nan, 
            'proveedores' : dd['suppliers'].title() if dd['suppliers'] is not None else np.nan, 
            'contratante' : dd['buyer'].title() if dd['buyer'] is not None else np.nan, 
            'ejecutado' : round(float(dd['amount']), 2) if dd['amount'] is not None else np.nan, 
            'presupuesto' : round(float(dd['budget']), 2) if dd['budget'] is not None else np.nan, 
            'descripcion' : re\
                .sub('(\s)?.rden de compra para adquirir los siguientes productos:(\s)+', '',  dd['description'])\
                .title()\
                .strip() if dd['description'] is not None else np.nan,
            'date' : dd['date'].replace('T', ' ')  if dd['date'] is not None else np.nan,}


def get_data(year, index):
    """
    Function makes API calls to the SERCOP's open data API. It stores data on government's procurement operations and write \
    the data to the file contratacion_db.csv.
    
    Parameters
    ----------
    year : Numeric
        Must be an int or float or object to be able to be converted to int data type.
    index : Numeric
        Must be an int or float or object to be able to be converted to int data type.
          
    """
    data = []
    url = 'https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds'
    
    for page in index:
        try:
            response = requests.get(url, params={'year':int(year), 'page':int(page)})
            
            if int(response.headers.get('X-RateLimit-Remaining')) < 2 | response.status_code == 429:
                time.sleep(8)
                
            if response.status_code == 200:
                page_ = response.json()['page']
                data_ = response.json()['data']
            else:
                page_ = None
                data_ = None
            
            
            data_ = [tweak_record(dd) for dd in data_]
            
            print('-'*100)
            print('Parsing page: ', page_)
            print('Response Status: ', response.status_code)
            print('Rate Limit Remaining: ', response.headers.get('X-RateLimit-Remaining'))
            print('Current Data Array Length: ', len(data))
            
            data.extend(data_)
            
        except Exception as e:
            print('-'*100)
            print('**Call crashed**')
            print(f'An error ocurred: {e}')
            print('Parsing page: ', page_)
            print('Response Status: ', response.status_code)
            print('Rate Limit Remaining: ', response.headers.get('X-RateLimit-Remaining'))
            print('Current Data Array Length: ', len(data))
            print('**Call crashed**')

    return data 


def write_data(data, write_header=False):
    """
    Function writes rows to csv 
    """
    # select keys as column names
    cols = data[0].keys()

    with open('../data/contratacion_db.csv', 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        
        # if write_header is set to True, write headers.
        if write_header:
            writer.writeheader()
        
        # write csv rows with data dictionaries 
        writer.writerows(data)
        
        
if __name__ == '__main__':
    year = int(sys.argv[1])  
    batch_size = int(sys.argv[2])
    num_pages = int(sys.argv[3])

    index=np.arange(1, num_pages + 1, 1)
    
    print(f'\n1#. Year selected: {year}\n2#. Number of Pages: {num_pages}\n3#. Size of Individual Batch: {batch_size}')
    
    # log beginning time
    print(f'Began at {time.strftime("%H:%M:%S")}\n')
    start = time.time()
    batch_process(year, index, batch_size)
    # log ending time
    print(f'\nFinished at {time.strftime("%H-%M-%S")}')
    end = time.time()
    print(f'Execution runtime: {end - start:.2f} seconds')


    

# @investigate why no file is being written