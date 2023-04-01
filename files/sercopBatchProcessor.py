import requests
import os
import re
import csv
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
            res = []
            for dd in requests.get(url, params={'year':int(year), 'page':int(page)}).json()['data']:
                dd_out = {}
                dd_out['id'] = dd['id']
                dd_out['ocid'] = dd['ocid']
                dd_out['method'] = dd['method'].title()
                dd_out['internal_type'] = dd['internal_type']
                dd_out['loc'] = dd['locality'].title()
                dd_out['prov'] = dd['region'].title()
                dd_out['proveedores'] = dd['suppliers'].title()
                dd_out['contratante'] = dd['buyer'].title()
                dd_out['ejecutado'] = round(float(dd['amount']), 2)
                dd_out['presupuesto'] = round(float(dd['budget']), 2)
                dd_out['descripcion'] = dd['description']
                dd_out['descripcion'] = re\
                    .sub('(\s)?.rden de compra para adquirir los siguientes productos:(\s)+', '',  dd['description']) \
                    .title() \
                    .strip()
                dd_out['date'] = dd['date'].replace('T', ' ')
                
                res.append(dd_out)
                
        except:
            res = []
        
        data += res
        
    return data


def write_data(data, write_header=False):
    """
    Function writes rows to csv 
    """
    # select keys as column names
    cols = data[0].keys()

    with open('../data/contratacion_2023_db.csv', 'a', newline='', encoding='utf-8') as f:
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
    
    print(f'1#. Year selected:{year}\n2#. Number of Pages: {num_pages}\n3#. Size of Individual Batch:{batch_size}')
    
    batch_process(year=year, index=index, batch_size=batch_size)