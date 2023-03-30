import requests
import os
import sys
import csv
import numpy as np

int(sys.argv[1])  
int(sys.argv[2])


def batch_process(year, index, batch_size):
    """
    Batch processing API calls
    
    Parameters
    ----------
    year : Numeric
    index : np.ndarray
    batch_size: Numeric
    """
    n = len(index)
    
    for ii in np.arange(0, n, batch_size):
        batch = index[ii:ii+batch_size]
        
        # get data for batch
        data = get_data(year, batch)
        
        # write to csv
        write_data(data)
        

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
            res = requests.get(url, 
                         params={'year':int(year), 'page':int(page)})\
            .json()['data']
        except:
            res = []
        
        data += res
        
    return data

def write_data(data):
    """
    Function writes rows to csv 
    """
    # select keys as column names
    cols = data[0].keys()

    with open('../data/contratacion_db.csv', 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        
        # write column/field names if file is created
        if not os.path.isfile('../data/contratacion_db.csv'):
            writer.writeheader()
        
        # write csv rows with data dictionaries 
        writer.writerows(data)   