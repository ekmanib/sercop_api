import aiohttp
import asyncio
import sys
import csv
import re
import time
import numpy as np

def batch_process(year, index, batch_size):
    """
    """
    n = len(index)
    
    for ii in np.arange(0, n, batch_size):
        batch = index[ii:ii+batch_size]
        
        data = asyncio.run(get_data(year, batch))
        
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

async def fetch(session, semaphore, url, year, page):
    """
    """
    async with semaphore:
        attempt = 1
        while attempt < 6:
            try:
                async with session.get(
                    url, params={"year": int(year), "page": int(page)}
                ) as response:
                    # execution of response.json()
                    res = await response.json()
                    page_ = res['page']
                    data_ = res['data']

                    print('-'*100)
                    print('Parsing page: ', page_)
                    print('Response Status: ', response.status)
                    print('Rate Limit Remaining: ', response.headers.get('X-RateLimit-Remaining'))

                    return [tweak_record(res) for res in data_]    
            except:
                await asyncio.sleep(30 - attempt)
                attempt += 1


async def get_data(year, index):
    """
    """
    # specify url
    url = "https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds"

    # create list to contain data
    data = []
    
    # semaphore allows to only handle 10 calls asynchronously
    semaphore = asyncio.Semaphore(10)
    
    async with aiohttp.ClientSession() as session:
        # there will be `batch_size` number of tasks in tkss and each task (fetch) will return a list with 10 dictionaries
        tkss = [asyncio\
                .create_task(fetch(session, semaphore, url, year, page)) for page in index]
        # asyncio.gather method will return a list with each task's output
        batch_res = await asyncio.gather(*tkss, return_exceptions=True)
        
        return [obs for res in batch_res for obs in res]
    
def write_data(data, write_header=False):
    """
    Function writes rows to csv 
    """
    # select keys as column names
    cols = data[0].keys()

    with open('../data/contratacion_async_db.csv', 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        
        # if write_header is set to True, write headers.
        if write_header:
            writer.writeheader()
        
        # write csv rows with data dictionaries 
        writer.writerows(data)


if __name__ == "__main__":
    year = int(sys.argv[1])
    start = int(sys.argv[2])
    end = int(sys.argv[3])
    batch_size = int(sys.argv[4])
    
    #set up the index of pages to be retrieved
    index = np.arange(start, end + 1)
        
    print(f'Retrieving pages from {start} to {end}')

    # log beginning time
    print(f'Began at {time.strftime("%H:%M:%S")}\n')
    start = time.time()
    batch_process(year, index, batch_size)
    # log ending time
    print(f'\nFinished at {time.strftime("%H-%M-%S")}')
    end = time.time()
    print(f'Execution runtime: {end - start:.2f} seconds')
