import asyncio
import aiohttp
import re
import csv
import sys
import time
import numpy as np

def batch_process(year, index, batch_size):
    
    n = len(index)
    
    for ii in np.arange(0, n, batch_size):
        batch = index[ii:ii+batch_size]
        print(f'running batch: {batch[0]}-{batch[-1]}')
        
        data = asyncio.run(get_data(year, batch))
        
        if ii == 0:
            write_data(data, write_header=True)
        else:
            write_data(data, write_header=False)

            
async def fetch(session, url, params_):
    """
    Fetch and transform loaded data
    """
    try:
        async with session.get(url, params=params_) as resp:
            # await the coroutine resp
            data_ = await resp.json()
            
            print(data_)
            print('*'*1000)
            
            # iterable to contain page's content 
            res = []
            
            # access data dicionary and process data fields
            for dd in data_['data']:
                try: 
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
                    dd_out['descripcion'] = re\
                        .sub('(\s)?.rden de compra para adquirir los siguientes productos:(\s)+', '',  dd['description']) \
                        .title() \
                        .strip()
                    dd_out['date'] = dd['date'].replace('T', ' ')
                    
                except Exception as e:
                    print("An error ocurred:", str(e))
                    
                    
                #add resulting dictionary to contents_
                res.append(dd_out)
            
            return res

    except aiohttp.ClientError as error:
        print(f'An error ocurred: {error}')
    

async def get_data(year, index):
    
    data = []
    url = 'https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds'
    
    async with aiohttp.ClientSession() as session:
        async with asyncio.TaskGroup() as tg:
            # there will be `batch_size` number of tasks in tks and each task (fetch) will return a list with 10 dictionaries
            tks = [tg \
                   .create_task(fetch(session, url, params_ = {'year':year, 'page':int(pp)})) for pp in index]
            # asyncio.gather method will return a list with each task's outputs
            batch_res = await asyncio.gather(*tks)
            
    return [obs for res in batch_res for obs in res]


def write_data(data, write_header=False):
    """
    Function writes rows to csv 
    """
    # select keys as column names
    cols = data[0].keys()

    with open('../data/contratacion_async_2023_db.csv', 'a', newline='', encoding='utf-8') as f:
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
    
    index = np.arange(1, num_pages + 1, 1)
    
    print(f'1#. Year selected: {year}\n2#. Number of Pages: {num_pages}\n3#. Size of Individual Batch: {batch_size}')
    
    # log beginning time
    print(f'Began at {time.strftime("%H:%M:%S")}')
    start = time.time()
    batch_process(year, index, batch_size)
    # log ending time
    print(f'Finished at {time.strftime("%H-%M-%S")}')
    end = time.time()
    print(f'Execution runtime: {end - start:.2f} seconds')
