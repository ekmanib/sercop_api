import aiohttp
import asyncio
import sys
import time
import numpy as np

async def fetch(session, url, params_):
    async with session.get(url, params=params_) as response:
        res = await response.json()
        print('Response Status: ', response.status)
        print('Rate Limit Remaining: ', response.headers.get('X-RateLimit-Remaining'))
        print('At Page No. ', res['page'])
        print('Page Contents: ', len(res['data']))

async def get_data(year, index):
    
    url = 'https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds'
    
    async with aiohttp.ClientSession() as session:
        async with asyncio.TaskGroup() as tg:
            tkss = [tg.create_task(fetch(session, url, page)) for page in index]
            data = await asyncio.gather(*tkss)
            print(len(data))
            

if __name__ == '__main__':
    year = int(sys.argv[1])
    start = int(sys.argv[2])
    end = int(sys.argv[3])
    
    index = np.arange(start, end + 1, 1)
    
    # log beginning time
    print(f'Began at {time.strftime("%H:%M:%S")}\n')
    start = time.time()
    asyncio.run(get_data(year, index))
    # log ending time
    print(f'\nFinished at {time.strftime("%H-%M-%S")}')
    end = time.time()
    print(f'Execution runtime: {end - start:.2f} seconds')