import os
from gathering_single_text import primeInfoGet


def scrape(year):
    while True:
        variables = ["year","ocid","date","region","title","description","method","suppliers","buyer","amount","budget"]
        if not os.path.isdir(f"Compras_Publicas_{year}"):
            stuff = 1
            print(f"stuff = {stuff}")
        else:
            stuff = len(os.listdir(f"Compras_Publicas_{year}"))+1
            print(f"stuff = {stuff}")
        try:
            a = primeInfoGet(year,"https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds",variables,
                              obs=0,page=stuff)
            print(f"It's finally finished!")
            break
        except:
            print("collection failed")
            pass
