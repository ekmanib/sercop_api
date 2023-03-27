#Database Creation Using API
#Import Libraries
import time
import requests
import pandas as pd
import os

#Define a list of relevant variables to automatize information acquisition
relevant_vars = ["year","ocid","date","region","title","description","method","suppliers","buyer","amount"\
                 ,"budget"]#Define a list of relevant variables to automatize information acquisition

#Access to API's data
#API number 1: "Búsqueda de procesos de contratación por medio de palabra"

#Need an initial response to start while loop
def firstResponse(yr,pge):
    url_t = "https://datosabiertos.compraspublicas.gob.ec/PLATAFORMA/api/search_ocds"
    payload = {"year":yr,"page":pge}
    r = requests.get(url_t,params=payload)
    return r

#Individual information saver.
def infoSave(variables,response,item):
    temp = []
    for i in variables:
        i = response["data"][item][str(i)]
        temp.append(i)   
    return temp 

def mergeData(directory,yr):
    master_list = []
    if os.path.exists(f"{directory}/Year_{yr}.csv") == True:
        os.remove(f"{directory}/Year_{yr}.csv")
    for file in os.listdir(directory):
        #List with Dataframes
        master_list.append(pd.read_csv(f"{directory}/{file}"))
    #Merge all dataframes into a single big one
    master_df = pd.concat(master_list).reset_index(drop = True)
    #Save master dataframe as csv
    master_df.to_csv(f"{directory}/Year_{yr}.csv",index = False)

#Information gatherer
def primeInfoGet(yr,url,varlist,obs=0,page=0):
    directory = f"Compras_Publicas_{yr}"
    start = time.time()
    
    
    #First Response
    r = firstResponse(yr,1)
    rp = r.json()
    observations = 0
    page_count = page - 1
    debug_count = 0
    #Creation of empty Pandas Dataframe to save all the pertinent information from the database.
    #If no observations parameter is set, automatically gather all available data for that year.
    #Make all the API calls for the specific year (each page represents a call)
    pagina = rp["pages"]
    #Also, only run the code if and only if the response from the API is equal to 200.
    while pagina - rp["page"] > 1:
    #If we try to scrape all the data without giving the API some time it will eventually
    #block access to it, which will raise an error for some unspecified amount of time. The
    #try except statement allows the script to wait some time in case the error is raised, and
    #start over the iteration as soon as access is regained.
        datitos = varlist[:]
        try:
            if obs == 0:
                pass
            elif observations >= obs:
                break
            #API Call that contains the relevant information.
            page_count = page_count + 1
            url_n = url 
            payload = {"page":str(page_count),"year":str(yr)}
            #Incorporate gateway through session:

            #DEBUG
            #print(payload)
            r = requests.get(url_n,params=payload)
            print(r)
            rp = r.json()
            pg = rp["page"]
#             print(f"current page = {pg}")
            if not os.path.isdir(directory):
                os.mkdir(directory)
            #Generate a text file with the variable names.
            with open(f"{directory}/{pg}.txt","w") as f:
                stepy1 = f"{varlist}".replace("[","")
                stepy2 = stepy1.replace("]","")
                stepy3 = stepy2.replace("'","")
                f.write(f"{stepy3}")
            #Now that the call has been made, save this information in many variables as a txt file.
            for item in range(len(rp["data"])):
                with open(f"{directory}/{pg}.txt","a") as f:
                    step1 = str(infoSave(varlist,rp,item)).replace("[","")
                    step2 = step1.replace("]","")
                    step3 = step2.replace("'","")
                    f.write("\n"+step3)
                observations = observations + 1

                #After storing the information in the variables, append it to the pandas dataframe
                #DEBUG
                if obs == observations:
                    break

            debug_count += 1

#             print(f"page_count = {page_count}")
     #In case a connection error of any type happens...
        except:
            
#           Connection Forbidden or Blocked
            if r.status_code != 429:
                print("Connection Failed... retrying")
                rp = firstResponse(yr,page_count-1).json()
                page_count = page_count - 1
#           Too Many requests
            else:
                print("Too many requests. Waiting to regain access...")
                time.sleep(5)
                page_count = page_count - 1
    #Merge files into a single csv file:
    # mergeData(directory,yr)
    
    end = time.time()
    print(f"Success! Total iteration time:{end-start}")