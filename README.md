# API-Gathering
Code for extracting information from Contrataciones Públicas Ecuador API In the process of building the DataHub we defined the data we would be the most interested in extracting. One of such data is the one for public contracts, provided by Ecuador's government. A web scraping algorithm won't be necessary for this operation, because the government has designed an API that allows us to browse through the data and aquire all the information available for each contracting project the government's institutions have concluded.

The following code shows makes the API call using the endpoint, and extracts a user determined set of observations, along with the variables specified by the user.

### How to use Compras Públicas OCDS API scraper:
1) You must make sure you have the two main files for the script; namely: gathering_single_text.py and scrape.py

2) gathering_single_text.py provides the structure and methods for reading into the API's database, whereas scrape.py facilitates
the gathering of such information.

3) Open python in your preferred terminal.

4) Import the scrape method from the scrape.py file.

5) In the terminal type in scrape(<year you want to get information from>) to start running the program. The method will generate
a folder for the year's information if not already there, and start acquiring and saving the information in txt files. Each of these
contain a total of 10 government projects and their details.

6) Leave the program be until the information has been completely gathered. The program is robust to network blocks and losses of internet,
so don't worry about them and let the script do its thing.

7) For many years it's a good idea to use virtual machines from Google and run an instance of the program on each of them; this makes the
gathering way faster, as it downloads data from many years simultaneously.