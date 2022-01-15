# download_data_from_eikon
In this depository you can find code examples on how to download financial data from Eikon API for all the asset that belong to an equity index. 

download_support.py file is the module we will use as support. Make sure it is saved in your venv. 

To start downloading, run run_file.py file. Before running, make sure the following folders have been created on your venv. This is where data will be saved.  

current_data/SPX/data_downloaded/ 

  excluded_assets/ 

csv/ 

pkl/ 

xlsx/ 

final_data 

csv/ 

pkl/ 

xlsx/ 

I like to save data on different formats in case I need to share it, analyze it, open file using excel, etc 

Index_ric: Eikon index RIC. You can choose any equity index you’d like to download. Examples: 
    SPX: S&P 500 
    IBEX: Ibex 35 
    FCHI: Cac 40 

update_index : set it to true in case you’d like to update the equity index constituents' members.  

item_list_1: the code will download the last 11 years of the financial data included in this list for every asset that belongs to the equity index chosen.

item_list_2: the code will download the last trading date data of the financial data included in this list for every asset 
 
