import pandas as pd
import eikon as ek
import numpy as np
from tqdm.notebook import tqdm
import download_support

# Set Eikon App Key
ek.set_app_key('DEFAULT_CODE_BOOK_APP_KEY')

# Eikon Index RIC
index_ric = '.SPX'
# Start date
start_date = '20160101'
# Confirm back testing
backtesting = True
# Update index consituents True or False
update_index = False 
# Digit number or 'all'. We can set a limit amount of assets to be downloaded. 
# Usefull when debugging the code. Otherwise, set 'all'
lenght = 'all'
# Folder where our results will be saved
folder_path = 'backtesting/' 

'''
'TR.GrossProfit'
'TR.GrossMargin'
'TR.InterestIncNetNonOpTotal',
'TR.CashFromFinancingActivities',
'TR.TreasuryStockCommon', 
'TR.RetainedEarnings', 
'TR.IssuanceRetirementOfStockNet', 
'TR.NetDebtTot'
'''

# Item list to download
item_list_1 = ['TR.TotalRevenue', 
               'TR.OperatingIncome', 
               'TR.NetIncomeBeforeTaxes',
               'TR.NetIncomeBeforeExtraItems',
               'TR.DilutedEpsExclExtra',
               'TR.CashAndSTInvestments', 
               'TR.CashFromOperatingActivities',
               'TR.CashFromInvestingActivities',
               'TR.TotalReceivablesNet', 
               'TR.TotalDebtOutstanding', 
               'TR.TotalLiabilities',
               'TR.TotalEquity',
               'TR.CapitalExpenditures', 
               'TR.TotalAssetsReported']

# Additional items to be downloaded
item_list_3 = ['TR.GICSSubIndustry', 'TR.GICSSubIndustryCode']

print('Total Progress')
# Start process
for item in tqdm(item_list_1):
    print('Downloading', item)
    # Create download class
    download_data = download_support.DownloadData(index_ric = index_ric, 
                                                  fin_item = item, 
                                                  update_index = update_index, 
                                                  backtesting = backtesting,
                                                  folder_path = folder_path,
                                                  start_date = start_date)
    # Download index constituents
    download_data.get_index_constituents()
    # Download data for all assets
    download_data.run_all_asset_list_1(lenght = lenght)

# Download industry name and code
for item in tqdm(item_list_3):
    print('Downloading', item)
    # Create download class
    download_data = download_support.DownloadData(index_ric = index_ric, 
                                                  fin_item = item, 
                                                  update_index = update_index, 
                                                  backtesting = backtesting,
                                                  folder_path = folder_path,
                                                  start_date = start_date)
    # Download index constituents
    download_data.get_index_constituents()
    # Download data for all assets
    download_data.run_all_asset_list_2(lenght = lenght)
