import pandas as pd
import eikon as ek
import numpy as np
import download_support
from tqdm.notebook import tqdm

# Function parameters
# -------------------
# Eikon Index RIC
index_ric = '.SPX' 
# Update index consituents True or False
update_index = True  
# Digit number or 'all'. We can set a limit the amount of assets to be downloaded. 
# Usefull when debugging the code. Otherwise, set 'all'
assets_to_download = 'all'
# We will only download all information contained in item_list_2 if set to True. 
download_list_2 = False    
# Folder where our results will be saved
folder_path = 'current_data/'

# Set Eikon App Key
ek.set_app_key('DEFAULT_CODE_BOOK_APP_KEY')

'''
'TR.GrossProfit'
'TR.GrossMargin'
'TR.InterestIncNetNonOpTotal',
'TR.CashFromFinancingActivities',
'TR.TreasuryStockCommon', 
'TR.RetainedEarnings', 
'TR.IssuanceRetirementOfStockNet', 
'TR.NetDebtTot',
'TR.TotalRevenue', 
'TR.OperatingIncome', 
'TR.NetIncomeBeforeTaxes',
'TR.NetIncomeBeforeExtraItems',
'TR.DilutedEpsExclExtra',
'TR.CashAndSTInvestments', 
'TR.CashFromOperatingActivities',
'TR.CashFromInvestingActivities',
'''

# Item list to download
item_list_1 = ['TR.TotalReceivablesNet', 
               'TR.TotalDebtOutstanding', 
               'TR.TotalLiabilities',
               'TR.TotalEquity',
               'TR.CapitalExpenditures', 
               'TR.TotalAssetsReported']

# Reduced item list to download
item_list_2 = ['TR.PriceClose', 'TR.CompanyMarketCap', 'TR.PE']

# Additional items to be downloaded
item_list_3 = ['TR.GICSSubIndustryCode']

# Run function
download_support.download(index_ric = index_ric, 
                          update_index = update_index,
                          lenght = assets_to_download, 
                          download_list_2 = download_list_2, 
                          item_list_1 = item_list_1, 
                          item_list_2 = item_list_2,
                          item_list_3 = item_list_3, 
                          backtesting = backtesting, 
                          folder_path = folder_path)
