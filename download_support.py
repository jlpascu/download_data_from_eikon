import pandas as pd
import eikon as ek
import numpy as np
from tqdm.notebook import tqdm
from time import sleep
import sys
from datetime import date

def download(index_ric, 
             update_index, 
             lenght, 
             download_list_2, 
             item_list_1, 
             item_list_2, 
             item_list_3, 
             backtesting, 
             folder_path):
    '''
    Starts downloading process. 
    Downloads both item_list_1 and item_list_2 our just item_list_2 
    
    Args:
        index_ric: Eikon Index RIC
        update_index: Update index consituents True or False
        lenght: Amount of assets to be downloaded. Useful to detect code errors.
        download_list_2: if True we will download finacial items in item_list_2
        item_list_1: item list cotaining Eikon RIC of the financial item to be downloaded
        item_list_2: item list cotaining Eikon RIC of the financial item to be downloaded
        backtesting: True or False. Wether or not we are running a backtesting. 
        folder_path: Folder where our results will be saved
    Returns:
        None
    '''
    # Check which list to download
    if download_list_2 == True:
        # Start downloading process for only item list 2
        download_item_list_2(item_list_2, index_ric, lenght, update_index, backtesting, folder_path)
    elif download_list_2 == False:
        # Start downloading process for item list 1
        download_item_list_1(item_list_1, index_ric, lenght, update_index, backtesting, folder_path)
        # Start downloading process for item list 2
        download_item_list_2(item_list_2, index_ric, lenght, update_index, backtesting, folder_path)
        # Start downloading process for item list 3
        download_item_list_2(item_list_3, index_ric, lenght, update_index, backtesting, folder_path)


def download_item_list_1(item_list, index_ric, lenght, update_index, backtesting, folder_path):
    '''
    Downloads  financial data in item_list. 
    
    Args:
        item_list: item list cotaining Eikon RIC of the financial item to be downloaded
        index_ric: Eikon Index RIC
        lenght: Amount of assets to be downloaded. Useful to detect code errors.
        update_index: Update index consituents True or False
        backtesting: True or False. Wether or not we are running a backtesting.
        folder_path: Folder where our results will be saved
    Returns:
        None
    '''
    print('Total Progress')
    # Start process
    for item in tqdm(item_list):
        print('Downloading', item)
        # Create download class
        download_data = DownloadData(index_ric = index_ric, 
                                     fin_item = item, 
                                     update_index = update_index, 
                                     backtesting = backtesting, 
                                     folder_path = folder_path)
        # Download index constituents
        download_data.get_index_constituents()
        # Download data for all assets
        download_data.run_all_asset_list_1(lenght = lenght)

def download_item_list_2(item_list, index_ric, lenght, update_index, backtesting, folder_path):
    '''
    Downloads last price, price-to-earnings and market capitalization. 
    
    Args:
        item_list: item list cotaining Eikon RIC of the financial item to be downloaded
        index_ric: Eikon Index RIC
        lenght: Amount of assets to be downloaded. Useful to detect code errors.
        update_index: Update index consituents True or False
        backtesting: True or False. Wether or not we are running a backtesting.
        folder_path: Folder where our results will be saved
    Returns:
        None
    '''
    for item in tqdm(item_list):
        print('Downloading', item)
        # Create download class
        download_data = DownloadData(index_ric = index_ric, 
                                     fin_item = item, 
                                     update_index = update_index, 
                                     backtesting = backtesting, 
                                     folder_path = folder_path)
        # Download index constituents
        download_data.get_index_constituents()
        # Download last pe and price
        download_data.run_all_asset_list_2(lenght = lenght)
            
class DownloadData:
    def __init__(self, index_ric, fin_item, 
                 update_index, backtesting,
                 folder_path, start_date = None):
        '''
        Args:
            index_ric: Reuters index RIC 
            fin_item: Reuters code for financial item
            update_index: True or False depending if we want to update index constituents or not
            backtesting: True or False. Wether or not we are running a backtesting.
            folder_path: Folder where our results will be saved
            start_date: start date of the backtesting. Use only when we are downloading data for a backtesting.
        Returns:
            None
        '''
        self.index_ric = index_ric                           # Eikon equity index RIC
        self.fin_item = fin_item                             # Eikoon code for the financial reatio we will download
        self.fin_data_df = pd.DataFrame()                    # DataFrame with financial data of all index assets
        self.constituents_df = pd.DataFrame()                # DataFrame with the index constituents
        self.count = 0                                       # Counts iterations
        self.asset_excluded = []                             # This list contains the assets that API failed to download data
        self.update_index = update_index                     # True or False if we want index constituents to be updated
        self.backtesting = backtesting                       # True or False in case we are running a backtesting
        self.folder_path = folder_path                       # Folder where our results will be saved
        self.sdate = 'FY-10'                                 # Default start date year -10
        self.edate = 'FY0'                                   # Default end date current last year
        self.start_date = start_date
        self.column_names = self.update_dates(start_date)    # In case we run a backtesting, default dates must be updated
        
    def update_dates(self, start_date):
        '''
        Updates dates and column names in case we are running a backtesting
        
        Args:
            start_date: backtesting starting date
        Returns:
            None
        '''
        # If backtesting is equal to false, we maintain current column names 
        # and default sdate and edate are not changed form our Class. Start date is 
        # Update with current day
        if self.backtesting == False:
            # Update start date to today
            today = date.today()
            self.start_date = today.strftime("%Y%m%d")
            # Return correct current column names
            column_names = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
            return column_names
        # If backtesting is equal to True, column names, sdate
        # must be saved in the class.
        elif self.backtesting == True: 
            # 1) Update start and end date eikon code for get_data request
            edate = 2020 - int(start_date[:4])
            sdate = edate + 10
            self.edate = 'FY-' + str(edate)
            self.sdate = 'FY-' + str(sdate)
            # 2) Update column names of our results DF. 
            # Create list
            column_names = []
            # Isolate year date and substract 10 years
            starting_year = int(start_date[:4]) - 10
            # Create column list
            for unit in range(0,11,1):
                # Save years in string format
                year = str(starting_year + unit)
                column_names.append(year)
            # Update columns name for new years
            return column_names
        
    def get_index_constituents(self):
        '''
        Downloads index constituents names and RIC Eikon code
        We try to avoid downloading index constituents everytime. If update index is set
        to TRUE, index constituents is downloaded again. If it is set to False, code reads
        index pickle file.
        
        Args:
            None
        Returns:
            None
        '''
        if self.update_index == False:
            # Save file name
            file_name = self.index_ric[1:]
            # Read DF
            self.constituents_df = self.read_file(file_name, intermediate='final_data')
        elif self.update_index == True:
            # Download data
            self.constituents_df = ek.get_data(instruments = self.index_ric, 
                                               fields = ['TR.IndexConstituentRIC', 'TR.ETPConstituentName'])[0]
            # Save data into different formats
            df = self.constituents_df
            file_name = self.index_ric[1:]
            self.save_df(df, file_name, intermediate='final_data')

    def transform_df(self, df, asset_name):
        '''
        Transforms df structure from the one downloaded from eikon 
                	Instrument	Gross Profit
        0	POOL.OQ	471262000
        1	POOL.OQ	531590000
        2	POOL.OQ	567407000
        
        To
                2010	    2011	    	    
        POOL.OQ	1613746000	1793318000
        
        Args:
            df: DF containg financial information dwonloaded from Eikon terminal
            asset_name : Eikon asset RIC
        Returns: df with the appropiate structure
        '''
        index_current_name = df.columns[1]
        df = df.T.drop('Instrument')
        df.columns = self.column_names
        df = df.rename(index = {index_current_name: asset_name})
        return df
    
    def add_zeros_to_excluded_assets(self):
        '''
        Converts asset excluded to DF with years as columns
        and fills each column with 0
        
        Args:
            None
        Returns:
            None
        '''
        # Create DataFrame
        self.asset_excluded = pd.DataFrame(self.asset_excluded)
        # Fill each column with 0
        for year in self.column_names:
            self.asset_excluded[year] = np.nan
        # Set asset names column as index
        self.asset_excluded = self.asset_excluded.set_index(keys = 0)

    def get_data_list_2(self, instrument):
        '''
        Downdloads market data of financial iitem included in item list 2
        
        Args:
            insturment: RIC asset
        '''
        try:
            df, err = ek.get_data(instruments = instrument, 
                                  fields = self.fin_item, 
                                  parameters = {'SDate': self.start_date})
        except:
            print("Unexpected error occured for asset:", instrument, sys.exc_info()[0])
            print('Try again in 10 seconds')
            sleep(10)
            try: 
                df, err = ek.get_data(instruments = instrument, 
                                      fields = self.fin_item, 
                                      parameters = {'SDate': self.start_date})
            except:
                # If asset cannont be downloaded, we skip it and save it in excluded asset lists
                print("Unexpected error:", sys.exc_info()[0])
                print('Two times failed, we skip asset', instrument, 'We save it into our excluded asset list')
                self.asset_excluded.append(instrument)
        # If this is the first asset, save df into the class
        if self.count == 0:
            self.fin_data_df = df
        else:
            # If it is no the first asset, just append data to existing DF
            self.fin_data_df = self.fin_data_df.append(df)
    
    def run_all_asset_list_2(self, lenght):
        '''
        Downloads data specied when creating DownloadData for all ric codes included
        at constituents_df
        
        Args:
            lenght : limit the number of assets to be downloaded. Default 'all'
        Returns:
            None
        '''
        # Check lenght
        if lenght == 'all':
            lenght = len(self.constituents_df)
        # Run for every asset in the index
        for asset in tqdm(range(0, lenght, 1)):
            # Get asset RIC
            asset_ric = self.constituents_df.iloc[asset, 1]
            # Get data
            self.get_data_list_2(instrument = asset_ric)
            # Keep track of the number of assets beind downloaded
            self.count = self.count + 1
        # Add assets with no financial data to our final DF. 
        if len(self.asset_excluded) != 0:
            # Add zeros to excluded asset df
            self.add_zeros_to_excluded_assets()
            # Save excluded assets file
            self.save_df(df = self.asset_excluded, file_name=str(self.fin_item), intermediate='excluded_assets')
            # Append to final df
            self.fin_data_df = self.fin_data_df.append(self.asset_excluded)
        # Set index
        self.fin_data_df = self.fin_data_df.set_index('Instrument')
        # Delate index name
        self.fin_data_df.index.name = ""
        # Save final dataframes to csv & pickle files
        self.save_df(df = self.fin_data_df, file_name=str(self.fin_item), intermediate='final_data')
    
    def get_data_list_1(self, instrument):
        '''
        Downdloads fiancial data from fiscal year -10 to fiscal year 0
        
        Args:
            insturment: RIC asset
        '''
        # Create DataFrame to store EIKON answer
        df = pd.DataFrame()
        # Create object where we will save the number of iterations
        num_iter = 0
        # While DF lenght is smaller than one (Eikon Api answer is empty), we'll send another request
        while len(df) <= 1:
            try:
                # Make resquest to Eikon Api
                df, err = ek.get_data(instruments = instrument, 
                                    fields = self.fin_item, 
                                    parameters = {'SDate': self.sdate, 'EDate': self.edate})
            except:
                print("Unexpected error occured for asset:", instrument, sys.exc_info()[0])
                print('Try again in 10 seconds')
                sleep(10)
                try:
                    df, err = ek.get_data(instruments = instrument, 
                                        fields = self.fin_item, 
                                        parameters = {'SDate': self.sdate, 'EDate': self.edate})
                except:
                    # If asset cannont be downloaded, we skip it and save it in excluded asset lists
                    print("Unexpected error:", sys.exc_info()[0])
                    print('Two times failed, we skip asset', instrument, 'We save it into our excluded asset list')
                    self.asset_excluded.append(instrument)
            if num_iter == 2:
                print('We have received several empty answers for', instrument, 'We save it into our excluded asset list')
                # If it is empty, save asset into our excluded asset list
                self.asset_excluded.append(instrument)
                # Break iteration
                break
            # Count iteration
            num_iter += 1
        # In case we have received a non empty answer
        if len(df) > 1:
            # Transfor DF format
            df = self.transform_df(df, instrument)
            # If this is the first asset, create pd DF
            if self.count == 0:
                self.fin_data_df = df
            else:
                # If it is no the first asset, just append data to existing DF
                self.fin_data_df = self.fin_data_df.append(df)

    def run_all_asset_list_1(self, lenght):
        '''
        Downloads data financial data for the Eikon code stored in self.fin_item
        for all ric codes included at constituents_df

        Args:
            lenght: limit the number of assets to be downloaded. Default 'all'
        Returns:
            None
        '''
        # Check lenght
        if lenght == 'all':
            lenght = len(self.constituents_df)
        # Run for every asset in the index
        for asset in tqdm(range(0, lenght, 1)):
            # Get asset RIC
            asset_ric = self.constituents_df.iloc[asset, 1]
            # Get data
            self.get_data_list_1(instrument = asset_ric)
            # Keep track of the number of assets beind downloaded
            self.count = self.count + 1
        # Add assets with no financial data to our final DF. 
        if len(self.asset_excluded) != 0:
            # Add zeros to excluded asset df
            self.add_zeros_to_excluded_assets()
            # Save excluded assets file
            self.save_df(df = self.asset_excluded, file_name=str(self.fin_item), intermediate='excluded_assets')
            # Append to final df
            self.fin_data_df = self.fin_data_df.append(self.asset_excluded)
        # Save final dataframes to 
        self.save_df(df = self.fin_data_df, file_name=str(self.fin_item), intermediate='final_data')

    def save_df(self, df, file_name, intermediate = ""):
        '''
        Save DF to different formats
        
        Args:
            df: DataFrame to be saved
            file_name: name of the saved file
            intermediate: additional path file can be passed for specifc folders
        Returns:
            None
        '''
        # build path field
        path_file = self.folder_path + self.index_ric[1:] + '/' + 'data_downloaded/' + intermediate + '/'
        df.to_pickle(path_file + 'pkl/' + file_name + '.pkl')
        df.to_csv(path_file + 'csv/' + file_name + '.csv')
        df.to_excel(path_file + 'xlsx/' + file_name + '.xlsx')

    def read_file(self, file_name, intermediate = ""):
        '''
        Reads file name

        Args: 
            file_name: name of the file to be opened
        Returns:
            df: DF
        '''
        # Build path
        path_file = self.folder_path + self.index_ric[1:] + '/' + 'data_downloaded/' + intermediate + '/'
        # Reutrns DF
        df = pd.read_pickle(path_file + 'pkl/' + file_name + '.pkl')
        return df

#### End of DownloadData Class ####

def create_index_df(index_ric):
    '''
    Downloads asset names and saves index in a dataframe. This function has been coded for 
    equity indexes constiuents that are not included in our current Eikon license. 
    
    Args:
        index_ric: Eikon Index RIC
    Returns:
        None
    '''
    # Read excel file
    index_df = pd.read_excel(index_ric[1:] + '.xlsx', header=None)
    # Change column original name
    index_df.columns = ['Constituent RIC']
    # Include index RIC column
    index_df['Instrument'] = index_ric
    # Download all names
    asset_name_list = []
    for ric in tqdm(index_df.loc[:,'Constituent RIC']):
        try:
            name, err = ek.get_data(
            instruments = [ric],
            fields = ['TR.CommonName']
            )
            asset_name_list.append(name.loc[0,'Company Common Name'])
        except:
            # We wait 10 seconds and try downloading again
            print("Unexpected error occured for asset:", ric)
            print('Try again in 10 seconds')
            sleep(10)
            try:
                name, err = ek.get_data(
                instruments = [ric],
                fields = ['TR.CommonName']
                )
                asset_name_list.append(name.loc[0,'Company Common Name'])
            except:
                print('Two times failed, save None for this asset', ric)
                asset_name_list.append(None)
    # Change list into dataframe
    index_df['Constituent Name'] = asset_name_list
    # Reorder columns
    index_df = index_df[['Instrument', 'Constituent RIC', 'Constituent Name']]
    # Save index dataframe to different files format
    folder = 'current_data/' + index_ric[1:] + '/' + 'data_downloaded/final_data/'
    path_file_pkl = folder + 'pkl/' + index_ric[1:] + '.pkl'
    path_file_csv = folder + 'csv/' + index_ric[1:] + '.csv'
    path_file_xlsx = folder + 'xlsx/' + index_ric[1:] + '.xlsx'
    index_df.to_pickle(path_file_pkl)
    index_df.to_csv(path_file_csv)
    index_df.to_excel(path_file_xlsx)
