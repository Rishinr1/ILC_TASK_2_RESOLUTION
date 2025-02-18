


import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pyarrow.compute as pc
import gc
from decimal import Decimal  # Add this import statement
import pyarrow.dataset as ds
import shutil
import gc
import time
import sqlalchemy as sa
import pyodbc
import concurrent.futures
import re
from concurrent.futures import ThreadPoolExecutor





start_time = time.time()  # Start time


# Function to flush the cache
def flush_cache():
    gc.collect()

flush_cache()


folder_path = r'D:\RISHIN\14_2_1ILC_NZFL\PLT\Risk_Lob\GU\PeriodRange=1-250000' # enter the path of the parquet files GU
folder_path_gr =r'D:\RISHIN\14_2_1ILC_NZFL\PLT\Risk_Lob\GR\PeriodRange=1-250000'# enter the path of the parquet files GR
output_folder_path = r"D:\RISHIN\TESTING\TEST_8"   # enter the path where the output files should be saved
speriod=50000   # enter the simulation period
samples=5       # enter the number of samples
amplification="PLA"    # enter PLA or nPLA
ILCYEAR = "ILC2024"   # enter ILC and YEAR
ILCNAME = "NZFL"  # ILC NAME 
ANALYSIS_TYPE="EP" # enter EP or HIST
subperil = "" # enter subperil if any and you can order the suffix as you require


proname = f"{ILCYEAR}_{ILCNAME}_{ANALYSIS_TYPE}_{amplification}" # order the suffix as you require ,# add subperil also if any by adding extra _{subperil}  ..donot add currency  

currency="NZD" #enter currency
region=currency
database = "IED2024_NZFL_PC_NZD_EDM240_ILCRun"#enter attached database name



# folder_path = r'D:\RISHIN\13_ILC_resolution\input\PARQUET_FILES'
# folder_path_gr = r'D:\RISHIN\13_ILC_TASK\input\PARQUET_FILES_GR'

# speriod=int(input("Enter the simulation period: "))
# samples=int(input("Enter the number of samples: "))
# proname=(input("enter file suffix example : example ILC2024_NZFL_EP_PLA suffix should have only 3 "_" ))
# region=input("enter region example : example NZD  ")
# database = input('Enter the database name IED2024_NZFL_PC_NZD_EDM240_ILCRun')

# proname="ILC2024_EUWS_PLA_WI_EP_BE"
# currency="EUR"
# region=currency
# database = "IED2024_EUWS_PC_MIX_EDM230_ILC_LOB_UPDATE_20240905"


hiphen_count=proname.count('_') + 7 #delete this if aint working


parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]

parquet_files_gr = [os.path.join(folder_path_gr, f) for f in os.listdir(folder_path_gr) if f.endswith('.parquet')]




def delete_folder_and_files(folder_path):
    
    if os.path.exists(folder_path):
        # Delete all files inside the folder
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        
        # Delete the folder itself
        os.rmdir(folder_path)
        print(f'Successfully deleted the folder: {folder_path}')
    else:
        print(f'The folder {folder_path} does not exist.')


# In[10]:


# Check if there are any Parquet files in the folder
if parquet_files:
    # Read the first Parquet file in chunks
    parquet_file = pq.ParquetFile(parquet_files[0])
    for batch in parquet_file.iter_batches(batch_size=1000):
        # Convert the first batch to a PyArrow Table
        table = pa.Table.from_batches([batch])
        
        # Convert the PyArrow Table to a Pandas DataFrame
        df = table.to_pandas()
        
        # Extract the first value of LocationName and split it by '_'
        location_name = df['LocationName'].iloc[0]
        country = location_name.split('_')[0]
        
        
        # Define the main folder path
        main_folder_path = os.path.join(output_folder_path, f'{proname}_{region}_Losses')
        
        # Define subfolders
        subfolders = ['EP', 'PLT', 'STATS']
        nested_folders = ['Lob', 'Portfolio','Admin1','Admin1_Lob','Cresta','Cresta_Lob',]
        innermost_folders = ['GR', 'GU']
        
        # Create the main folder and subfolders
        for subfolder in subfolders:
            subfolder_path = os.path.join(main_folder_path, subfolder)
            os.makedirs(subfolder_path, exist_ok=True)
            
            # Filter nested folders for 'PLT'
            if subfolder == 'PLT':
                filtered_nested_folders = ['Lob', 'Portfolio']
            else:
                filtered_nested_folders = nested_folders
            
            for nested_folder in filtered_nested_folders:
                nested_folder_path = os.path.join(subfolder_path, nested_folder)
                os.makedirs(nested_folder_path, exist_ok=True)
                
                for innermost_folder in innermost_folders:
                    innermost_folder_path = os.path.join(nested_folder_path, innermost_folder)
                    os.makedirs(innermost_folder_path, exist_ok=True)
        
        print(f"Folders created successfully at {main_folder_path}")
        break  # Process only the first batch
else:
    print("No Parquet files found in the specified folder.")


# In[13]:


main_folder_path = os.path.join(output_folder_path, f'{proname}_{region}_Losses')

processing_folder_path = os.path.join(main_folder_path, 'processing')
resolution_folder_path = os.path.join(processing_folder_path, 'Resolution Added')
resolution_folder_path_gr = os.path.join(processing_folder_path, 'Resolution Added_gr')
partial_folder_path = os.path.join(processing_folder_path, 'Partial')   
concatenated_folder_path = os.path.join(processing_folder_path, 'Concatenated')



# In[15]:


def connect_to_database(server, database):
    connection_string = f'mssql+pyodbc://{server}/{database}?driver=SQL+Server+Native+Client+11.0'
    engine = sa.create_engine(connection_string)
    connection = engine.connect()
    return connection

def read_parquet_file(file_path):
    table = pq.read_table(file_path)
    return table

def fetch_database_data(connection):
    address_query = 'SELECT ADDRESSID, ADMIN1GEOID AS Admin1Id, Admin1Name, zone1GEOID AS CrestaId, Zone1 AS CrestaName FROM Address'
    address_df = pd.read_sql(address_query, connection)
    address_table = pa.Table.from_pandas(address_df)
    return address_table



def join_dataframes(parquet_table, address_table):
    parquet_df = parquet_table.to_pandas()
    address_df = address_table.to_pandas()
    df = parquet_df.merge(address_df, left_on='LocationId', right_on='ADDRESSID', how='left')
    return pa.Table.from_pandas(df)

def save_joined_dataframe(joined_table, output_file):
    pq.write_table(joined_table, output_file)
    print(f"Saved joined file to {output_file}")

def process_file(file, address_table, output_folder):
    gc.collect()

    parquet_table = read_parquet_file(file)
    joined_table = join_dataframes(parquet_table, address_table)
    output_file = os.path.join(output_folder, os.path.basename(file))
    save_joined_dataframe(joined_table, output_file)
    del parquet_table
    del joined_table
    gc.collect()

def process_parquet_files(folder_path, output_folder, server, database, batch_size=2):
    os.makedirs(output_folder, exist_ok=True)
    connection = connect_to_database(server, database)
    address_table = fetch_database_data(connection)

    parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]
    connection.close()

    # Process files in batches
    for i in range(0, len(parquet_files), batch_size):
        batch_files = parquet_files[i:i + batch_size]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_file, file, address_table, output_folder) for file in batch_files]
            for future in concurrent.futures.as_completed(futures):
                future.result()

server = 'localhost'

process_parquet_files(folder_path, resolution_folder_path, server, database)


# In[16]:


process_parquet_files(folder_path_gr, resolution_folder_path_gr, server, database)


# In[2]:


parquet_files_grp = [os.path.join(resolution_folder_path, f) for f in os.listdir(resolution_folder_path) if f.endswith('.parquet')]
parquet_files_grp_gr = [os.path.join(resolution_folder_path_gr, f) for f in os.listdir(resolution_folder_path_gr) if f.endswith('.parquet')]

delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)

# In[22]:


def fetch_lobdet_data(server, database):
    connection = connect_to_database(server, database)
    try:
        lobdet_query = 'SELECT LOBNAME, LOBDETID FROM lobdet'
        lobdet_df = pd.read_sql(lobdet_query, connection)
        lobname_to_lobid_2 = dict(zip(lobdet_df.LOBNAME, lobdet_df.LOBDETID))
    finally:
        connection.close()
    return lobname_to_lobid_2


# In[33]:


lobname_to_lobid=fetch_lobdet_data(server, database)


# In[9]:


#EP_admin_lob
def process_parquet_files_2(parquet_files, filter_string, lob_id, speriod, samples, rps_values,parquet_file_path,Cat):
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')
    os.makedirs(partial_folder_path, exist_ok=True)
    os.makedirs(concatenated_folder_path, exist_ok=True)

    # Initialize an empty list to store the results
    final_grouped_table_1 = []

    # Process each Parquet file individually
    for file in parquet_files:
        # Read the Parquet file into a PyArrow Table
        table = pq.read_table(file)
        
        # Filter the table based on the filter_string
        table = table.filter(pc.equal(table['LobName'], filter_string))
        
        # Skip if the filtered table is empty
        
        grouped_table_1 = table.group_by(['EventId', 'PeriodId', 'EventDate','Admin1Name','Admin1Id']).aggregate([('Loss', 'sum')])
        grouped_table_1 = grouped_table_1.rename_columns(['EventId', 'PeriodId', 'EventDate','Admin1Name','Admin1Id','Sum_Loss'])
    
        # Write intermediate results to disk
        pq.write_table(grouped_table_1, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))

    # Read all intermediate files and concatenate them
    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]

    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]

    final_table_1 = pa.concat_tables(final_grouped_table_1)

    # Perform final grouping and sorting
    f_grouped_table_1 = final_table_1.group_by(['EventId', 'PeriodId', 'EventDate','Admin1Name','Admin1Id']).aggregate([('Sum_Loss', 'sum')])
    sorted_final_table_1 = f_grouped_table_1.sort_by([('Sum_Loss_sum', 'descending')])

    # Get distinct Admin1Id and Admin1Name
    distinct_admins = sorted_final_table_1.select(['Admin1Id', 'Admin1Name']).to_pandas().drop_duplicates()
    distinct_admins = distinct_admins.reset_index(drop=True)
    pq.write_table(sorted_final_table_1, os.path.join(concatenated_folder_path, 'final_grouped_table_1.parquet'))
     # Delete all non-concatenated files
    for f in intermediate_files_1:
        os.remove(f)
    
    for idx, row in distinct_admins.iterrows():
        admin1_id = row['Admin1Id']
        admin1_name = row['Admin1Name']
        # Filter the table for the current Admin1Id and Admin1Name
        filtered_table = sorted_final_table_1.filter(pa.compute.equal(sorted_final_table_1['Admin1Id'], admin1_id))
        dataframe_1 = filtered_table.to_pandas()

        dataframe_2 = dataframe_1.groupby(['PeriodId','Admin1Name','Admin1Id'], as_index=False).agg({'Sum_Loss_sum': 'max'})
        dataframe_2.rename(columns={'Sum_Loss_sum': 'Max_Loss'}, inplace=True)
        dataframe_2 = dataframe_2.sort_values(by='Max_Loss', ascending=False).reset_index(drop=True)

        dataframe_2['rate'] = (1 / (speriod * samples))
        # Compute cumulative rate and RPs
        dataframe_2['cumrate'] = dataframe_2['rate'].cumsum().round(6)
        dataframe_2['RPs'] = (1 / dataframe_2['cumrate'])

        # Given RP values list
        rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

        # Find the minimum RP in the dataframe
        min_rp = dataframe_2['RPs'].min()

        # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
        if min_rp > 2:
            # Find the RP values that need to be added
            missing_rps = [rp for rp in rp_values if rp < min_rp]

            new_rows = []
            for rp in missing_rps:
                temp_df = dataframe_2.iloc[[-1]].copy()  # Copy the last row as a template
                temp_df['RPs'] = rp
                new_rows.append(temp_df)

            # Concatenate the original dataframe with new rows
            new_rows_df = pd.concat(new_rows, ignore_index=True)
            dataframe_2 = pd.concat([dataframe_2, new_rows_df], ignore_index=True)

            # Fill NaN values only in the new rows
            new_rows_indices = new_rows_df.index
            dataframe_2.loc[new_rows_indices] = dataframe_2.loc[new_rows_indices].fillna(0)
        # Continue with further calculations
        dataframe_2['TCE_OEP_1'] = ((dataframe_2['Max_Loss'] - dataframe_2['Max_Loss'].shift(-1)) * (dataframe_2['cumrate'] + dataframe_2['cumrate'].shift(-1)) * 0.5)
        dataframe_2['TCE_OEP_2'] = (dataframe_2['TCE_OEP_1'].shift().cumsum() * dataframe_2['RPs'])
        dataframe_2['TCE_OEP_Final'] = (dataframe_2['TCE_OEP_2'] + dataframe_2['Max_Loss'])

        dataframe_3 = dataframe_1.groupby(['PeriodId','Admin1Name','Admin1Id'], as_index=False).agg({'Sum_Loss_sum': 'sum'})
        dataframe_3.rename(columns={'Sum_Loss_sum': 'S_Sum_Loss'}, inplace=True)
        dataframe_3 = dataframe_3.sort_values(by='S_Sum_Loss', ascending=False).reset_index(drop=True)

        dataframe_3['rate'] = (1 / (speriod * samples))
        dataframe_3['cumrate'] = dataframe_3['rate'].cumsum().round(6)
        dataframe_3['RPs'] = (1 / dataframe_3['cumrate'])


        # Given RP values list
        rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

        # Find the minimum RP in the dataframe
        min_rp = dataframe_3['RPs'].min()

        # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
        if min_rp > 2:
            # Find the RP values that need to be added
            missing_rps = [rp for rp in rp_values if rp < min_rp]

            new_rows = []
            for rp in missing_rps:
                temp_df = dataframe_3.iloc[[-1]].copy()  # Copy the last row
                temp_df['RPs'] = rp
                new_rows.append(temp_df)

            # Concatenate the original dataframe with new rows
            new_rows_df = pd.concat(new_rows, ignore_index=True)
            dataframe_3 = pd.concat([dataframe_3, new_rows_df], ignore_index=True)

            # Fill NaN values only in the new rows
            new_rows_indices = new_rows_df.index
            dataframe_3.loc[new_rows_indices] = dataframe_3.loc[new_rows_indices].fillna(0)
            
        dataframe_3['TCE_AEP_1'] = ((dataframe_3['S_Sum_Loss'] - dataframe_3['S_Sum_Loss'].shift(-1)) * (dataframe_3['cumrate'] + dataframe_3['cumrate'].shift(-1)) * 0.5)
        dataframe_3['TCE_AEP_2'] = (dataframe_3['TCE_AEP_1'].shift().cumsum() * dataframe_3['RPs'])
        dataframe_3['TCE_AEP_Final'] = (dataframe_3['TCE_AEP_2'] + dataframe_3['S_Sum_Loss'])

        fdataframe_2 = pd.DataFrame()
        fdataframe_3 = pd.DataFrame()

        for value in rps_values:
                                
                closest_index_2 = (dataframe_2['RPs'] - value).abs().idxmin()
                
                # Assign the closest value to the new DataFrames
                fdataframe_2 = pd.concat([fdataframe_2, dataframe_2.loc[[closest_index_2]]])
                fdataframe_3 = pd.concat([fdataframe_3, dataframe_3.loc[[closest_index_2]]])
                
                # Update the closest value to match the rp value exactly
                #fdataframe_2.at[closest_index_2, 'RPs'] = float(value)
                #fdataframe_3.at[closest_index_2, 'RPs'] = float(value)

        fdataframe_2.rename(columns={'Max_Loss': 'OEP', 'TCE_OEP_Final': 'TCE-OEP'}, inplace=True)
        columns_to_keep_2 = ['RPs', 'Admin1Name', 'Admin1Id']
        columns_to_melt_2 = ['OEP', 'TCE-OEP']
        melted_df_2 = fdataframe_2.melt(id_vars=columns_to_keep_2, value_vars=columns_to_melt_2, var_name='EPType', value_name='Loss')
        melted_df_2.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
        final_df_2 = melted_df_2[['EPType', 'Loss', 'ReturnPeriod', 'Admin1Name', 'Admin1Id']]

        fdataframe_3.rename(columns={'S_Sum_Loss': 'AEP', 'TCE_AEP_Final': 'TCE-AEP'}, inplace=True)
        columns_to_keep_3 = ['RPs', 'Admin1Name', 'Admin1Id']
        columns_to_melt_3 = ['AEP', 'TCE-AEP']
        melted_df_3 = fdataframe_3.melt(id_vars=columns_to_keep_3, value_vars=columns_to_melt_3, var_name='EPType', value_name='Loss')
        melted_df_3.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
        final_df_3 = melted_df_3[['EPType', 'Loss', 'ReturnPeriod','Admin1Name','Admin1Id']]

        final_df_EP_LOB_GU = pd.concat([final_df_2, final_df_3], ignore_index=True)
        new_ep_type_order = ["OEP", "AEP", "TCE-OEP", "TCE-AEP"]
        final_df_EP_LOB_GU['EPType'] = pd.Categorical(final_df_EP_LOB_GU['EPType'], categories=new_ep_type_order, ordered=True)
        final_df_EP_LOB_GU = final_df_EP_LOB_GU.sort_values(by=['EPType', 'ReturnPeriod'], ascending=[True, False]).reset_index(drop=True)

        # Add LobID and LobName columns
        final_df_EP_LOB_GU['LOBId'] = lob_id
        final_df_EP_LOB_GU['LOBName'] = filter_string
        final_df_EP_LOB_GU['LOBId'] = final_df_EP_LOB_GU['LOBId'].apply(lambda x: Decimal(x))
        final_df_EP_LOB_GU['Admin1Id'] = final_df_EP_LOB_GU['Admin1Id'].astype('int64')
        final_df_EP_LOB_GU['Admin1Id'] = final_df_EP_LOB_GU['Admin1Id'].apply(lambda x: Decimal(x))
        


        # Define the schema to match the required Parquet file schema
        schema = pa.schema([
            pa.field('EPType', pa.string(), nullable=True),
            pa.field('Loss', pa.float64(), nullable=True),
            pa.field('ReturnPeriod', pa.float64(), nullable=True),
            pa.field('Admin1Id',pa.int64(), nullable=True),
            pa.field('Admin1Name', pa.string(), nullable=True),
            pa.field('LOBName', pa.string(), nullable=True),
            pa.field('LOBId', pa.decimal128(38, 0), nullable=True),
        ])

        # Convert DataFrame to Arrow Table with the specified schema
        table = pa.Table.from_pandas(final_df_EP_LOB_GU, schema=schema)

        underscore_count = parquet_file_path.count('_')

        export_path =os.path.join(main_folder_path,'EP','Admin1_Lob',Cat)
        parquet_file_path = os.path.join(export_path, f"{os.path.splitext(parquet_file_path)[0]}_{idx}.parquet")

        # If there are 9 or more underscores, modify the file path
        if underscore_count >= hiphen_count: # anything wrong replace hiphen count with 9
            parts = parquet_file_path.split('_')
            # Remove the second last part which contains the number and the underscore before it
            parts = parts[:-2] + parts[-1:]
            parquet_file_path = '_'.join(parts)

        # Write the table to the parquet file
        pq.write_table(table, parquet_file_path)

        print(f"Parquet file saved successfully at {parquet_file_path}")
    
    



rps_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]



#GU
for i, (lobname, lobid) in enumerate(lobname_to_lobid.items()):
    parquet_file_path = f'{proname}_{region}_EP_Admin1_Lob_GU_{i}.parquet'
    try:
        process_parquet_files_2(parquet_files_grp, lobname, lobid, speriod, samples, rps_values, parquet_file_path, "GU")
    except (NameError, AttributeError, ValueError) as e:
        print(f"Error processing {lobname}: {e}")
        pass


#for GR
    
for i, (lobname, lobid) in enumerate(lobname_to_lobid.items()):
    parquet_file_path = f'{proname}_{region}_EP_Admin1_Lob_GR_{i}.parquet'
    try:
        process_parquet_files_2(parquet_files_grp_gr, lobname, lobid, speriod, samples, rps_values, parquet_file_path, "GR")
    except (NameError, AttributeError, ValueError) as e:
        print(f"Error processing {lobname}: {e}")
        pass


partial_folder_path = os.path.join(processing_folder_path, 'partial')
concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')


# In[77]:


def process_parquet_files_Port_2(parquet_files, speriod, samples, rps_values,Cat):
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')

    os.makedirs(partial_folder_path, exist_ok=True)
    os.makedirs(concatenated_folder_path, exist_ok=True)

    # Initialize an empty list to store the results
    final_grouped_table_1 = []

    # Process each Parquet file individually
    for file in parquet_files:
        # Read the Parquet file into a PyArrow Table
        table = pq.read_table(file)

        grouped_table_1 = table.group_by(['EventId', 'PeriodId', 'EventDate','Admin1Name','Admin1Id']).aggregate([('Loss', 'sum')])
        grouped_table_1 = grouped_table_1.rename_columns(['EventId', 'PeriodId', 'EventDate','Admin1Name','Admin1Id','Sum_Loss'])
    
        # Write intermediate results to disk
        pq.write_table(grouped_table_1, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))

    # Read all intermediate files and concatenate them
    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]

    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]

    final_table_1 = pa.concat_tables(final_grouped_table_1)

    # Perform final grouping and sorting
    f_grouped_table_1 = final_table_1.group_by(['EventId', 'PeriodId', 'EventDate', 'Admin1Name', 'Admin1Id']).aggregate([('Sum_Loss', 'sum')])
    sorted_final_table_1 = f_grouped_table_1.sort_by([('Sum_Loss_sum', 'descending')])
    pq.write_table(sorted_final_table_1, os.path.join(concatenated_folder_path, 'final_grouped_table_1.parquet'))
     # Delete all non-concatenated files
    for f in intermediate_files_1:
        os.remove(f)

    # Get distinct Admin1Id and Admin1Name
    distinct_admins = sorted_final_table_1.select(['Admin1Id', 'Admin1Name']).to_pandas().drop_duplicates()
    distinct_admins = distinct_admins.reset_index(drop=True)


    for idx, row in distinct_admins.iterrows():
        admin1_id = row['Admin1Id']
        admin1_name = row['Admin1Name']

        # Filter the table for the current Admin1Id and Admin1Name
        filtered_table = sorted_final_table_1.filter(pa.compute.equal(sorted_final_table_1['Admin1Id'], admin1_id))

        # Convert to pandas DataFrame
        dataframe_1 = filtered_table.to_pandas()
        dataframe_2 = dataframe_1.groupby(['PeriodId','Admin1Name','Admin1Id'], as_index=False).agg({'Sum_Loss_sum': 'max'})
        dataframe_2.rename(columns={'Sum_Loss_sum': 'Max_Loss'}, inplace=True)
        dataframe_2 = dataframe_2.sort_values(by='Max_Loss', ascending=False).reset_index(drop=True)

        dataframe_2['rate'] = (1 / (speriod * samples))
        dataframe_2['cumrate'] = dataframe_2['rate'].cumsum().round(6)
        dataframe_2['RPs'] = (1 / dataframe_2['cumrate'])

        # Given RP values list
        rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

        # Find the minimum RP in the dataframe
        min_rp = dataframe_2['RPs'].min()

        # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
        if min_rp > 2:
            # Find the RP values that need to be added
            missing_rps = [rp for rp in rp_values if rp < min_rp]

            new_rows = []
            for rp in missing_rps:
                temp_df = dataframe_2.iloc[[-1]].copy()  # Copy the last row as a template
                temp_df['RPs'] = rp
                new_rows.append(temp_df)

            # Concatenate the original dataframe with new rows
            new_rows_df = pd.concat(new_rows, ignore_index=True)
            dataframe_2 = pd.concat([dataframe_2, new_rows_df], ignore_index=True)

            # Fill NaN values only in the new rows
            new_rows_indices = new_rows_df.index
            dataframe_2.loc[new_rows_indices] = dataframe_2.loc[new_rows_indices].fillna(0)
        dataframe_2['TCE_OEP_1'] = ((dataframe_2['Max_Loss'] - dataframe_2['Max_Loss'].shift(-1)) * (dataframe_2['cumrate'] + dataframe_2['cumrate'].shift(-1)) * 0.5)
        dataframe_2['TCE_OEP_2'] = (dataframe_2['TCE_OEP_1'].shift().cumsum() * dataframe_2['RPs'])
        dataframe_2['TCE_OEP_Final'] = (dataframe_2['TCE_OEP_2'] + dataframe_2['Max_Loss'])

        dataframe_3 = dataframe_1.groupby(['PeriodId','Admin1Name','Admin1Id'], as_index=False).agg({'Sum_Loss_sum': 'sum'})
        dataframe_3.rename(columns={'Sum_Loss_sum': 'S_Sum_Loss'}, inplace=True)
        dataframe_3 = dataframe_3.sort_values(by='S_Sum_Loss', ascending=False).reset_index(drop=True)

        dataframe_3['rate'] = (1 / (speriod * samples))
        dataframe_3['cumrate'] = dataframe_3['rate'].cumsum().round(6)
        dataframe_3['RPs'] = (1 / dataframe_3['cumrate'])


        # Given RP values list
        rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

        # Find the minimum RP in the dataframe
        min_rp = dataframe_3['RPs'].min()

        # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
        if min_rp > 2:
            # Find the RP values that need to be added
            missing_rps = [rp for rp in rp_values if rp < min_rp]

            new_rows = []
            for rp in missing_rps:
                temp_df = dataframe_3.iloc[[-1]].copy()  # Copy the last row
                temp_df['RPs'] = rp
                new_rows.append(temp_df)

            # Concatenate the original dataframe with new rows
            new_rows_df = pd.concat(new_rows, ignore_index=True)
            dataframe_3 = pd.concat([dataframe_3, new_rows_df], ignore_index=True)

            # Fill NaN values only in the new rows
            new_rows_indices = new_rows_df.index
            dataframe_3.loc[new_rows_indices] = dataframe_3.loc[new_rows_indices].fillna(0)
        dataframe_3['TCE_AEP_1'] = ((dataframe_3['S_Sum_Loss'] - dataframe_3['S_Sum_Loss'].shift(-1)) * (dataframe_3['cumrate'] + dataframe_3['cumrate'].shift(-1)) * 0.5)
        dataframe_3['TCE_AEP_2'] = (dataframe_3['TCE_AEP_1'].shift().cumsum() * dataframe_3['RPs'])
        dataframe_3['TCE_AEP_Final'] = (dataframe_3['TCE_AEP_2'] + dataframe_3['S_Sum_Loss'])

        fdataframe_2 = pd.DataFrame()
        fdataframe_3 = pd.DataFrame()

        for value in rps_values:
            closest_index_2 = (dataframe_2['RPs'] - value).abs().sort_values().index[0]
            fdataframe_2 = pd.concat([fdataframe_2, dataframe_2.loc[[closest_index_2]]])
            fdataframe_3 = pd.concat([fdataframe_3, dataframe_3.loc[[closest_index_2]]])
                
            # Update the closest value to match the rp value exactly
            fdataframe_2.at[closest_index_2, 'RPs'] = float(value)
            fdataframe_3.at[closest_index_2, 'RPs'] = float(value)

        fdataframe_2.rename(columns={'Max_Loss': 'OEP', 'TCE_OEP_Final': 'TCE-OEP'}, inplace=True)
        columns_to_keep_2 = ['RPs','Admin1Name','Admin1Id']
        columns_to_melt_2 = ['OEP', 'TCE-OEP']
        melted_df_2 = fdataframe_2.melt(id_vars=columns_to_keep_2, value_vars=columns_to_melt_2, var_name='EPType', value_name='Loss')
        melted_df_2.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
        final_df_2 = melted_df_2[['EPType', 'Loss', 'ReturnPeriod','Admin1Name','Admin1Id']]

        fdataframe_3.rename(columns={'S_Sum_Loss': 'AEP', 'TCE_AEP_Final': 'TCE-AEP'}, inplace=True)
        columns_to_keep_3 = ['RPs','Admin1Name','Admin1Id']
        columns_to_melt_3 = ['AEP', 'TCE-AEP']
        melted_df_3 = fdataframe_3.melt(id_vars=columns_to_keep_3, value_vars=columns_to_melt_3, var_name='EPType', value_name='Loss')
        melted_df_3.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
        final_df_3 = melted_df_3[['EPType', 'Loss', 'ReturnPeriod','Admin1Name','Admin1Id']]

        final_df_EP_Portfolio_GU = pd.concat([final_df_2, final_df_3], ignore_index=True)
        new_ep_type_order = ["OEP", "AEP", "TCE-OEP", "TCE-AEP"]
        final_df_EP_Portfolio_GU['EPType'] = pd.Categorical(final_df_EP_Portfolio_GU['EPType'], categories=new_ep_type_order, ordered=True)
        final_df_EP_Portfolio_GU = final_df_EP_Portfolio_GU.sort_values(by=['EPType', 'ReturnPeriod'], ascending=[True, False]).reset_index(drop=True)
        final_df_EP_Portfolio_GU['Admin1Id'] = final_df_EP_Portfolio_GU['Admin1Id'].astype('int64')
        final_df_EP_Portfolio_GU['Admin1Id'] = final_df_EP_Portfolio_GU['Admin1Id'].apply(lambda x: Decimal(x))

        # Define the schema to match the required Parquet file schema
        schema = pa.schema([
            pa.field('EPType', pa.string(), nullable=True),
            pa.field('Loss', pa.float64(), nullable=True),
            pa.field('ReturnPeriod', pa.float64(), nullable=True),
            pa.field('Admin1Id', pa.decimal128(38, 0), nullable=True),
            pa.field('Admin1Name', pa.string(), nullable=True),
        ])

        # Convert DataFrame to Arrow Table with the specified schema
        table = pa.Table.from_pandas(final_df_EP_Portfolio_GU, schema=schema)
        #FOR GU

        export_path =os.path.join(main_folder_path,'EP','Admin1',Cat)
        parquet_file_path = os.path.join(export_path,f'{proname}_{region}_EP_Admin1_{Cat}_{idx}.parquet')
        pq.write_table(table, parquet_file_path)
        print(f"Parquet file saved successfully at {parquet_file_path}")

try:
    process_parquet_files_Port_2(parquet_files_grp, speriod, samples, rps_values,"GU")
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing : {e}")
    pass

#FOR GR
try:
    process_parquet_files_Port_2(parquet_files_grp_gr, speriod, samples, rps_values,"GR")
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing : {e}")
    pass


# In[48]:


partial_folder_path = os.path.join(processing_folder_path, 'partial')
concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')

delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)
gc.collect()



# In[88]:


#EP_ cresta Lob updated below


# In[129]:


def process_parquet_files_EP_Cresta_lob_2(parquet_files, filter_string, lob_id, speriod, samples, rps_values,parquet_file_path,Cat):
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')
    os.makedirs(partial_folder_path, exist_ok=True)
    os.makedirs(concatenated_folder_path, exist_ok=True)

    # Initialize an empty list to store the results
    final_grouped_table_1 = []

    # Process each Parquet file individually
    for file in parquet_files:
        # Read the Parquet file into a PyArrow Table
        table = pq.read_table(file)
        
        # Filter the table based on the filter_string
        table = table.filter(pc.equal(table['LobName'], filter_string))
        
        # Skip if the filtered table is empty
        
        grouped_table_1 = table.group_by(['EventId', 'PeriodId', 'EventDate','CrestaName','CrestaId']).aggregate([('Loss', 'sum')])
        grouped_table_1 = grouped_table_1.rename_columns(['EventId', 'PeriodId', 'EventDate','CrestaName','CrestaId','Sum_Loss'])
    
        # Write intermediate results to disk
        pq.write_table(grouped_table_1, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))

    # Read all intermediate files and concatenate them
    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]

    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]

    final_table_1 = pa.concat_tables(final_grouped_table_1)

    # Perform final grouping and sorting
    f_grouped_table_1 = final_table_1.group_by(['EventId', 'PeriodId', 'EventDate','CrestaName','CrestaId']).aggregate([('Sum_Loss', 'sum')])
    sorted_final_table_1 = f_grouped_table_1.sort_by([('Sum_Loss_sum', 'descending')])

    # Get distinct Admin1Id and Admin1Name
    distinct_admins = sorted_final_table_1.select(['CrestaName','CrestaId']).to_pandas().drop_duplicates()
    distinct_admins = distinct_admins.reset_index(drop=True)
    pq.write_table(sorted_final_table_1, os.path.join(concatenated_folder_path, 'final_grouped_table_1.parquet'))
     # Delete all non-concatenated files
    for f in intermediate_files_1:
        os.remove(f)
    
    for idx, row in distinct_admins.iterrows():
        admin1_id = row['CrestaId']
        admin1_name = row['CrestaName']
        # Filter the table for the current Admin1Id and Admin1Name
        filtered_table = sorted_final_table_1.filter(pa.compute.equal(sorted_final_table_1['CrestaId'], admin1_id))
        dataframe_1 = filtered_table.to_pandas()

        dataframe_2 = dataframe_1.groupby(['PeriodId','CrestaName','CrestaId'], as_index=False).agg({'Sum_Loss_sum': 'max'})
        dataframe_2.rename(columns={'Sum_Loss_sum': 'Max_Loss'}, inplace=True)
        dataframe_2 = dataframe_2.sort_values(by='Max_Loss', ascending=False).reset_index(drop=True)

        dataframe_2['rate'] = (1 / (speriod * samples))
        dataframe_2['cumrate'] = dataframe_2['rate'].cumsum().round(6)
        dataframe_2['RPs'] = (1 / dataframe_2['cumrate'])

        # Given RP values list
        rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

        # Find the minimum RP in the dataframe
        min_rp = dataframe_2['RPs'].min()

        # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
        if min_rp > 2:
            # Find the RP values that need to be added
            missing_rps = [rp for rp in rp_values if rp < min_rp]

            new_rows = []
            for rp in missing_rps:
                temp_df = dataframe_2.iloc[[-1]].copy()  # Copy the last row as a template
                temp_df['RPs'] = rp
                new_rows.append(temp_df)

            # Concatenate the original dataframe with new rows
            new_rows_df = pd.concat(new_rows, ignore_index=True)
            dataframe_2 = pd.concat([dataframe_2, new_rows_df], ignore_index=True)

            # Fill NaN values only in the new rows
            new_rows_indices = new_rows_df.index
            dataframe_2.loc[new_rows_indices] = dataframe_2.loc[new_rows_indices].fillna(0)        
        dataframe_2['TCE_OEP_1'] = ((dataframe_2['Max_Loss'] - dataframe_2['Max_Loss'].shift(-1)) * (dataframe_2['cumrate'] + dataframe_2['cumrate'].shift(-1)) * 0.5)
        dataframe_2['TCE_OEP_2'] = (dataframe_2['TCE_OEP_1'].shift().cumsum() * dataframe_2['RPs'])
        dataframe_2['TCE_OEP_Final'] = (dataframe_2['TCE_OEP_2'] + dataframe_2['Max_Loss'])

        dataframe_3 = dataframe_1.groupby(['PeriodId','CrestaName','CrestaId'], as_index=False).agg({'Sum_Loss_sum': 'sum'})
        dataframe_3.rename(columns={'Sum_Loss_sum': 'S_Sum_Loss'}, inplace=True)
        dataframe_3 = dataframe_3.sort_values(by='S_Sum_Loss', ascending=False).reset_index(drop=True)

        dataframe_3['rate'] = (1 / (speriod * samples))
        dataframe_3['cumrate'] = dataframe_3['rate'].cumsum().round(6)
        dataframe_3['RPs'] = (1 / dataframe_3['cumrate'])


        # Given RP values list
        rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

        # Find the minimum RP in the dataframe
        min_rp = dataframe_3['RPs'].min()

        # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
        if min_rp > 2:
            # Find the RP values that need to be added
            missing_rps = [rp for rp in rp_values if rp < min_rp]

            new_rows = []
            for rp in missing_rps:
                temp_df = dataframe_3.iloc[[-1]].copy()  # Copy the last row
                temp_df['RPs'] = rp
                new_rows.append(temp_df)

            # Concatenate the original dataframe with new rows
            new_rows_df = pd.concat(new_rows, ignore_index=True)
            dataframe_3 = pd.concat([dataframe_3, new_rows_df], ignore_index=True)

            # Fill NaN values only in the new rows
            new_rows_indices = new_rows_df.index
            dataframe_3.loc[new_rows_indices] = dataframe_3.loc[new_rows_indices].fillna(0)
        dataframe_3['TCE_AEP_1'] = ((dataframe_3['S_Sum_Loss'] - dataframe_3['S_Sum_Loss'].shift(-1)) * (dataframe_3['cumrate'] + dataframe_3['cumrate'].shift(-1)) * 0.5)
        dataframe_3['TCE_AEP_2'] = (dataframe_3['TCE_AEP_1'].shift().cumsum() * dataframe_3['RPs'])
        dataframe_3['TCE_AEP_Final'] = (dataframe_3['TCE_AEP_2'] + dataframe_3['S_Sum_Loss'])

        fdataframe_2 = pd.DataFrame()
        fdataframe_3 = pd.DataFrame()

        for value in rps_values:
                                
                closest_index_2 = (dataframe_2['RPs'] - value).abs().idxmin()
                
                # Assign the closest value to the new DataFrames
                fdataframe_2 = pd.concat([fdataframe_2, dataframe_2.loc[[closest_index_2]]])
                fdataframe_3 = pd.concat([fdataframe_3, dataframe_3.loc[[closest_index_2]]])
                
                # Update the closest value to match the rp value exactly
                fdataframe_2.at[closest_index_2, 'RPs'] = float(value)
                fdataframe_3.at[closest_index_2, 'RPs'] = float(value)

        fdataframe_2.rename(columns={'Max_Loss': 'OEP', 'TCE_OEP_Final': 'TCE-OEP'}, inplace=True)
        columns_to_keep_2 = ['RPs', 'CrestaName','CrestaId']
        columns_to_melt_2 = ['OEP', 'TCE-OEP']
        melted_df_2 = fdataframe_2.melt(id_vars=columns_to_keep_2, value_vars=columns_to_melt_2, var_name='EPType', value_name='Loss')
        melted_df_2.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
        final_df_2 = melted_df_2[['EPType', 'Loss', 'ReturnPeriod', 'CrestaName','CrestaId']]

        fdataframe_3.rename(columns={'S_Sum_Loss': 'AEP', 'TCE_AEP_Final': 'TCE-AEP'}, inplace=True)
        columns_to_keep_3 = ['RPs', 'CrestaName','CrestaId']
        columns_to_melt_3 = ['AEP', 'TCE-AEP']
        melted_df_3 = fdataframe_3.melt(id_vars=columns_to_keep_3, value_vars=columns_to_melt_3, var_name='EPType', value_name='Loss')
        melted_df_3.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
        final_df_3 = melted_df_3[['EPType', 'Loss', 'ReturnPeriod','CrestaName','CrestaId']]

        final_df_EP_LOB_GU = pd.concat([final_df_2, final_df_3], ignore_index=True)
        new_ep_type_order = ["OEP", "AEP", "TCE-OEP", "TCE-AEP"]
        final_df_EP_LOB_GU['EPType'] = pd.Categorical(final_df_EP_LOB_GU['EPType'], categories=new_ep_type_order, ordered=True)
        final_df_EP_LOB_GU = final_df_EP_LOB_GU.sort_values(by=['EPType', 'ReturnPeriod'], ascending=[True, False]).reset_index(drop=True)

        # Add LobID and LobName columns
        final_df_EP_LOB_GU['LOBId'] = lob_id
        final_df_EP_LOB_GU['LOBName'] = filter_string
        final_df_EP_LOB_GU['LOBId'] = final_df_EP_LOB_GU['LOBId'].apply(lambda x: Decimal(x))
        final_df_EP_LOB_GU['CrestaId'] = final_df_EP_LOB_GU['CrestaId'].astype('int64')
        final_df_EP_LOB_GU['CrestaId'] = final_df_EP_LOB_GU['CrestaId'].apply(lambda x: Decimal(x))
        


        # Define the schema to match the required Parquet file schema
        schema = pa.schema([
            pa.field('EPType', pa.string(), nullable=True),
            pa.field('Loss', pa.float64(), nullable=True),
            pa.field('ReturnPeriod', pa.float64(), nullable=True),
            pa.field('CrestaId',pa.int64(), nullable=True),
            pa.field('CrestaName', pa.string(), nullable=True),
            pa.field('LOBName', pa.string(), nullable=True),
            pa.field('LOBId', pa.decimal128(38, 0), nullable=True),
        ])

        # Convert DataFrame to Arrow Table with the specified schema
        table = pa.Table.from_pandas(final_df_EP_LOB_GU, schema=schema)

        underscore_count = parquet_file_path.count('_')

        export_path =os.path.join(main_folder_path,'EP','Cresta_Lob',Cat)
        parquet_file_path = os.path.join(export_path, f"{os.path.splitext(parquet_file_path)[0]}_{idx}.parquet")

        # If there are 21 or more underscores, modify the file path
        if underscore_count >= hiphen_count: # anything wrong replace hiphen count with 9
            parts = parquet_file_path.split('_')
            # Remove the second last part which contains the number and the underscore before it
            parts = parts[:-2] + parts[-1:]
            parquet_file_path = '_'.join(parts)

        # Write the table to the parquet file
        pq.write_table(table, parquet_file_path)

        print(f"Parquet file saved successfully at {parquet_file_path}")
    
    



rps_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]



#GU
for i, (lobname, lobid) in enumerate(lobname_to_lobid.items()):
    parquet_file_path = f'{proname}_{region}_EP_Cresta_Lob_GU_{i}.parquet'
    try:
        process_parquet_files_EP_Cresta_lob_2(parquet_files_grp, lobname, lobid, speriod, samples, rps_values, parquet_file_path, "GU")
    except (NameError, AttributeError, ValueError) as e:
        print(f"Error processing {lobname}: {e}")
        pass


#for GR
    
for i, (lobname, lobid) in enumerate(lobname_to_lobid.items()):
    parquet_file_path = f'{proname}_{region}_EP_Cresta_Lob_GR_{i}.parquet'
    try:
        process_parquet_files_EP_Cresta_lob_2(parquet_files_grp_gr, lobname, lobid, speriod, samples, rps_values, parquet_file_path, "GR")
    except (NameError, AttributeError, ValueError) as e:
        print(f"Error processing {lobname}: {e}")
        pass


partial_folder_path = os.path.join(processing_folder_path, 'partial')
concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')


# In[53]:


delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)
flush_cache()



# In[89]:


#for cresta portfolio

def process_parquet_files_Port_EP_Cresta_2(parquet_files, speriod, samples, rps_values,Cat):
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')

    os.makedirs(partial_folder_path, exist_ok=True)
    os.makedirs(concatenated_folder_path, exist_ok=True)

    # Initialize an empty list to store the results
    final_grouped_table_1 = []

    # Process each Parquet file individually
    for file in parquet_files:
        # Read the Parquet file into a PyArrow Table
        table = pq.read_table(file)

        grouped_table_1 = table.group_by(['EventId', 'PeriodId', 'EventDate','CrestaName','CrestaId']).aggregate([('Loss', 'sum')])
        grouped_table_1 = grouped_table_1.rename_columns(['EventId', 'PeriodId', 'EventDate','CrestaName','CrestaId','Sum_Loss'])
    
        # Write intermediate results to disk
        pq.write_table(grouped_table_1, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))

    # Read all intermediate files and concatenate them
    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]

    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]

    final_table_1 = pa.concat_tables(final_grouped_table_1)

    # Perform final grouping and sorting
    f_grouped_table_1 = final_table_1.group_by(['EventId', 'PeriodId', 'EventDate', 'CrestaName','CrestaId']).aggregate([('Sum_Loss', 'sum')])
    sorted_final_table_1 = f_grouped_table_1.sort_by([('Sum_Loss_sum', 'descending')])
    pq.write_table(sorted_final_table_1, os.path.join(concatenated_folder_path, 'final_grouped_table_1.parquet'))
     # Delete all non-concatenated files
    for f in intermediate_files_1:
        os.remove(f)

    # Get distinct Admin1Id and Admin1Name
    distinct_admins = sorted_final_table_1.select(['CrestaName','CrestaId']).to_pandas().drop_duplicates()
    distinct_admins = distinct_admins.reset_index(drop=True)


    for idx, row in distinct_admins.iterrows():
        admin1_id = row['CrestaId']
        admin1_name = row['CrestaName']

        # Filter the table for the current Admin1Id and Admin1Name
        filtered_table = sorted_final_table_1.filter(pa.compute.equal(sorted_final_table_1['CrestaId'], admin1_id))

        # Convert to pandas DataFrame
        dataframe_1 = filtered_table.to_pandas()
        dataframe_2 = dataframe_1.groupby(['PeriodId','CrestaName','CrestaId'], as_index=False).agg({'Sum_Loss_sum': 'max'})
        dataframe_2.rename(columns={'Sum_Loss_sum': 'Max_Loss'}, inplace=True)
        dataframe_2 = dataframe_2.sort_values(by='Max_Loss', ascending=False).reset_index(drop=True)

        dataframe_2['rate'] = (1 / (speriod * samples))
        dataframe_2['cumrate'] = dataframe_2['rate'].cumsum().round(6)
        dataframe_2['RPs'] = (1 / dataframe_2['cumrate'])

        # Given RP values list
        rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

        # Find the minimum RP in the dataframe
        min_rp = dataframe_2['RPs'].min()

        # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
        if min_rp > 2:
            # Find the RP values that need to be added
            missing_rps = [rp for rp in rp_values if rp < min_rp]

            new_rows = []
            for rp in missing_rps:
                temp_df = dataframe_2.iloc[[-1]].copy()  # Copy the last row as a template
                temp_df['RPs'] = rp
                new_rows.append(temp_df)

            # Concatenate the original dataframe with new rows
            new_rows_df = pd.concat(new_rows, ignore_index=True)
            dataframe_2 = pd.concat([dataframe_2, new_rows_df], ignore_index=True)

            # Fill NaN values only in the new rows
            new_rows_indices = new_rows_df.index
            dataframe_2.loc[new_rows_indices] = dataframe_2.loc[new_rows_indices].fillna(0)

        dataframe_2['TCE_OEP_1'] = ((dataframe_2['Max_Loss'] - dataframe_2['Max_Loss'].shift(-1)) * (dataframe_2['cumrate'] + dataframe_2['cumrate'].shift(-1)) * 0.5)
        dataframe_2['TCE_OEP_2'] = (dataframe_2['TCE_OEP_1'].shift().cumsum() * dataframe_2['RPs'])
        dataframe_2['TCE_OEP_Final'] = (dataframe_2['TCE_OEP_2'] + dataframe_2['Max_Loss'])

        dataframe_3 = dataframe_1.groupby(['PeriodId','CrestaName','CrestaId'], as_index=False).agg({'Sum_Loss_sum': 'sum'})
        dataframe_3.rename(columns={'Sum_Loss_sum': 'S_Sum_Loss'}, inplace=True)
        dataframe_3 = dataframe_3.sort_values(by='S_Sum_Loss', ascending=False).reset_index(drop=True)

        dataframe_3['rate'] = (1 / (speriod * samples))
        dataframe_3['cumrate'] = dataframe_3['rate'].cumsum().round(6)
        dataframe_3['RPs'] = (1 / dataframe_3['cumrate'])


        # Given RP values list
        rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

        # Find the minimum RP in the dataframe
        min_rp = dataframe_3['RPs'].min()

        # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
        if min_rp > 2:
            # Find the RP values that need to be added
            missing_rps = [rp for rp in rp_values if rp < min_rp]

            new_rows = []
            for rp in missing_rps:
                temp_df = dataframe_3.iloc[[-1]].copy()  # Copy the last row
                temp_df['RPs'] = rp
                new_rows.append(temp_df)

            # Concatenate the original dataframe with new rows
            new_rows_df = pd.concat(new_rows, ignore_index=True)
            dataframe_3 = pd.concat([dataframe_3, new_rows_df], ignore_index=True)

            # Fill NaN values only in the new rows
            new_rows_indices = new_rows_df.index
            dataframe_3.loc[new_rows_indices] = dataframe_3.loc[new_rows_indices].fillna(0)

        dataframe_3['TCE_AEP_1'] = ((dataframe_3['S_Sum_Loss'] - dataframe_3['S_Sum_Loss'].shift(-1)) * (dataframe_3['cumrate'] + dataframe_3['cumrate'].shift(-1)) * 0.5)
        dataframe_3['TCE_AEP_2'] = (dataframe_3['TCE_AEP_1'].shift().cumsum() * dataframe_3['RPs'])
        dataframe_3['TCE_AEP_Final'] = (dataframe_3['TCE_AEP_2'] + dataframe_3['S_Sum_Loss'])

        fdataframe_2 = pd.DataFrame()
        fdataframe_3 = pd.DataFrame()

        for value in rps_values:
            closest_index_2 = (dataframe_2['RPs'] - value).abs().idxmin()
            fdataframe_2 = pd.concat([fdataframe_2, dataframe_2.loc[[closest_index_2]]])
            fdataframe_3 = pd.concat([fdataframe_3, dataframe_3.loc[[closest_index_2]]])
            # Update the closest value to match the rp value exactly
            fdataframe_2.at[closest_index_2, 'RPs'] = float(value)
            fdataframe_3.at[closest_index_2, 'RPs'] = float(value)

        fdataframe_2.rename(columns={'Max_Loss': 'OEP', 'TCE_OEP_Final': 'TCE-OEP'}, inplace=True)
        columns_to_keep_2 = ['RPs','CrestaName','CrestaId']
        columns_to_melt_2 = ['OEP', 'TCE-OEP']
        melted_df_2 = fdataframe_2.melt(id_vars=columns_to_keep_2, value_vars=columns_to_melt_2, var_name='EPType', value_name='Loss')
        melted_df_2.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
        final_df_2 = melted_df_2[['EPType', 'Loss', 'ReturnPeriod','CrestaName','CrestaId']]

        fdataframe_3.rename(columns={'S_Sum_Loss': 'AEP', 'TCE_AEP_Final': 'TCE-AEP'}, inplace=True)
        columns_to_keep_3 = ['RPs','CrestaName','CrestaId']
        columns_to_melt_3 = ['AEP', 'TCE-AEP']
        melted_df_3 = fdataframe_3.melt(id_vars=columns_to_keep_3, value_vars=columns_to_melt_3, var_name='EPType', value_name='Loss')
        melted_df_3.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
        final_df_3 = melted_df_3[['EPType', 'Loss', 'ReturnPeriod','CrestaName','CrestaId']]

        final_df_EP_Portfolio_GU = pd.concat([final_df_2, final_df_3], ignore_index=True)
        new_ep_type_order = ["OEP", "AEP", "TCE-OEP", "TCE-AEP"]
        final_df_EP_Portfolio_GU['EPType'] = pd.Categorical(final_df_EP_Portfolio_GU['EPType'], categories=new_ep_type_order, ordered=True)
        final_df_EP_Portfolio_GU = final_df_EP_Portfolio_GU.sort_values(by=['EPType', 'ReturnPeriod'], ascending=[True, False]).reset_index(drop=True)
        final_df_EP_Portfolio_GU['CrestaId'] = final_df_EP_Portfolio_GU['CrestaId'].astype('int64')
        final_df_EP_Portfolio_GU['CrestaId'] = final_df_EP_Portfolio_GU['CrestaId'].apply(lambda x: Decimal(x))

        # Define the schema to match the required Parquet file schema
        schema = pa.schema([
            pa.field('EPType', pa.string(), nullable=True),
            pa.field('Loss', pa.float64(), nullable=True),
            pa.field('ReturnPeriod', pa.float64(), nullable=True),
            pa.field('CrestaId', pa.decimal128(38, 0), nullable=True),
            pa.field('CrestaName', pa.string(), nullable=True),
        ])

        # Convert DataFrame to Arrow Table with the specified schema
        table = pa.Table.from_pandas(final_df_EP_Portfolio_GU, schema=schema)
        #FOR GU

        export_path =os.path.join(main_folder_path,'EP','Cresta',Cat)
        parquet_file_path = os.path.join(export_path,f'{proname}_EP_Cresta_{Cat}_{idx}.parquet')
        pq.write_table(table, parquet_file_path)
        print(f"Parquet file saved successfully at {parquet_file_path}")

#FOR GU

try:
    process_parquet_files_Port_EP_Cresta_2(parquet_files_grp, speriod, samples, rps_values, "GU")
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing : {e}")
    pass




#FOR GR



try:
    process_parquet_files_Port_EP_Cresta_2(parquet_files_grp_gr, speriod, samples, rps_values,"GR")
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing : {e}")
    pass



# In[136]:


flush_cache()


# In[137]:


delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)


# In[90]:


#now for stats


# In[18]:


#now for stats LOB GU admin


def process_lob_stats_Admin1_Lob(parquet_files, parquet_file_path):
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')
    os.makedirs(partial_folder_path, exist_ok=True)
    os.makedirs(concatenated_folder_path, exist_ok=True)
    aggregated_tables_lob_stats = []

    

    # Process each Parquet file individually
    for file in parquet_files:
        # Check if the file exists
        if os.path.exists(file):
            # Read the Parquet file into a PyArrow Table
            table = pq.read_table(file)
            
            # Perform the aggregation: sum the Loss column grouped by LobName
            grouped = table.group_by(['LobName','Admin1Name','Admin1Id']).aggregate([('Loss', 'sum')])
            
            # Calculate AAL
            loss_sum = grouped.column('Loss_sum').to_numpy()
            aal = loss_sum / speriod / samples
            aal_array = pa.array(aal)
            grouped = grouped.append_column('AAL', aal_array)
            
            # Select only the necessary columns
            grouped = grouped.select(['LobName', 'AAL','Admin1Name','Admin1Id'])
            
            # Append the grouped Table to the list
            pq.write_table(grouped, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))
        else:
            print(f"File not found: {file}")

    
    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]
    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]
    final_table = pa.concat_tables(final_grouped_table_1)

    # Group the final Table again to ensure all groups are combined
    final_grouped = final_table.group_by(['LobName','Admin1Name','Admin1Id']).aggregate([('AAL', 'sum')])

    # Sort the final grouped Table by 'AAL' in descending order
    final_grouped = final_grouped.sort_by([('AAL_sum', 'descending')])
    pq.write_table(final_grouped, os.path.join(concatenated_folder_path, 'final_grouped_table_1.parquet'))
    for f in intermediate_files_1:
        os.remove(f)
    flush_cache()

    # Convert the final grouped Table to a Pandas DataFrame
    final_df = final_grouped.to_pandas()
    

    # Map LobName to LobId
    final_df['LobId'] = final_df['LobName'].map(lobname_to_lobid).apply(lambda x: Decimal(x))
    final_df['Admin1Id'] = final_df['Admin1Id'].astype('int64')
    final_df['Admin1Id'] = final_df['Admin1Id'].apply(lambda x: Decimal(x))


    final_df_STATS_Lob = final_df.rename(columns={'AAL_sum': 'AAL'})

    # Define the columns with NaN values for 'Std' and 'CV'
    final_df_STATS_Lob['Std'] = np.nan
    final_df_STATS_Lob['CV'] = np.nan

    # Reorder the columns to match the specified format
    final_df_STATS_Lob = final_df_STATS_Lob[['AAL', 'Std', 'CV', 'LobId', 'LobName','Admin1Name','Admin1Id']]
    final_df_STATS_Lob["LOBId"] = final_df_STATS_Lob["LobId"]
    final_df_STATS_Lob["LOBName"]=final_df_STATS_Lob["LobName"]

    # Define the desired schema
    desired_schema = pa.schema([
        pa.field('AAL', pa.float64()),
        pa.field('Std', pa.float64()),
        pa.field('CV', pa.float64()),
        pa.field('Admin1Id', pa.decimal128(38)),
        pa.field('Admin1Name', pa.string()),
        pa.field('LOBName', pa.string()),
        pa.field('LOBId', pa.decimal128(38)),

    ])

    # Convert the DataFrame back to a PyArrow Table with the desired schema
    final_table_STATS_Lob = pa.Table.from_pandas(final_df_STATS_Lob, schema=desired_schema)
    pq.write_table(final_table_STATS_Lob, parquet_file_path)
    print(f"Parquet file saved successfully at {parquet_file_path}")


#LOB GU STATS



parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Admin1_Lob', 'GU', f'{proname}_{region}_STATS_Admin1_Lob_GU_0.parquet')
process_lob_stats_Admin1_Lob(parquet_files_grp, parquet_file_path)


#LOB GR STATS


parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Admin1_Lob', 'GR', f'{proname}_{region}_STATS_Admin1_Lob_GR_0.parquet')
process_lob_stats_Admin1_Lob(parquet_files_grp_gr, parquet_file_path)


# In[21]:


delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)


# In[22]:


#stats admin1

def process_portfolio_stats_Admin1(parquet_files, export_path):
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    os.makedirs(partial_folder_path, exist_ok=True)

    # Process each Parquet file individually
    for file in parquet_files:
        # Read the Parquet file into a PyArrow Table
        table = pq.read_table(file)
        
        # Perform the aggregation: sum the Loss column grouped by LobName
        grouped = table.group_by(['Admin1Name','Admin1Id']).aggregate([('Loss', 'sum')])
        
        # Calculate AAL
        loss_sum = grouped.column('Loss_sum').to_numpy()
        aal = loss_sum / speriod / samples
        aal_array = pa.array(aal)
        grouped = grouped.append_column('AAL', aal_array)
        
        # Select only the necessary columns
        grouped = grouped.select([ 'AAL','Admin1Name','Admin1Id'])
        
        # Append the grouped Table to the list
        pq.write_table(grouped, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))
        
    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]
    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]
    final_table = pa.concat_tables(final_grouped_table_1)

    # Concatenate all the grouped Tables
    final_table=final_table.group_by(['Admin1Name','Admin1Id']).aggregate([('AAL', 'sum')])
    final_table=final_table.sort_by([('AAL_sum', 'descending')])
    final_table=final_table.rename_columns(['Admin1Name','Admin1Id','AAL'])

    # Convert the final table to a Pandas DataFrame
    final_df = final_table.to_pandas()
    final_df=final_df.sort_values(by='AAL')
    final_df['Admin1Id'] = final_df['Admin1Id'].astype('int64')
    final_df['Admin1Id'] = final_df['Admin1Id'].apply(lambda x: Decimal(x))
    final_df['Std']=np.nan
    final_df['CV']=np.nan
    # Get the exact values of Admin1Id and Admin1Name

    # Sum all the AAL values without grouping by LobName
    #total_aal = final_df['AAL'].sum()

    # Define the desired schema
    desired_schema = pa.schema([
        pa.field('AAL', pa.float64()),
        pa.field('Std', pa.float64()),
        pa.field('CV', pa.float64()),
        pa.field('Admin1Id', pa.decimal128(38)),
        pa.field('Admin1Name', pa.string()),
    ])

    # Convert the DataFrame back to a PyArrow Table with the desired schema
    final_table_STATS_Portfolio = pa.Table.from_pandas(final_df, schema=desired_schema)
    pq.write_table(final_table_STATS_Portfolio, export_path)
    print(f"Parquet file saved successfully at {export_path}")


#GU

parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Admin1', 'GU', f'{proname}_{region}_STATS_Admin1_GU_0.parquet')
process_portfolio_stats_Admin1(parquet_files_grp, parquet_file_path)

#GR.


flush_cache()
parquet_file_path_gr = os.path.join(main_folder_path, 'STATS', 'Admin1', 'GR', f'{proname}_{region}_STATS_Admin1_GR_0.parquet')
process_portfolio_stats_Admin1(parquet_files_grp_gr, parquet_file_path_gr)


# In[91]:


delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)
flush_cache()

# In[23]:


#now for stats LOB GU Cresta_Lob


def process_lob_stats_Cresta_Lob(parquet_files, parquet_file_path):
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    os.makedirs(partial_folder_path, exist_ok=True)
    aggregated_tables_lob_stats = []


    # Process each Parquet file individually
    for file in parquet_files:
        # Check if the file exists
        if os.path.exists(file):
            # Read the Parquet file into a PyArrow Table
            table = pq.read_table(file)
            
            # Perform the aggregation: sum the Loss column grouped by LobName
            grouped = table.group_by(['LobName','CrestaName','CrestaId']).aggregate([('Loss', 'sum')])
            
            # Calculate AAL
            loss_sum = grouped.column('Loss_sum').to_numpy()
            aal = loss_sum / speriod / samples
            aal_array = pa.array(aal)
            grouped = grouped.append_column('AAL', aal_array)
            
            # Select only the necessary columns
            grouped = grouped.select(['LobName', 'AAL','CrestaName','CrestaId'])
            
            # Append the grouped Table to the list
            pq.write_table(grouped, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))
        else:
            print(f"File not found: {file}")

    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]
    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]
    final_table = pa.concat_tables(final_grouped_table_1)
    # Group the final Table again to ensure all groups are combined
    final_grouped = final_table.group_by(['LobName','CrestaName','CrestaId']).aggregate([('AAL', 'sum')])

    # Sort the final grouped Table by 'AAL' in descending order
    final_grouped = final_grouped.sort_by([('AAL_sum', 'descending')])

    # Convert the final grouped Table to a Pandas DataFrame
    final_df = final_grouped.to_pandas()

    # Map LobName to LobId
    final_df['LobId'] = final_df['LobName'].map(lobname_to_lobid).apply(lambda x: Decimal(x))
    final_df['CrestaId'] = final_df['CrestaId'].astype('int64')
    final_df['CrestaId'] = final_df['CrestaId'].apply(lambda x: Decimal(x))


    final_df_STATS_Lob = final_df.rename(columns={'AAL_sum': 'AAL'})

    # Define the columns with NaN values for 'Std' and 'CV'
    final_df_STATS_Lob['Std'] = np.nan
    final_df_STATS_Lob['CV'] = np.nan

    # Reorder the columns to match the specified format
    final_df_STATS_Lob = final_df_STATS_Lob[['AAL', 'Std', 'CV', 'LobId', 'LobName','CrestaName','CrestaId']]
    final_df_STATS_Lob["LOBId"] = final_df_STATS_Lob["LobId"]
    final_df_STATS_Lob["LOBName"]=final_df_STATS_Lob["LobName"]

    # Define the desired schema
    desired_schema = pa.schema([
        pa.field('AAL', pa.float64()),
        pa.field('Std', pa.float64()),
        pa.field('CV', pa.float64()),
        pa.field('CrestaId', pa.decimal128(38)),
        pa.field('CrestaName', pa.string()),
        pa.field('LOBName', pa.string()),
        pa.field('LOBId', pa.decimal128(38)),
       
    ])

    # Convert the DataFrame back to a PyArrow Table with the desired schema
    final_table_STATS_Lob = pa.Table.from_pandas(final_df_STATS_Lob, schema=desired_schema)
    pq.write_table(final_table_STATS_Lob, parquet_file_path)
    print(f"Parquet file saved successfully at {parquet_file_path}")



# In[91]:


#LOB GU STATS


# In[92]:


parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Cresta_Lob', 'GU', f'{proname}_{region}_STATS_Cresta_Lob_GU_0.parquet')
process_lob_stats_Cresta_Lob(parquet_files_grp, parquet_file_path)




#LOB GR STATS




parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Cresta_Lob', 'GR', f'{proname}_{region}_STATS_Cresta_Lob_GR_0.parquet')
process_lob_stats_Cresta_Lob(parquet_files_grp_gr, parquet_file_path)


# In[ ]:


delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)


# In[24]:


def process_portfolio_stats_Cresta(parquet_files, export_path):
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    os.makedirs(partial_folder_path, exist_ok=True)

    # Process each Parquet file individually
    for file in parquet_files:
        # Read the Parquet file into a PyArrow Table
        table = pq.read_table(file)
        
        # Perform the aggregation: sum the Loss column grouped by LobName
        grouped = table.group_by(['CrestaName','CrestaId']).aggregate([('Loss', 'sum')])
        
        # Calculate AAL
        loss_sum = grouped.column('Loss_sum').to_numpy()
        aal = loss_sum / speriod / samples
        aal_array = pa.array(aal)
        grouped = grouped.append_column('AAL', aal_array)
        
        # Select only the necessary columns
        grouped = grouped.select([ 'AAL','CrestaName','CrestaId'])
        
        # Append the grouped Table to the list
        pq.write_table(grouped, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))

    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]
    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]
    final_table = pa.concat_tables(final_grouped_table_1)
    final_table=final_table.group_by(['CrestaName','CrestaId']).aggregate([('AAL', 'sum')])
    final_table=final_table.sort_by([('AAL_sum', 'descending')])
    final_table=final_table.rename_columns(['CrestaName','CrestaId','AAL'])

    # Convert the final table to a Pandas DataFrame
    final_df = final_table.to_pandas()
    final_df=final_df.sort_values(by='AAL')
    final_df['CrestaId'] = final_df['CrestaId'].astype('int64')
    final_df['CrestaId'] = final_df['CrestaId'].apply(lambda x: Decimal(x))
    final_df['Std']=np.nan
    final_df['CV']=np.nan
    # Get the exact values of Admin1Id and Admin1Name

    # Sum all the AAL values without grouping by LobName
    #total_aal = final_df['AAL'].sum()

    # Define the desired schema
    desired_schema = pa.schema([
        pa.field('AAL', pa.float64()),
        pa.field('Std', pa.float64()),
        pa.field('CV', pa.float64()),
        pa.field('CrestaId', pa.decimal128(38)),
        pa.field('CrestaName', pa.string()),
    ])

    # Convert the DataFrame back to a PyArrow Table with the desired schema
    final_table_STATS_Portfolio = pa.Table.from_pandas(final_df, schema=desired_schema)
    pq.write_table(final_table_STATS_Portfolio, export_path)
    print(f"Parquet file saved successfully at {export_path}")


#GU

parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Cresta', 'GU', f'{proname}_{region}_STATS_Cresta_GU_0.parquet')
process_portfolio_stats_Cresta(parquet_files_grp, parquet_file_path)

#GR.

parquet_file_path_gr = os.path.join(main_folder_path, 'STATS', 'Cresta', 'GR', f'{proname}_{region}_STATS_Cresta_GR_0.parquet')
process_portfolio_stats_Cresta(parquet_files_grp_gr, parquet_file_path_gr)




# In[ ]:


delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)


# In[93]:


flush_cache()
def process_parquet_files_EP_lob(parquet_files, export_path, filter_string, lob_id, speriod, samples, rps_values,parquet_file_path):
    processing_folder_path = os.path.join(main_folder_path, 'processing')
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')

    os.makedirs(processing_folder_path, exist_ok=True)
    os.makedirs(partial_folder_path, exist_ok=True)
    os.makedirs(concatenated_folder_path, exist_ok=True)

    # Initialize an empty list to store the results
    final_grouped_table_1 = []

    # Process each Parquet file individually
    for file in parquet_files:
        # Read the Parquet file into a PyArrow Table
        table = pq.read_table(file)
        
        # Filter the table based on the filter_string
        table = table.filter(pc.equal(table['LobName'], filter_string))
        # Skip if the filtered table is empty
        if len(table) == 0:
            continue

        grouped_table_1 = table.group_by(['EventId', 'PeriodId', 'EventDate']).aggregate([('Loss', 'sum')])
        grouped_table_1 = grouped_table_1.rename_columns(['EventId', 'PeriodId', 'EventDate', 'Sum_Loss'])
    
        # Write intermediate results to disk
        pq.write_table(grouped_table_1, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))

    # Read all intermediate files and concatenate them
    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]

    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]

    final_table_1 = pa.concat_tables(final_grouped_table_1)

    # Perform final grouping and sorting
    f_grouped_table_1 = final_table_1.group_by(['EventId', 'PeriodId', 'EventDate']).aggregate([('Sum_Loss', 'sum')])
    sorted_final_table_1 = f_grouped_table_1.sort_by([('Sum_Loss_sum', 'descending')])
    pq.write_table(sorted_final_table_1, os.path.join(concatenated_folder_path, 'final_grouped_table_1.parquet'))

    # Delete all non-concatenated files
    for f in intermediate_files_1:
        os.remove(f)
    
    dataframe_1 = sorted_final_table_1.to_pandas()

    dataframe_2 = dataframe_1.groupby(['PeriodId'], as_index=False).agg({'Sum_Loss_sum': 'max'})
    dataframe_2.rename(columns={'Sum_Loss_sum': 'Max_Loss'}, inplace=True)
    dataframe_2 = dataframe_2.sort_values(by='Max_Loss', ascending=False).reset_index(drop=True)

    dataframe_2['rate'] = (1 / (speriod * samples))
    dataframe_2['cumrate'] = dataframe_2['rate'].cumsum().round(6)
    dataframe_2['RPs'] = (1 / dataframe_2['cumrate'])

    # Given RP values list
    rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

    # Find the minimum RP in the dataframe
    min_rp = dataframe_2['RPs'].min()

    # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
    if min_rp > 2:
        # Find the RP values that need to be added
        missing_rps = [rp for rp in rp_values if rp < min_rp]

        new_rows = []
        for rp in missing_rps:
            temp_df = dataframe_2.iloc[[-1]].copy()  # Copy the last row as a template
            temp_df['RPs'] = rp
            new_rows.append(temp_df)

        # Concatenate the original dataframe with new rows
        new_rows_df = pd.concat(new_rows, ignore_index=True)
        dataframe_2 = pd.concat([dataframe_2, new_rows_df], ignore_index=True)

        # Fill NaN values only in the new rows
        new_rows_indices = new_rows_df.index
        dataframe_2.loc[new_rows_indices] = dataframe_2.loc[new_rows_indices].fillna(0)


    dataframe_2['TCE_OEP_1'] = ((dataframe_2['Max_Loss'] - dataframe_2['Max_Loss'].shift(-1)) * (dataframe_2['cumrate'] + dataframe_2['cumrate'].shift(-1)) * 0.5)
    dataframe_2['TCE_OEP_2'] = (dataframe_2['TCE_OEP_1'].shift().cumsum() * dataframe_2['RPs'])
    dataframe_2['TCE_OEP_Final'] = (dataframe_2['TCE_OEP_2'] + dataframe_2['Max_Loss'])

    dataframe_3 = dataframe_1.groupby(['PeriodId'], as_index=False).agg({'Sum_Loss_sum': 'sum'})
    dataframe_3.rename(columns={'Sum_Loss_sum': 'S_Sum_Loss'}, inplace=True)
    dataframe_3 = dataframe_3.sort_values(by='S_Sum_Loss', ascending=False).reset_index(drop=True)

    dataframe_3['rate'] = (1 / (speriod * samples))
    dataframe_3['cumrate'] = dataframe_3['rate'].cumsum().round(6)
    dataframe_3['RPs'] = (1 / dataframe_3['cumrate'])


    # Given RP values list
    rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

    # Find the minimum RP in the dataframe
    min_rp = dataframe_3['RPs'].min()

    # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
    if min_rp > 2:
        # Find the RP values that need to be added
        missing_rps = [rp for rp in rp_values if rp < min_rp]

        new_rows = []
        for rp in missing_rps:
            temp_df = dataframe_3.iloc[[-1]].copy()  # Copy the last row
            temp_df['RPs'] = rp
            new_rows.append(temp_df)

        # Concatenate the original dataframe with new rows
        new_rows_df = pd.concat(new_rows, ignore_index=True)
        dataframe_3 = pd.concat([dataframe_3, new_rows_df], ignore_index=True)

        # Fill NaN values only in the new rows
        new_rows_indices = new_rows_df.index
        dataframe_3.loc[new_rows_indices] = dataframe_3.loc[new_rows_indices].fillna(0)


    dataframe_3['TCE_AEP_1'] = ((dataframe_3['S_Sum_Loss'] - dataframe_3['S_Sum_Loss'].shift(-1)) * (dataframe_3['cumrate'] + dataframe_3['cumrate'].shift(-1)) * 0.5)
    dataframe_3['TCE_AEP_2'] = (dataframe_3['TCE_AEP_1'].shift().cumsum() * dataframe_3['RPs'])
    dataframe_3['TCE_AEP_Final'] = (dataframe_3['TCE_AEP_2'] + dataframe_3['S_Sum_Loss'])

    fdataframe_2 = pd.DataFrame()
    fdataframe_3 = pd.DataFrame()

    for value in rps_values:
        closest_index_2 = (dataframe_2['RPs'] - value).abs().idxmin()
        fdataframe_2 = pd.concat([fdataframe_2, dataframe_2.loc[[closest_index_2]]])
        fdataframe_3 = pd.concat([fdataframe_3, dataframe_3.loc[[closest_index_2]]])

    fdataframe_2.rename(columns={'Max_Loss': 'OEP', 'TCE_OEP_Final': 'TCE-OEP'}, inplace=True)
    columns_to_keep_2 = ['RPs']
    columns_to_melt_2 = ['OEP', 'TCE-OEP']
    melted_df_2 = fdataframe_2.melt(id_vars=columns_to_keep_2, value_vars=columns_to_melt_2, var_name='EPType', value_name='Loss')
    melted_df_2.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
    final_df_2 = melted_df_2[['EPType', 'Loss', 'ReturnPeriod']]

    fdataframe_3.rename(columns={'S_Sum_Loss': 'AEP', 'TCE_AEP_Final': 'TCE-AEP'}, inplace=True)
    columns_to_keep_3 = ['RPs']
    columns_to_melt_3 = ['AEP', 'TCE-AEP']
    melted_df_3 = fdataframe_3.melt(id_vars=columns_to_keep_3, value_vars=columns_to_melt_3, var_name='EPType', value_name='Loss')
    melted_df_3.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
    final_df_3 = melted_df_3[['EPType', 'Loss', 'ReturnPeriod']]

    final_df_EP_LOB_GU = pd.concat([final_df_2, final_df_3], ignore_index=True)
    new_ep_type_order = ["OEP", "AEP", "TCE-OEP", "TCE-AEP"]
    final_df_EP_LOB_GU['EPType'] = pd.Categorical(final_df_EP_LOB_GU['EPType'], categories=new_ep_type_order, ordered=True)
    final_df_EP_LOB_GU = final_df_EP_LOB_GU.sort_values(by=['EPType', 'ReturnPeriod'], ascending=[True, False]).reset_index(drop=True)

    # Add LobID and LobName columns
    final_df_EP_LOB_GU['LobId'] = lob_id
    final_df_EP_LOB_GU['LobName'] = filter_string
    final_df_EP_LOB_GU['LobId'] = final_df_EP_LOB_GU['LobId'].apply(lambda x: Decimal(x))


    # Define the schema to match the required Parquet file schema
    schema = pa.schema([
        pa.field('EPType', pa.string(), nullable=True),
        pa.field('Loss', pa.float64(), nullable=True),
        pa.field('ReturnPeriod', pa.float64(), nullable=True),
        pa.field('LobId', pa.decimal128(38, 0), nullable=True),
        pa.field('LobName', pa.string(), nullable=True),
    ])

    # Convert DataFrame to Arrow Table with the specified schema
    table = pa.Table.from_pandas(final_df_EP_LOB_GU, schema=schema)
    parquet_file_path = os.path.join(export_path, parquet_file_path)

    # Save to Parquet
    pq.write_table(table, parquet_file_path)

    print(f"Parquet file saved successfully at {parquet_file_path}")



    delete_folder_and_files(partial_folder_path)
    delete_folder_and_files(concatenated_folder_path)


#GU
for i, (lobname, lobid) in enumerate(lobname_to_lobid.items()):
    export_path =os.path.join(main_folder_path, 'EP', 'Lob','GU')

    parquet_file_path = f'{proname}_{region}_EP_Lob_GU_{i}.parquet'
    try:
        process_parquet_files_EP_lob(parquet_files,export_path, lobname, lobid, speriod, samples, rps_values, parquet_file_path)
    except (NameError, AttributeError, ValueError) as e:
        print(f"Error processing {lobname}: {e}")
        pass


#for GR
    
for i, (lobname, lobid) in enumerate(lobname_to_lobid.items()):
    export_path =os.path.join(main_folder_path, 'EP', 'Lob','GR')

    parquet_file_path = f'{proname}_{region}_EP_Lob_GR_{i}.parquet'
    try:
        process_parquet_files_EP_lob(parquet_files_gr,export_path, lobname, lobid, speriod, samples, rps_values, parquet_file_path)
    except (NameError, AttributeError, ValueError) as e:
        print(f"Error processing {lobname}: {e}")
        pass


partial_folder_path = os.path.join(processing_folder_path, 'partial')
concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')


# In[58]:


def process_parquet_files_Port(parquet_files, export_path, speriod, samples, rps_values,parquet_file_path):
    processing_folder_path = os.path.join(main_folder_path, 'processing')
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')

    os.makedirs(processing_folder_path, exist_ok=True)
    os.makedirs(partial_folder_path, exist_ok=True)
    os.makedirs(concatenated_folder_path, exist_ok=True)

    # Initialize an empty list to store the results
    final_grouped_table_1 = []

    # Process each Parquet file individually
    for file in parquet_files:
        # Read the Parquet file into a PyArrow Table
        table = pq.read_table(file)

        grouped_table_1 = table.group_by(['EventId', 'PeriodId', 'EventDate']).aggregate([('Loss', 'sum')])
        grouped_table_1 = grouped_table_1.rename_columns(['EventId', 'PeriodId', 'EventDate', 'Sum_Loss'])
    
        # Write intermediate results to disk
        pq.write_table(grouped_table_1, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))

    # Read all intermediate files and concatenate them
    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]

    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]

    final_table_1 = pa.concat_tables(final_grouped_table_1)

    # Perform final grouping and sorting
    f_grouped_table_1 = final_table_1.group_by(['EventId', 'PeriodId', 'EventDate']).aggregate([('Sum_Loss', 'sum')])
    sorted_final_table_1 = f_grouped_table_1.sort_by([('Sum_Loss_sum', 'descending')])
    pq.write_table(sorted_final_table_1, os.path.join(concatenated_folder_path, 'final_grouped_table_1.parquet'))

    # Delete all non-concatenated files
    for f in intermediate_files_1:
        os.remove(f)
    
    dataframe_1 = sorted_final_table_1.to_pandas()

    dataframe_2 = dataframe_1.groupby(['PeriodId'], as_index=False).agg({'Sum_Loss_sum': 'max'})
    dataframe_2.rename(columns={'Sum_Loss_sum': 'Max_Loss'}, inplace=True)
    dataframe_2 = dataframe_2.sort_values(by='Max_Loss', ascending=False).reset_index(drop=True)

    dataframe_2['rate'] = (1 / (speriod * samples))
    dataframe_2['cumrate'] = dataframe_2['rate'].cumsum().round(6)
    dataframe_2['RPs'] = (1 / dataframe_2['cumrate'])

    # Given RP values list
    rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

    # Find the minimum RP in the dataframe
    min_rp = dataframe_2['RPs'].min()

    # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
    if min_rp > 2:
        # Find the RP values that need to be added
        missing_rps = [rp for rp in rp_values if rp < min_rp]

        new_rows = []
        for rp in missing_rps:
            temp_df = dataframe_2.iloc[[-1]].copy()  # Copy the last row as a template
            temp_df['RPs'] = rp
            new_rows.append(temp_df)

        # Concatenate the original dataframe with new rows
        new_rows_df = pd.concat(new_rows, ignore_index=True)
        dataframe_2 = pd.concat([dataframe_2, new_rows_df], ignore_index=True)

        # Fill NaN values only in the new rows
        new_rows_indices = new_rows_df.index
        dataframe_2.loc[new_rows_indices] = dataframe_2.loc[new_rows_indices].fillna(0)

    dataframe_2['TCE_OEP_1'] = ((dataframe_2['Max_Loss'] - dataframe_2['Max_Loss'].shift(-1)) * (dataframe_2['cumrate'] + dataframe_2['cumrate'].shift(-1)) * 0.5)
    dataframe_2['TCE_OEP_2'] = (dataframe_2['TCE_OEP_1'].shift().cumsum() * dataframe_2['RPs'])
    dataframe_2['TCE_OEP_Final'] = (dataframe_2['TCE_OEP_2'] + dataframe_2['Max_Loss'])

    dataframe_3 = dataframe_1.groupby(['PeriodId'], as_index=False).agg({'Sum_Loss_sum': 'sum'})
    dataframe_3.rename(columns={'Sum_Loss_sum': 'S_Sum_Loss'}, inplace=True)
    dataframe_3 = dataframe_3.sort_values(by='S_Sum_Loss', ascending=False).reset_index(drop=True)

    dataframe_3['rate'] = (1 / (speriod * samples))
    dataframe_3['cumrate'] = dataframe_3['rate'].cumsum().round(6)
    dataframe_3['RPs'] = (1 / dataframe_3['cumrate'])


    # Given RP values list
    rp_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]

    # Find the minimum RP in the dataframe
    min_rp = dataframe_3['RPs'].min()

    # If the lowest RP in dataframe_2 is greater than 2, add new rows with updated RP values
    if min_rp > 2:
        # Find the RP values that need to be added
        missing_rps = [rp for rp in rp_values if rp < min_rp]

        new_rows = []
        for rp in missing_rps:
            temp_df = dataframe_3.iloc[[-1]].copy()  # Copy the last row
            temp_df['RPs'] = rp
            new_rows.append(temp_df)

        # Concatenate the original dataframe with new rows
        new_rows_df = pd.concat(new_rows, ignore_index=True)
        dataframe_3 = pd.concat([dataframe_3, new_rows_df], ignore_index=True)

        # Fill NaN values only in the new rows
        new_rows_indices = new_rows_df.index
        dataframe_3.loc[new_rows_indices] = dataframe_3.loc[new_rows_indices].fillna(0)
    
    dataframe_3['TCE_AEP_1'] = ((dataframe_3['S_Sum_Loss'] - dataframe_3['S_Sum_Loss'].shift(-1)) * (dataframe_3['cumrate'] + dataframe_3['cumrate'].shift(-1)) * 0.5)
    dataframe_3['TCE_AEP_2'] = (dataframe_3['TCE_AEP_1'].shift().cumsum() * dataframe_3['RPs'])
    dataframe_3['TCE_AEP_Final'] = (dataframe_3['TCE_AEP_2'] + dataframe_3['S_Sum_Loss'])

    fdataframe_2 = pd.DataFrame()
    fdataframe_3 = pd.DataFrame()

    for value in rps_values:
        closest_index_2 = (dataframe_2['RPs'] - value).abs().idxmin()
        fdataframe_2 = pd.concat([fdataframe_2, dataframe_2.loc[[closest_index_2]]])
        fdataframe_3 = pd.concat([fdataframe_3, dataframe_3.loc[[closest_index_2]]])

    fdataframe_2.rename(columns={'Max_Loss': 'OEP', 'TCE_OEP_Final': 'TCE-OEP'}, inplace=True)
    columns_to_keep_2 = ['RPs']
    columns_to_melt_2 = ['OEP', 'TCE-OEP']
    melted_df_2 = fdataframe_2.melt(id_vars=columns_to_keep_2, value_vars=columns_to_melt_2, var_name='EPType', value_name='Loss')
    melted_df_2.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
    final_df_2 = melted_df_2[['EPType', 'Loss', 'ReturnPeriod']]

    fdataframe_3.rename(columns={'S_Sum_Loss': 'AEP', 'TCE_AEP_Final': 'TCE-AEP'}, inplace=True)
    columns_to_keep_3 = ['RPs']
    columns_to_melt_3 = ['AEP', 'TCE-AEP']
    melted_df_3 = fdataframe_3.melt(id_vars=columns_to_keep_3, value_vars=columns_to_melt_3, var_name='EPType', value_name='Loss')
    melted_df_3.rename(columns={'RPs': 'ReturnPeriod'}, inplace=True)
    final_df_3 = melted_df_3[['EPType', 'Loss', 'ReturnPeriod']]

    final_df_EP_Portfolio_GU = pd.concat([final_df_2, final_df_3], ignore_index=True)
    new_ep_type_order = ["OEP", "AEP", "TCE-OEP", "TCE-AEP"]
    final_df_EP_Portfolio_GU['EPType'] = pd.Categorical(final_df_EP_Portfolio_GU['EPType'], categories=new_ep_type_order, ordered=True)
    final_df_EP_Portfolio_GU = final_df_EP_Portfolio_GU.sort_values(by=['EPType', 'ReturnPeriod'], ascending=[True, False]).reset_index(drop=True)

    # Define the schema to match the required Parquet file schema
    schema = pa.schema([
        pa.field('EPType', pa.string(), nullable=True),
        pa.field('Loss', pa.float64(), nullable=True),
        pa.field('ReturnPeriod', pa.float64(), nullable=True),
    ])

    # Convert DataFrame to Arrow Table with the specified schema
    table = pa.Table.from_pandas(final_df_EP_Portfolio_GU, schema=schema)

    # Save to Parquet
    pq.write_table(table, parquet_file_path)

    print(f"Parquet file saved successfully at {parquet_file_path}")
    delete_folder_and_files(partial_folder_path)
    delete_folder_and_files(concatenated_folder_path)






#FOR GU




export_path =os.path.join(main_folder_path, 'EP', 'Portfolio','GU')
parquet_file_path = os.path.join(export_path, f'{proname}_{region}_EP_Portfolio_GU_0.parquet')
try:
    process_parquet_files_Port(parquet_files, export_path, speriod, samples, rps_values, parquet_file_path)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing : {e}")
    pass






#FOR GR




export_path_GR =os.path.join(main_folder_path,'EP','Portfolio','GR')
parquet_file_path_GR = os.path.join(export_path_GR, f'{proname}_{region}_EP_Portfolio_GR_0.parquet')
try:
    process_parquet_files_Port(parquet_files_gr, export_path_GR, speriod, samples, rps_values, parquet_file_path_GR)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing : {e}")
    pass




delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)
flush_cache()


# In[94]:


#now for stats LOB GU 



def process_lob_stats(parquet_files, parquet_file_path):
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    os.makedirs(partial_folder_path, exist_ok=True)

    # Process each Parquet file individually
    for file in parquet_files:
        # Check if the file exists
        if os.path.exists(file):
            # Read the Parquet file into a PyArrow Table
            table = pq.read_table(file)
            
            # Perform the aggregation: sum the Loss column grouped by LobName
            grouped = table.group_by('LobName').aggregate([('Loss', 'sum')])
            
            # Calculate AAL
            loss_sum = grouped.column('Loss_sum').to_numpy()
            aal = loss_sum / speriod / samples
            aal_array = pa.array(aal)
            grouped = grouped.append_column('AAL', aal_array)
            
            # Select only the necessary columns
            grouped = grouped.select(['LobName', 'AAL'])
            
            # Append the grouped Table to the list
            pq.write_table(grouped, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))
        else:
            print(f"File not found: {file}")

    
    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]
    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]
    final_table = pa.concat_tables(final_grouped_table_1)

    # Group the final Table again to ensure all groups are combined
    final_grouped = final_table.group_by('LobName').aggregate([('AAL', 'sum')])

    # Sort the final grouped Table by 'AAL' in descending order
    final_grouped = final_grouped.sort_by([('AAL_sum', 'descending')])

    # Convert the final grouped Table to a Pandas DataFrame
    final_df = final_grouped.to_pandas()

    # Map LobName to LobId
    final_df['LobId'] = final_df['LobName'].map(lobname_to_lobid).apply(lambda x: Decimal(x))

    final_df_STATS_Lob = final_df.rename(columns={'AAL_sum': 'AAL'})

    # Define the columns with NaN values for 'Std' and 'CV'
    final_df_STATS_Lob['Std'] = np.nan
    final_df_STATS_Lob['CV'] = np.nan

    # Reorder the columns to match the specified format
    final_df_STATS_Lob = final_df_STATS_Lob[['AAL', 'Std', 'CV', 'LobId', 'LobName']]

    # Define the desired schema
    desired_schema = pa.schema([
        pa.field('AAL', pa.float64()),
        pa.field('Std', pa.float64()),
        pa.field('CV', pa.float64()),
        pa.field('LobId', pa.decimal128(38)),
        pa.field('LobName', pa.string())
    ])

    # Convert the DataFrame back to a PyArrow Table with the desired schema
    final_table_STATS_Lob = pa.Table.from_pandas(final_df_STATS_Lob, schema=desired_schema)
    pq.write_table(final_table_STATS_Lob, parquet_file_path)
    print(f"Parquet file saved successfully at {parquet_file_path}")
    delete_folder_and_files(partial_folder_path)
    delete_folder_and_files(concatenated_folder_path)




# In[91]:


#LOB GU STATS


# In[92]:


parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Lob', 'GU', f'{proname}_{region}_STATS_Lob_GU_0.parquet')
process_lob_stats(parquet_files, parquet_file_path)


# In[ ]:


#LOB GR STATS


# In[93]:


parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Lob', 'GR', f'{proname}_{region}_STATS_Lob_GR_0.parquet')
process_lob_stats(parquet_files_gr, parquet_file_path)



# In[60]:


#Portfolio STATS 


# In[102]:


def process_portfolio_stats(parquet_files, export_path):
    partial_folder_path = os.path.join(processing_folder_path, 'partial')
    os.makedirs(partial_folder_path, exist_ok=True)

    # Process each Parquet file individually
    for file in parquet_files:
        # Read the Parquet file into a PyArrow Table
        table = pq.read_table(file)
        
        # Perform the aggregation: sum the Loss column grouped by LobName
        grouped = table.group_by('LobName').aggregate([('Loss', 'sum')])
        
        # Calculate AAL
        loss_sum = grouped.column('Loss_sum').to_numpy()
        aal = loss_sum / speriod / samples
        aal_array = pa.array(aal)
        grouped = grouped.append_column('AAL', aal_array)
        
        # Select only the necessary columns
        grouped = grouped.select(['LobName', 'AAL'])
        
        # Append the grouped Table to the list
        pq.write_table(grouped, os.path.join(partial_folder_path, f'grouped_table_1_{os.path.basename(file)}'))

    intermediate_files_1 = [os.path.join(partial_folder_path, f) for f in os.listdir(partial_folder_path) if f.startswith('grouped_table_1_')]
    final_grouped_table_1 = [pq.read_table(f) for f in intermediate_files_1]
    final_table = pa.concat_tables(final_grouped_table_1)
    # Convert the final table to a Pandas DataFrame
    final_df = final_table.to_pandas()

    # Sum all the AAL values without grouping by LobName
    total_aal = final_df['AAL'].sum()

    # Create a DataFrame with the specified columns
    final_df_STATS_Portfolio = pd.DataFrame({
        'AAL': [total_aal],
        'Std': [np.nan],
        'CV': [np.nan],
    })

    # Define the desired schema
    desired_schema = pa.schema([
        pa.field('AAL', pa.float64()),
        pa.field('Std', pa.float64()),
        pa.field('CV', pa.float64()),
    ])

    # Convert the DataFrame back to a PyArrow Table with the desired schema
    final_table_STATS_Portfolio = pa.Table.from_pandas(final_df_STATS_Portfolio, schema=desired_schema)
    pq.write_table(final_table_STATS_Portfolio, export_path)
    print(f"Parquet file saved successfully at {export_path}")
    delete_folder_and_files(partial_folder_path)
    delete_folder_and_files(concatenated_folder_path)






# In[106]:


#GU

parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Portfolio', 'GU', f'{proname}_{region}_STATS_Portfolio_GU_0.parquet')
process_portfolio_stats(parquet_files, parquet_file_path)


# In[ ]:


#GR.


# In[107]:


parquet_file_path_gr = os.path.join(main_folder_path, 'STATS', 'Portfolio', 'GR', f'{proname}_{region}_STATS_Portfolio_GR_0.parquet')
process_portfolio_stats(parquet_files_gr, parquet_file_path_gr)


# In[ ]:













# In[ ]:


delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)


# In[ ]:


#NOW FOR PLT LOB


# In[96]:


def process_PLT_lob(parquet_files, export_path):
    # Directory to store intermediate results
    intermediate_dir = os.path.join(main_folder_path, 'intermediate_results')
    os.makedirs(intermediate_dir, exist_ok=True)

    # Process each Parquet file in chunks and write intermediate results to disk
    for i, file in enumerate(parquet_files):
        file_path = os.path.join(folder_path, file)
        parquet_file = pq.ParquetFile(file_path)
        for j, batch in enumerate(parquet_file.iter_batches()):
            table = pa.Table.from_batches([batch])
            
            # Cast columns to the desired types
            table = table.set_column(table.schema.get_field_index('PeriodId'), 'PeriodId', pa.compute.cast(table['PeriodId'], pa.decimal128(38, 0)))
            table = table.set_column(table.schema.get_field_index('EventId'), 'EventId', pa.compute.cast(table['EventId'], pa.decimal128(38, 0)))
            table = table.set_column(table.schema.get_field_index('EventDate'), 'EventDate', pa.compute.cast(table['EventDate'], pa.timestamp('ms', tz='UTC')))
            table = table.set_column(table.schema.get_field_index('LossDate'), 'LossDate', pa.compute.cast(table['LossDate'], pa.timestamp('ms', tz='UTC')))
            table = table.set_column(table.schema.get_field_index('Loss'), 'Loss', pa.compute.cast(table['Loss'], pa.float64()))
            table = table.set_column(table.schema.get_field_index('Region'), 'Region', pa.compute.cast(table['Region'], pa.string()))
            table = table.set_column(table.schema.get_field_index('Peril'), 'Peril', pa.compute.cast(table['Peril'], pa.string()))
            table = table.set_column(table.schema.get_field_index('Weight'), 'Weight', pa.compute.cast(table['Weight'], pa.float64()))
            table = table.set_column(table.schema.get_field_index('LobId'), 'LobId', pa.compute.cast(table['LobId'], pa.decimal128(38, 0)))
            table = table.set_column(table.schema.get_field_index('LobName'), 'LobName', pa.compute.cast(table['LobName'], pa.string()))
            
            grouped_table = table.group_by(group_by_columns).aggregate([('Loss', 'sum')])
            intermediate_file = os.path.join(intermediate_dir, f"intermediate_{i}_{j}.parquet")
            pq.write_table(grouped_table, intermediate_file)

    # Read intermediate results and combine them
    intermediate_files = [os.path.join(intermediate_dir, f) for f in os.listdir(intermediate_dir) if f.endswith('.parquet')]
    intermediate_tables = [pq.read_table(file) for file in intermediate_files]
    combined_grouped_table = pa.concat_tables(intermediate_tables)

    # Perform the final group by and aggregation
    final_grouped_table = combined_grouped_table.group_by(group_by_columns).aggregate([('Loss_sum', 'sum')])
    final_grouped_table = final_grouped_table.sort_by([('Loss_sum_sum', 'descending')])

    # Rename the aggregated column
    final_grouped_table = final_grouped_table.rename_columns(group_by_columns + ['Loss'])

    # Reorder the columns in the desired order
    final_grouped_table = final_grouped_table.select(ordered_columns)

    # Save the final table to a Parquet file
        # Delete intermediate files
    for file in intermediate_files:
        try:
            os.remove(file)
        except FileNotFoundError:
            print(f"File not found: {file}")

        # Remove the intermediate directory
    try:
        os.rmdir(intermediate_dir)
    except FileNotFoundError:
        print(f"Directory not found: {intermediate_dir}")
    except OSError:
        print(f"Directory not empty or other error: {intermediate_dir}")

    try:
        pq.write_table(final_grouped_table, export_path)
        print(f"Parquet file saved successfully at {export_path}")
    except PermissionError as e:
        print(f"PermissionError: {e}")
    except Exception as e:
        print(f"Error saving Parquet file: {e}")
        







schema = pa.schema([
    pa.field('PeriodId', pa.decimal128(38, 0), nullable=True),
    pa.field('EventId', pa.decimal128(38, 0), nullable=True),
    pa.field('EventDate', pa.timestamp('ms', tz='UTC'), nullable=True),
    pa.field('LossDate', pa.timestamp('ms', tz='UTC'), nullable=True),
    pa.field('Loss', pa.float64(), nullable=True),
    pa.field('Region', pa.string(), nullable=True),
    pa.field('Peril', pa.string(), nullable=True),
    pa.field('Weight', pa.float64(), nullable=True),
    pa.field('LobId', pa.decimal128(38, 0), nullable=True),
    pa.field('LobName', pa.string(), nullable=True)
])

group_by_columns = ['PeriodId', 'EventId', 'EventDate', 'LossDate', 'Region', 'Peril', 'Weight', 'LobId', 'LobName']
ordered_columns = ['PeriodId', 'EventId', 'EventDate', 'LossDate', 'Loss', 'Region', 'Peril', 'Weight', 'LobId', 'LobName']





#for GU




export_path = os.path.join(main_folder_path, 'PLT', 'Lob', 'GU', f'{proname}_{region}_PLT_Lob_GU_0.parquet')

process_PLT_lob(parquet_files, export_path)




#for GR




export_path = os.path.join(main_folder_path, 'PLT', 'Lob', 'GR', f'{proname}_{region}_PLT_Lob_GR_0.parquet')

process_PLT_lob(parquet_files_gr, export_path)


flush_cache()




#PLT Portfolio


group_by_columns = ['PeriodId', 'EventId', 'EventDate', 'LossDate', 'Region', 'Peril', 'Weight']
ordered_columns = ['PeriodId', 'EventId', 'EventDate', 'LossDate', 'Loss', 'Region', 'Peril', 'Weight']

def process_PLT_portfolio_2(parquet_files, export_path):
    # Flush memory at the beginning
    gc.collect()

    # Directory to store intermediate results
    intermediate_dir = os.path.join(main_folder_path, 'PLT', 'Portfolio', 'GU', 'intermediate_results')
    os.makedirs(intermediate_dir, exist_ok=True)

    # Process each Parquet file in chunks and write intermediate results to disk
    for i, file in enumerate(parquet_files):
        parquet_file = pq.ParquetFile(file)
        for j, batch in enumerate(parquet_file.iter_batches()):
            table = pa.Table.from_batches([batch])
            # Cast columns to the desired types
            table = table.set_column(table.schema.get_field_index('PeriodId'), 'PeriodId', pa.compute.cast(table['PeriodId'], pa.decimal128(38, 0)))
            table = table.set_column(table.schema.get_field_index('EventId'), 'EventId', pa.compute.cast(table['EventId'], pa.decimal128(38, 0)))
            table = table.set_column(table.schema.get_field_index('EventDate'), 'EventDate', pa.compute.cast(table['EventDate'], pa.timestamp('ms', tz='UTC')))
            table = table.set_column(table.schema.get_field_index('LossDate'), 'LossDate', pa.compute.cast(table['LossDate'], pa.timestamp('ms', tz='UTC')))
            table = table.set_column(table.schema.get_field_index('Loss'), 'Loss', pa.compute.cast(table['Loss'], pa.float64()))
            table = table.set_column(table.schema.get_field_index('Region'), 'Region', pa.compute.cast(table['Region'], pa.string()))
            table = table.set_column(table.schema.get_field_index('Peril'), 'Peril', pa.compute.cast(table['Peril'], pa.string()))
            table = table.set_column(table.schema.get_field_index('Weight'), 'Weight', pa.compute.cast(table['Weight'], pa.float64()))
            


            grouped_table = table.group_by(group_by_columns).aggregate([('Loss', 'sum')])
            intermediate_file = os.path.join(intermediate_dir, f"intermediate_{i}_{j}.parquet")
            pq.write_table(grouped_table, intermediate_file)

    # Read intermediate results and combine them
    intermediate_files = [os.path.join(intermediate_dir, f) for f in os.listdir(intermediate_dir) if f.endswith('.parquet')]
    intermediate_tables = [pq.read_table(file) for file in intermediate_files]
    combined_grouped_table = pa.concat_tables(intermediate_tables)

    # Perform the final group by and aggregation
    final_grouped_table = combined_grouped_table.group_by(group_by_columns).aggregate([('Loss_sum', 'sum')])

    # Rename the aggregated column
    final_grouped_table = final_grouped_table.rename_columns(group_by_columns + ['Loss'])

    
    # Convert the table to the specified schema
    final_grouped_table = pa.Table.from_arrays(
        [final_grouped_table.column(name).cast(schema.field(name).type) for name in schema.names],
        schema=schema
    )

    final_grouped_table = final_grouped_table.sort_by([('Loss', 'descending')])

    # Save the final table to a Parquet file
    # Delete intermediate files
    for file in intermediate_files:
        try:
            os.remove(file)
        except FileNotFoundError:
            print(f"File not found: {file}")

        # Remove the intermediate directory
    try:
        os.rmdir(intermediate_dir)
    except FileNotFoundError:
        print(f"Directory not found: {intermediate_dir}")
    except OSError:
        print(f"Directory not empty or other error: {intermediate_dir}")

    try:
        pq.write_table(final_grouped_table, export_path)
        print(f"Parquet file saved successfully at {export_path}")
    except PermissionError as e:
        print(f"PermissionError: {e}")
    except Exception as e:
        print(f"Error saving Parquet file: {e}")
    




schema = pa.schema([
    pa.field('PeriodId', pa.decimal128(38, 0), nullable=True),
    pa.field('EventId', pa.decimal128(38, 0), nullable=True),
    pa.field('EventDate', pa.timestamp('ms', tz='UTC'), nullable=True),
    pa.field('LossDate', pa.timestamp('ms', tz='UTC'), nullable=True),
    pa.field('Loss', pa.float64(), nullable=True),
    pa.field('Region', pa.string(), nullable=True),
    pa.field('Peril', pa.string(), nullable=True),
    pa.field('Weight', pa.float64(), nullable=True),
    
])





#FOR GU




export_path = os.path.join(main_folder_path, 'PLT', 'Portfolio', 'GU', f'{proname}_{region}_PLT_Portfolio_GU_0.parquet')

process_PLT_portfolio_2(parquet_files, export_path)




#FOR GR




export_path = os.path.join(main_folder_path, 'PLT', 'Portfolio', 'GR', f'{proname}_{region}_PLT_Portfolio_GR_0.parquet')

process_PLT_portfolio_2(parquet_files_gr, export_path)


#updates made after here 

# # Define the folder to be zipped
# folder_to_zip = main_folder_path  # Change this to your folder path

# # Get the parent directory and zip file name
# parent_dir, folder_name = os.path.split(folder_to_zip)
# output_zip = os.path.join(parent_dir, folder_name)  # Same name as folder

# # Create a zip archive
# shutil.make_archive(output_zip, 'zip', folder_to_zip)

# # Remove the original folder after zipping
# shutil.rmtree(folder_to_zip)

# print(f"Replaced '{folder_to_zip}' with '{output_zip}.zip'")



###################################################


# def delete_files_with_multiple_extensions(folder_path):
#     """Delete files with multiple '.parquet' extensions."""
#     for root, dirs, files in os.walk(folder_path):
#         for file in files:
#             if file.count('.parquet') > 1:
#                 os.remove(os.path.join(root, file))


# def concatenate_parquet_files(main_folder_path):
#     subfolders = [
#         'EP/Admin1_Lob/GU',
#         'EP/Admin1_Lob/GR',
#         'EP/Cresta_Lob/GU',
#         'EP/Cresta_Lob/GR'
#     ]

#     for subfolder in subfolders:
#         folder_path = os.path.join(main_folder_path, subfolder)
#         files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]
        
#         file_groups = {}
#         for file in files:
#             parts = file.split('_')
#             if len(parts) >= 9:
#                 key = parts[8]
#                 if key not in file_groups:
#                     file_groups[key] = []
#                 file_groups[key].append(file)
        
#         for key, group_files in file_groups.items():
#             tables = [pq.read_table(os.path.join(folder_path, f)) for f in group_files]
#             concatenated_table = pa.concat_tables(tables)
#             new_file_name = '_'.join(group_files[0].split('_')[:8]) + f'_{key}.parquet'
#             pq.write_table(concatenated_table, os.path.join(folder_path, new_file_name))
            
#             for file in group_files:
#                 os.remove(os.path.join(folder_path, file))
#     delete_files_with_multiple_extensions(main_folder_path)


# concatenate_parquet_files(main_folder_path)


def delete_files_with_multiple_extensions(folder_path):
    """Delete files with multiple '.parquet' extensions."""
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.count('.parquet') > 1:
                os.remove(os.path.join(root, file))


def concatenate_parquet_files(main_folder_path):
    subfolders = [
        'EP/Admin1_Lob/GU',
        'EP/Admin1_Lob/GR',
        'EP/Cresta_Lob/GU',
        'EP/Cresta_Lob/GR'
    ]

    for subfolder in subfolders:
        folder_path = os.path.join(main_folder_path, subfolder)
        files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]
        
        file_groups = {}
        for file in files:
            parts = file.split('_')
            if len(parts) >= hiphen_count:
                key = parts[hiphen_count-1]
                if key not in file_groups:
                    file_groups[key] = []
                file_groups[key].append(file)
        
        for key, group_files in file_groups.items():
            tables = [pq.read_table(os.path.join(folder_path, f)) for f in group_files]
            concatenated_table = pa.concat_tables(tables)
            new_file_name = '_'.join(group_files[0].split('_')[:hiphen_count-1]) + f'_{key}.parquet'
            pq.write_table(concatenated_table, os.path.join(folder_path, new_file_name))
            
            for file in group_files:
                os.remove(os.path.join(folder_path, file))
    delete_files_with_multiple_extensions(main_folder_path)


concatenate_parquet_files(main_folder_path)


delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)
delete_folder_and_files(resolution_folder_path)
delete_folder_and_files(resolution_folder_path_gr)
delete_folder_and_files(processing_folder_path)


end_time = time.time()  # End time
elapsed_time = (end_time - start_time) / 60  # Convert seconds to minutes

print(f"Process finished in {elapsed_time:.2f} minutes")

# In[ ]:


# In[ ]:





# In[ ]:




