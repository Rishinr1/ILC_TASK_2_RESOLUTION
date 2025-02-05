import dask.dataframe as dd
from dask.distributed import Client
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

start_time = time.time()  # Start time


# Function to flush the cache
def flush_cache():
    gc.collect()

flush_cache()


# In[114]:
folder_path = r'D:\RISHIN\13_ILC_TASK1\input\PARQUET_FILES'
folder_path_gr = r'D:\RISHIN\13_ILC_TASK1\input\PARQUET_FILES_GR'



speriod=int(input("Enter the simulation period: "))
samples=int(input("Enter the number of samples: "))



output_folder_path = input("Enter the output folder path: ")


# In[115]:


# Define the folder containing the Parquet files

# List all Parquet files in the folder
parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]


# In[116]:


# Define the folder containing the Parquet files

# List all Parquet files in the folder
parquet_files_gr = [os.path.join(folder_path_gr, f) for f in os.listdir(folder_path_gr) if f.endswith('.parquet')]


# In[ ]:


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


# In[7]:


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
        main_folder_path = os.path.join(output_folder_path, f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_Losses')
        
        # Define subfolders
        subfolders = ['Admin_1','Admin_1_Lob','Cresta','Cresta_1','EP', 'PLT', 'STATS']
        nested_folders = ['Lob', 'Portfolio']
        innermost_folders = ['GR', 'GU']
        
        # Create the main folder and subfolders
        for subfolder in subfolders:
            subfolder_path = os.path.join(main_folder_path, subfolder)
            os.makedirs(subfolder_path, exist_ok=True)
            
            for nested_folder in nested_folders:
                nested_folder_path = os.path.join(subfolder_path, nested_folder)
                os.makedirs(nested_folder_path, exist_ok=True)
                
                for innermost_folder in innermost_folders:
                    innermost_folder_path = os.path.join(nested_folder_path, innermost_folder)
                    os.makedirs(innermost_folder_path, exist_ok=True)
        
        print(f"Folders created successfully at {main_folder_path}")
        break  # Process only the first batch
else:
    print("No Parquet files found in the specified folder.")


# In[ ]:


#for EP  Lob


# In[74]:


def process_parquet_files(parquet_files, export_path, filter_string, lob_id, speriod, samples, rps_values,parquet_file_path):
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
    dataframe_2['TCE_OEP_1'] = ((dataframe_2['Max_Loss'] - dataframe_2['Max_Loss'].shift(-1)) * (dataframe_2['cumrate'] + dataframe_2['cumrate'].shift(-1)) * 0.5)
    dataframe_2['TCE_OEP_2'] = (dataframe_2['TCE_OEP_1'].shift().cumsum() * dataframe_2['RPs'])
    dataframe_2['TCE_OEP_Final'] = (dataframe_2['TCE_OEP_2'] + dataframe_2['Max_Loss'])

    dataframe_3 = dataframe_1.groupby(['PeriodId'], as_index=False).agg({'Sum_Loss_sum': 'sum'})
    dataframe_3.rename(columns={'Sum_Loss_sum': 'S_Sum_Loss'}, inplace=True)
    dataframe_3 = dataframe_3.sort_values(by='S_Sum_Loss', ascending=False).reset_index(drop=True)

    dataframe_3['rate'] = (1 / (speriod * samples))
    dataframe_3['cumrate'] = dataframe_3['rate'].cumsum().round(6)
    dataframe_3['RPs'] = (1 / dataframe_3['cumrate'])
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
    final_df_EP_LOB_GU['LobID'] = lob_id
    final_df_EP_LOB_GU['LobName'] = filter_string
    final_df_EP_LOB_GU['LobID'] = final_df_EP_LOB_GU['LobID'].apply(lambda x: Decimal(x))


    # Define the schema to match the required Parquet file schema
    schema = pa.schema([
        pa.field('EPType', pa.string(), nullable=True),
        pa.field('Loss', pa.float64(), nullable=True),
        pa.field('ReturnPeriod', pa.float64(), nullable=True),
        pa.field('LobID', pa.decimal128(38, 0), nullable=True),
        pa.field('LobName', pa.string(), nullable=True),
    ])

    # Convert DataFrame to Arrow Table with the specified schema
    table = pa.Table.from_pandas(final_df_EP_LOB_GU, schema=schema)

    # Save to Parquet
    pq.write_table(table, parquet_file_path)

    print(f"Parquet file saved successfully at {parquet_file_path}")



# In[ ]:


#FOR GU


# In[76]:


export_path =os.path.join(main_folder_path, 'EP', 'Lob','GU')
parquet_file_path_AUTO = os.path.join(export_path, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GU_1.parquet')
parquet_file_path_AGR = os.path.join(export_path, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GU_0.parquet')
parquet_file_path_COM = os.path.join(export_path, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GU_2.parquet')
parquet_file_path_IND = os.path.join(export_path, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GU_3.parquet')
parquet_file_path_SPER = os.path.join(export_path, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GU_4.parquet')
parquet_file_path_FRST= os.path.join(export_path, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GU_5.parquet')
parquet_file_path_GLH = os.path.join(export_path, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GU_6.parquet')

rps_values = [10000, 5000, 1000, 500, 250, 200, 100, 50, 25, 10, 5, 2]




try:
    process_parquet_files(parquet_files, export_path, 'AGR', 1, speriod, samples, rps_values, parquet_file_path_AGR)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing AGR: {e}")
    pass

try:
    process_parquet_files(parquet_files, export_path, 'AUTO', 2, speriod, samples, rps_values, parquet_file_path_AUTO)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing AUTO: {e}")
    pass

try:
    process_parquet_files(parquet_files, export_path, 'COM', 3, speriod, samples, rps_values, parquet_file_path_COM)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing COM: {e}")
    pass

try:
    process_parquet_files(parquet_files, export_path, 'IND', 4, speriod, samples, rps_values, parquet_file_path_IND)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing IND: {e}")
    pass

try:
    process_parquet_files(parquet_files, export_path, 'SPER', 5, speriod, samples, rps_values, parquet_file_path_SPER)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing SPER: {e}")
    pass

try:
    process_parquet_files(parquet_files, export_path, 'FRST', 6, speriod, samples, rps_values, parquet_file_path_FRST)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing FRST: {e}")
    pass

try:
    process_parquet_files(parquet_files, export_path, 'GLH', 7, speriod, samples, rps_values, parquet_file_path_GLH)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing GLH: {e}")
    pass




# In[ ]:


#NEXT FOR GR


# In[77]:


export_path_gr =os.path.join(main_folder_path, 'EP', 'Lob','GR')
parquet_file_path_AUTO = os.path.join(export_path_gr, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GR_1.parquet')
parquet_file_path_AGR = os.path.join(export_path_gr, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GR_0.parquet')
parquet_file_path_COM = os.path.join(export_path_gr, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GR_2.parquet')
parquet_file_path_IND = os.path.join(export_path_gr, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GR_3.parquet')
parquet_file_path_SPER = os.path.join(export_path_gr, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GR_4.parquet')
parquet_file_path_FRST= os.path.join(export_path_gr, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GR_5.parquet')
parquet_file_path_GLH = os.path.join(export_path_gr, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Lob_GR_6.parquet')





try:
    process_parquet_files(parquet_files_gr, export_path, 'AGR', 1, speriod, samples, rps_values, parquet_file_path_AGR)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing AGR: {e}")
    pass

try:
    process_parquet_files(parquet_files_gr, export_path, 'AUTO', 2, speriod, samples, rps_values, parquet_file_path_AUTO)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing AUTO: {e}")
    pass

try:
    process_parquet_files(parquet_files_gr, export_path, 'COM', 3, speriod, samples, rps_values, parquet_file_path_COM)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing COM: {e}")
    pass

try:
    process_parquet_files(parquet_files_gr, export_path, 'IND', 4, speriod, samples, rps_values, parquet_file_path_IND)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing IND: {e}")
    pass

try:
    process_parquet_files(parquet_files_gr, export_path, 'SPER', 5, speriod, samples, rps_values, parquet_file_path_SPER)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing SPER: {e}")
    pass

try:
    process_parquet_files(parquet_files_gr, export_path, 'FRST', 6, speriod, samples, rps_values, parquet_file_path_FRST)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing FRST: {e}")
    pass

try:
    process_parquet_files(parquet_files_gr, export_path, 'GLH', 7, speriod, samples, rps_values, parquet_file_path_GLH)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing GLH: {e}")
    pass


# In[78]:





# In[80]:


processing_folder_path = os.path.join(main_folder_path, 'processing')
partial_folder_path = os.path.join(processing_folder_path, 'partial')
concatenated_folder_path = os.path.join(processing_folder_path, 'concatenated')

delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)
delete_folder_and_files(processing_folder_path)


# In[ ]:





# In[82]:


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
    dataframe_2['TCE_OEP_1'] = ((dataframe_2['Max_Loss'] - dataframe_2['Max_Loss'].shift(-1)) * (dataframe_2['cumrate'] + dataframe_2['cumrate'].shift(-1)) * 0.5)
    dataframe_2['TCE_OEP_2'] = (dataframe_2['TCE_OEP_1'].shift().cumsum() * dataframe_2['RPs'])
    dataframe_2['TCE_OEP_Final'] = (dataframe_2['TCE_OEP_2'] + dataframe_2['Max_Loss'])

    dataframe_3 = dataframe_1.groupby(['PeriodId'], as_index=False).agg({'Sum_Loss_sum': 'sum'})
    dataframe_3.rename(columns={'Sum_Loss_sum': 'S_Sum_Loss'}, inplace=True)
    dataframe_3 = dataframe_3.sort_values(by='S_Sum_Loss', ascending=False).reset_index(drop=True)

    dataframe_3['rate'] = (1 / (speriod * samples))
    dataframe_3['cumrate'] = dataframe_3['rate'].cumsum().round(6)
    dataframe_3['RPs'] = (1 / dataframe_3['cumrate'])
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



# In[ ]:


#FOR GU


# In[86]:


export_path =os.path.join(main_folder_path, 'EP', 'Portfolio','GU')
parquet_file_path = os.path.join(export_path, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Portfolio_GU_0.parquet')
try:
    process_parquet_files_Port(parquet_files, export_path, speriod, samples, rps_values, parquet_file_path)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing : {e}")
    pass





# In[ ]:


#FOR GR


# In[87]:


export_path_GR =os.path.join(main_folder_path,'EP','Portfolio','GR')
parquet_file_path_GR = os.path.join(export_path_GR, 'ILC2024_EUWS_PLA_WI_EP_BE_EUR_EP_Portfolio_GR_0.parquet')
try:
    process_parquet_files_Port(parquet_files_gr, export_path_GR, speriod, samples, rps_values, parquet_file_path_GR)
except (NameError, AttributeError,ValueError) as e:
    print(f"Error processing : {e}")
    pass





# In[88]:


delete_folder_and_files(partial_folder_path)
delete_folder_and_files(concatenated_folder_path)
delete_folder_and_files(processing_folder_path)


# In[ ]:


flush_cache()


# In[ ]:


#now for stats LOB GU 


# In[90]:


from decimal import Decimal

def process_lob_stats(parquet_files, parquet_file_path):
    aggregated_tables_lob_stats = []

    # Define the mapping of LobName to LobId
    lobname_to_lobid = {
        'AGR': 1,
        'AUTO': 2,
        'COM': 3,
        'IND': 4,
        'SPER': 5,
        'FRST': 6,
        'GLH': 7
    }

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
            aggregated_tables_lob_stats.append(grouped)
        else:
            print(f"File not found: {file}")

    # Check if any tables were aggregated
    if not aggregated_tables_lob_stats:
        print("No tables were aggregated. Please check the input files.")
    else:
        # Concatenate all the grouped Tables
        final_table = pa.concat_tables(aggregated_tables_lob_stats)

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



# In[91]:


#LOB GU STATS


# In[92]:


parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_STATS_Lob_GU_0.parquet')
process_lob_stats(parquet_files, parquet_file_path)


# In[ ]:


#LOB GR STATS


# In[93]:


parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Lob', 'GR', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_STATS_Lob_GR_0.parquet')
process_lob_stats(parquet_files_gr, parquet_file_path)


# In[ ]:


#Portfolio STATS 


# In[102]:


def process_portfolio_stats(parquet_files, export_path):
    aggregated_tables = []

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
        aggregated_tables.append(grouped)

    # Concatenate all the grouped Tables
    final_table = pa.concat_tables(aggregated_tables)

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





# In[106]:


#GU

parquet_file_path = os.path.join(main_folder_path, 'STATS', 'Portfolio', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_STATS_Portfolio_GU_0.parquet')
process_portfolio_stats(parquet_files, parquet_file_path)


# In[ ]:


#GR.


# In[107]:


parquet_file_path_gr = os.path.join(main_folder_path, 'STATS', 'Portfolio', 'GR', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_STATS_Portfolio_GR_0.parquet')
process_portfolio_stats(parquet_files_gr, parquet_file_path_gr)


# In[ ]:


#NOW FOR PLT LOB


# In[122]:


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



# In[123]:


#for GU


# In[124]:


export_path = os.path.join(main_folder_path, 'PLT', 'Lob', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_PLT_Lob_GU_0.parquet')

process_PLT_lob(parquet_files, export_path)


# In[121]:


#for GR


# In[125]:


export_path = os.path.join(main_folder_path, 'PLT', 'Lob', 'GR', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_PLT_Lob_GR_0.parquet')

process_PLT_lob(parquet_files_gr, export_path)


# In[ ]:


flush_cache()


# In[ ]:


#PLT Portfolio


# In[126]:


def process_PLT_portfolio(parquet_files, export_path):
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

    # Convert PeriodId and EventId to strings
    final_grouped_table = final_grouped_table.set_column(
        final_grouped_table.schema.get_field_index('PeriodId'),
        'PeriodId',
        final_grouped_table.column('PeriodId').cast(pa.string())
    )
    final_grouped_table = final_grouped_table.set_column(
        final_grouped_table.schema.get_field_index('EventId'),
        'EventId',
        final_grouped_table.column('EventId').cast(pa.string())
    )

    # Convert the table to the specified schema
    final_grouped_table = pa.Table.from_arrays(
        [final_grouped_table.column(name).cast(schema.field(name).type) for name in schema.names],
        schema=schema
    )

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


    # Write the table to a Parquet file with the specified schema
    final_grouped_table = final_grouped_table.sort_by([('Loss', 'descending')])


    pq.write_table(final_grouped_table, export_path)
    print(f"Parquet file saved successfully at {export_path}")




schema = pa.schema([
    pa.field('PeriodId', pa.decimal128(38, 0), nullable=True, metadata={'field_id': '-1'}),
    pa.field('EventId', pa.decimal128(38, 0), nullable=True, metadata={'field_id': '-1'}),
    pa.field('EventDate', pa.timestamp('ms', tz='UTC'), nullable=True, metadata={'field_id': '-1'}),
    pa.field('LossDate', pa.timestamp('ms', tz='UTC'), nullable=True, metadata={'field_id': '-1'}),
    pa.field('Loss', pa.float64(), nullable=True, metadata={'field_id': '-1'}),
    pa.field('Region', pa.string(), nullable=True, metadata={'field_id': '-1'}),
    pa.field('Peril', pa.string(), nullable=True, metadata={'field_id': '-1'}),
    pa.field('Weight', pa.float64(), nullable=True, metadata={'field_id': '-1'})
])



# In[127]:


#FOR GU


# In[128]:


export_path = os.path.join(main_folder_path, 'PLT', 'Portfolio', 'GU', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_PLT_Portfolio_GU_0.parquet')

process_PLT_lob(parquet_files, export_path)


# In[ ]:


#FOR GR


# In[129]:


export_path = os.path.join(main_folder_path, 'PLT', 'Portfolio', 'GR', f'ILC2024_EUWS_PLA_WI_EP_{country}_EUR_PLT_Portfolio_GR_0.parquet')

process_PLT_lob(parquet_files_gr, export_path)


#updates made after here 

# Define the folder to be zipped
folder_to_zip = main_folder_path  # Change this to your folder path

# Get the parent directory and zip file name
parent_dir, folder_name = os.path.split(folder_to_zip)
output_zip = os.path.join(parent_dir, folder_name)  # Same name as folder

# Create a zip archive
shutil.make_archive(output_zip, 'zip', folder_to_zip)

# Remove the original folder after zipping
shutil.rmtree(folder_to_zip)

print(f"Replaced '{folder_to_zip}' with '{output_zip}.zip'")





end_time = time.time()  # End time
elapsed_time = (end_time - start_time) / 60  # Convert seconds to minutes

print(f"Process finished in {elapsed_time:.2f} minutes")



