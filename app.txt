#!/usr/bin/env python
# coding: utf-8

# # Stages of scripts for end to end caffeine report generation

# ## Pre - Stage 1 : Preliminary steps

# ### Step1: Import libraries

# #### Import libraries

# In[1]:


import pandas as pd
import os
import sys
import subprocess
import numpy as np
from bs4 import BeautifulSoup
import re
from pdfrw import PdfReader, PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet


from PyPDF2 import PdfMerger

# Check the number of arguments
#if len(sys.argv) < 2:
    #print("Usage: python script.py <arg1> <arg2> ...")
    #sys.exit(1)

# Access command-line arguments
#arg1 = sys.argv[1]
#arg2 = sys.argv[2]

# ### Step 2: Calling functions and creating folder paths

# #### Calling functions from functions_master

# In[2]:


# get_ipython().run_line_magic('run', '-i functions_master.ipynb')


# Defining folder path
# glb_root = (r"C:\Users\sangeetha\OneDrive\01-XCode\13_scripts\caffeine\working_folder")
glb_root = (r"C:\Users\voice\Desktop\prefinal")
glb_01_input = (glb_root + r"\01_input")
glb_02_master = (glb_root + r"\02_master")
glb_03_output = (glb_root + r"\03_output")
glb_04_temp_input = (glb_root + r"\04_temp_input")
glb_05_mapping_files = (glb_root + r"\05_mapping_files" + r"\P01_caffeine")
glb_masterfile =  glb_02_master + "\\Caffeine_Master.xlsx"
##### For report generation
glb_template_root = (glb_root + r"\06_templates")
#Define external binaries
wkhtml_bin = "C:\\Users\\voice\\Desktop\\prefinal\\External_binary_dependency\\wkhtmltopdf.exe"
ghostscript_bin = "C:\\Users\\voice\\Desktop\\prefinal\\External_binary_dependency\\gswin64.exe"

from functions_master import delete_files_in_folder, read_file_with_skiprows_MyHeritage, read_file_with_skiprows_23andme, read_file_with_skiprows_Ances, get_file_names_in_path, write_dataframes_to_parquet, read_specific_parquet_files, extract_columns_to_dataframe, merge_dataframes_same_colnames, merge_dataframes_differ_colnames,  remove_duplicates_and_create_column, get_icon_number, gen_summary_for_file, save_trait_to_file, get_bg_color, get_all_html_files, convert_html_to_pdf ,remove_before_angle_bracket, get_risk_details


# #### Defining the folder path

# In[4]:




# Defining empty dataframe
client_input = pd.DataFrame()


# #### Creating working directories

# In[5]:


# Create working directories if not existing
try:
    os.mkdir(glb_root)
except FileExistsError:
    print("Folder already exists. Skipping.")
try:
    os.mkdir(glb_01_input)
except FileExistsError:
    print("Folder already exists. Skipping.")

try:
    os.mkdir(glb_02_master)
except FileExistsError:
    print("Folder already exists. Skipping.")

try:
    os.mkdir(glb_03_output)
except FileExistsError:
    print("Folder already exists. Skipping.")

try:
    os.mkdir(glb_04_temp_input)
except FileExistsError:
    print("Folder already exists. Skipping.")

try:
    os.mkdir(glb_05_mapping_files)
except FileExistsError:
    print("Folder already exists. Skipping.")


# #### Clean up of old residue files

# In[6]:


#Start performing clean up of old residues
delete_files_in_folder(glb_03_output)
delete_files_in_folder(glb_04_temp_input)



# # ******* END OF PRE-STAGE1 ********

# ## Stage 1: Import and Cleaning input files

# - **Import files**
#     1. **Master file**
#         - Caffeine_Master.xlsx
#         - We are only interested in "Curation" sheet and "Proxy - European" sheet for this analysis
#     2. **Client_raw file**
#         - txt or csv format files from 23andMe/AncestryDNA/MyHeritage

# ### Step 1: Import Client file from input folder

# #### Access input folder

# In[7]:


#os.chdir(glb_01_input)


# #### Import client file depending on the source

# In[8]:


# Define a way to identify the source input files and all the appropriate function to read the input data
# Based on the type of inputs from various companies, we call the functions tuned particularly for the input scenarios
# Read the file and store the lines in a list
client_input.drop(client_input.index, inplace=True)

##################################################################
#Enumerate the 01_input folder for the client files of any format
##################################################################
file_names_list = get_file_names_in_path(glb_01_input)

input_file = file_names_list[0]
with open(glb_01_input + "\\" + input_file, 'r') as file:
    lines = file.readlines()

# Reading lines from client raw files based on sources as defined in the functions
for i, line in enumerate(lines):
    if  "AncestryDNA" in line:
        client_input = read_file_with_skiprows_Ances(glb_01_input + "\\" + input_file)
        break
    if "23andMe" in line:
        client_input = read_file_with_skiprows_23andme(glb_01_input + "\\" + input_file)
        break
    if "MyHeritage" in line:
        client_input = read_file_with_skiprows_MyHeritage(glb_01_input + "\\" + input_file)
        break


# #### Read client file

# In[9]:


client_input.head(5)


# ### Step 2: Import Caffeine Master from master folder

# #### Check folder path

# In[10]:


print (glb_masterfile)


# #### import Curation Sheet in Caffeine master

# In[11]:


#import 'Curation' sheet from Caffeine Master into cur_mast datafame
cur_mast= pd.read_excel(glb_masterfile,sheet_name = "Curation")


# In[12]:


#cur_mast.head()


# #### Import Proxy-European sheet in caffeine master

# In[13]:


#import '"Proxy - European"' sheet from Caffeine Master into proxy_mast datafame
proxy_mast= pd.read_excel(glb_masterfile,sheet_name = "Proxy - European")


# In[14]:


#proxy_mast.head()


# ### Step 3: Rename Columns
#
# 1. Open **"Curation sheet"** in the dataframe **cur_mast**:
#     - Rename each column in the dataframe with short "cur-" prefix
# 2. Open **"Proxy-European sheet"** in the dataframe **proxy_mast**:
#     - Rename each column in the dataframe with short "prxy-" prefix:

# In[15]:


#Rename columns in cur_mast
cur_mast.rename(columns={
    "trait_class": "cur_trt_clss",
    "trait": "cur_trt",
    "rsID": "cur_rs_id",
    "gene": "cur_gene",
    "NA": "cur_na",
    "VA": "cur_va",
    "association": "cur_asn",
    "effect (B or R)": "cur_effect",
    "score": "cur_scortyp",
    "annotation": "cur_anno",
    "ref": "cur_ref",
    "verification (y or n or c)": "cur_verifcn",
    "comment (scoring annotation)": "cur_comment"
}, inplace=True)


# In[16]:


#Rename columns in proxy_mast
proxy_mast.rename(columns={
    "rsID": "prxy_rs_id",
    "proxy-rsID": "prxy_prx_rs_id",
    "chromsome": "prxy_chrm",
    "position - rsID": "pos_rs_id",
    "position - proxy": "pos_prxy",
    "distance": "prxy_dist",
    "R2": "prxy_r2",
    "major allele": "prxy_mjr_alle",
    "minor allele": "prxy_mnr_alle",
    "MAF": "prxy_maf"
}, inplace=True)


# In[17]:


#check
# cur_mast.info()


# In[18]:


#check
# proxy_mast.info()


# ### Step 4: Select Relevant Columns to clean data
#  1. Create **new curation dataframe** with only the columns that are used for analysis
#  2. Create **new Proxy_European sheet** with only columns that are used for analysis
#  3. Create **new client file** with only columns that are used for analysis

# In[19]:


#Create new curation dataframe with only relavant columns
new_cur_mast = cur_mast[["cur_trt_clss","cur_trt","cur_rs_id", "cur_gene", "cur_na", "cur_va", "cur_effect", "cur_scortyp"]]


# In[20]:


#new_cur_mast.head()


# In[21]:


#Create new proxy dataframe with only relavant columns
new_prxy_mast = proxy_mast[["prxy_rs_id", "prxy_prx_rs_id", "prxy_mjr_alle", "prxy_mnr_alle"]]


# In[22]:


#new_prxy_mast.head(3)


# In[23]:


#Create new client file dataframe with only relavant columns
new_clnt_rw =client_input[["cli_rs_id","cli_geno"]]


# In[24]:


#new_clnt_rw.head(3)


# ### Step 5: Export to Temp Input
# 1. Export the new curation dataframe 'new_cur_mast' as output into Temp input folder
# 2. Export new proxy dataframe 'new_prxy_mast' as output into Temp input folder
# 3. Export client file dataframe 'new_clnt_rw' as output into Temp input folder

# In[25]:


#print(glb_04_temp_input)


# In[26]:


# Form a list of dataframes to be converted to parquet files
dataframes = [new_cur_mast,new_prxy_mast,new_clnt_rw]


# In[27]:


# Form a list of file names in parquet format
filenames = ['new_cur_mast.parquet', 'new_prxy_mast.parquet', 'new_clnt_rw.parquet']


# In[28]:


# Call the function to write the DataFrames to Parquet files
write_dataframes_to_parquet(dataframes, filenames, glb_04_temp_input)


# In[29]:


#  new_cur_mast.to_parquet(os.path.join(glb_04_temp_input, "new_cur_mast.parquet") , index=False)


# In[30]:


# new_prxy_mast.to_parquet(os.path.join(glb_04_temp_input, "new_prxy_mast.parquet") , index=False)


# In[31]:


# new_clnt_rw.to_parquet(os.path.join(glb_04_temp_input, "new_clnt_rw.parquet") , index=False)


# # ******* END OF STAGE1 ********

# # Stage 2: Input new files from Stage 1 and perform MATCHES

# ### Step1: Input files as dataframes into the environment

# - input curation master : **new_cur_mast**
# - input proxy file: **new_prxy_mast**
# - input client raw data: **new_clt_rw**

# In[32]:


specific_filenames = ['new_cur_mast.parquet', 'new_prxy_mast.parquet', 'new_clnt_rw.parquet']


# In[33]:


# Call the function to read specific Parquet files
dataframes = read_specific_parquet_files(glb_04_temp_input, specific_filenames)


# In[34]:


new_cur_mast = dataframes['new_cur_mast.parquet']
new_prxy_mast = dataframes['new_prxy_mast.parquet']
new_clnt_rw = dataframes['new_clnt_rw.parquet']


# In[35]:


#new_cur_mast.head()


# In[36]:


#new_prxy_mast.head()


# In[37]:


#new_clnt_rw.head()


# ### Step 2: Create Client_proxy_match file having matched proxy's for client rsid not in curation
#
# - First Dataframe: **Cl_pr_match** (Exported in same name as Parquet)
#     - Column1: **prxy_rs_id**: same as "rsID" in "Proxy-European"
#     - Column2: **prxy_prx_rs_id**: same as "proxy-rsID" in "Proxy-European"
#     - Column3: **cust_match**: Gives 1 when proxy found in client and 0 not found
# - Second Dataframe: **Clnt_prx_mtch1** (Exported in same name as Parquet)
#     - Has only the matched proxy IDs (i.e., rows with only '1s' in cust_match from Cl_pr_match)
#     - Column1: **prxy_rs_id**
#     - Column2: **prxy_prx_rs_id**
#     - Column3: **prxy_mjr_allele**: corresponding proxy major allele (Proxy_NA)
#     - Column4: **prxy_mnr_allele**: corresponding proxy minor allele (Proxy_VA)

# In[38]:


# Columns to extract
columns_to_extract = ["prxy_rs_id", "prxy_prx_rs_id"]


# In[39]:


cl_pr_match = extract_columns_to_dataframe(new_prxy_mast,columns_to_extract)


# In[40]:


cl_pr_match = cl_pr_match.copy()


# In[41]:


cl_pr_match['cust_match'] = 0  # Initialize the 'cust_match' column with 0
cl_pr_match.loc[cl_pr_match["prxy_prx_rs_id"].isin(new_clnt_rw["cli_rs_id"]), 'cust_match'] = 1


# In[42]:


cl_pr_match.head()


# In[43]:


# Dataframe clnt_prx_mtch1 contains those prxy_rs_id and prxy_prx_rs_id from cl_pr_match that has cust_match to be 1
clnt_prx_mtch1 = cl_pr_match.loc[cl_pr_match['cust_match']==1,['prxy_rs_id','prxy_prx_rs_id']]


# In[44]:


merge_on_columns = ['prxy_rs_id', 'prxy_prx_rs_id']


# In[45]:


# Call the function to merge the DataFrames
clnt_prx_mtch1  = merge_dataframes_same_colnames(clnt_prx_mtch1, new_prxy_mast, merge_on_columns, how='left')


# In[46]:


clnt_prx_mtch1.head(3)


# ### Step 3: Export the client_proxy_match file

# In[47]:


# Form a list of dataframes to be converted to parquet files
dataframes = [cl_pr_match,clnt_prx_mtch1]


# In[48]:


# Form a list of file names in parquet format
filenames = ['cl_pr_match.parquet', 'clnt_prx_mtch1.parquet']


# In[49]:


# Call the function to write the DataFrames to Parquet files
write_dataframes_to_parquet(dataframes, filenames, glb_04_temp_input)


# ### Step 4: Curation_trait_match (used in Stage 4)
# - Create a new Dataframe: cur_rs_trt_match
# - Is a match between **curation_rsid** and **trait_class** and create a unique_id for each rsid
# - It filters out all the curation rs_id from "Curation sheet" as it is

# In[50]:


columns_to_extract = ["cur_rs_id","cur_na","cur_va"]


# In[51]:


cur_rs_trt_match = extract_columns_to_dataframe(new_cur_mast,columns_to_extract)


# In[52]:


# #Create a dataframe cur_rs_trt_match with the curation_rsid and trait_class column from the new_prxy_mast dataframe
# cur_rs_trt_match = pd.DataFrame({"cur_rs_id":new_cur_mast["cur_rs_id"],"cur_na": new_cur_mast["cur_na"],"cur_va": new_cur_mast["cur_va"]})


# In[53]:


#cur_rs_trt_match


# ### Step 5: Export the cur_rs_trt_match file

# In[54]:


# Form a list of dataframes to be converted to parquet files
dataframes = [cur_rs_trt_match]


# In[55]:


# Form a list of file names in parquet format
filenames = ['cur_rs_trt_match.parquet']


# In[56]:


# Call the function to write the DataFrames to Parquet files
write_dataframes_to_parquet(dataframes, filenames, glb_04_temp_input)


# # ******* END OF STAGE2 ********

# # Stage 3: Creating of combined rsid and direct inputs from "Curation master"

# ### Step 1: Match rsids in Client raw file with rsids in Curation master

#  - Create an intermediate Dataframe: **Cur_cli_rsid_mtch**
# - It has all curation ids (in cur_rs_id) column, all the cli_rs_id (NA for those not found in client) and corresponding other columns in curation shee

# In[57]:


left_on_columns = ["cur_rs_id"]


# In[58]:


right_on_columns = ["cli_rs_id"]


# In[59]:


# Call the function to merge the DataFrames
Cur_cli_rsid_mtch  = merge_dataframes_differ_colnames(new_cur_mast, new_clnt_rw, left_on_columns, right_on_columns, how='left')


# In[60]:


#Cur_cli_rsid_mtch


# ### Step 2: Create dataframe with only rsids (QC_rsids) using Cur_cli_rsid_match

# In[61]:


#Create a dataframe QC_rsids with only the rsid columns from Cur_cli_rsid_match
columns_to_extract = ["cur_rs_id", "cli_rs_id"]


# In[62]:


QC_rsids = extract_columns_to_dataframe(Cur_cli_rsid_mtch,columns_to_extract)


# In[63]:


#QC_rsids.head()


# In[64]:


QC_rsids = QC_rsids.copy()


# In[65]:


#Fill with "NA" where ever the match between rsids were not found
QC_rsids= QC_rsids.fillna("NA")


# In[66]:


#create the proxy_rs_id column
QC_rsids["proxy_rs_id"]=''


# In[67]:


# Iterate over each row in QC_rsids
for index, row in QC_rsids.iterrows():
    # Check if client_rs_id value is "NA"
    if row['cli_rs_id'] == 'NA':
        Curation_rs_id = row['cur_rs_id']
        # Look up corresponding value of Curation_rs_id in prxy_rs_id of clnt_prx_mtch1
        match_value = clnt_prx_mtch1.loc[clnt_prx_mtch1['prxy_rs_id']== Curation_rs_id, "prxy_prx_rs_id"].values
        # If a match is found, assign corresponding value from prxy_prx_rs_id of clnt_prx_mtch1 to proxy_rs_id of QC_rsids
        if len(match_value) >0:
            QC_rsids.at[index,'proxy_rs_id']= match_value[0]
        else:
            QC_rsids.at[index,'proxy_rs_id']= "No Proxy"


# ### Step 3: Create combined rsids having both curation and proxy_ rsids

# In[68]:


# create the combnd_rs_id column
QC_rsids["combnd_rs_id"]=''


# In[69]:


# Iterate over each row in df1
for index, row in QC_rsids.iterrows():
    if row['proxy_rs_id'] == "":
        QC_rsids.at[index, 'combnd_rs_id'] = row['cli_rs_id']
    else:
        QC_rsids.at[index, 'combnd_rs_id'] = row['proxy_rs_id']


# In[70]:


#print(QC_rsids)


# ### Step 4: Create unique_IDs

# In[71]:


# create the unique_rs_id column
QC_rsids["rs_uniq_id"]=''


# In[72]:


QC_rsids["trait_class"] = new_cur_mast["cur_trt_clss"]


# In[73]:


# Create a list to keep track of the count for each unique value in combnd_rs_id column
count_list = []


# In[74]:


## Initialize counter
no_proxy_count = 0

for index, row in QC_rsids.iterrows():
    combnd_rs_id = row["combnd_rs_id"]
    cli_rs_id = row["cli_rs_id"]
    trait_class = row["trait_class"]

    # Condition 1: If "combnd_rs_id" column has "No Proxy", fill "No Proxy" in "rs_uniq_id"
    if combnd_rs_id == "No Proxy":
        no_proxy_count += 1
        QC_rsids.at[index, "rs_uniq_id"] = "No Proxy" + str(no_proxy_count)

    else:
        # Condition 2: If "cli_rs_id_" column has 'NA', form unique id with "|T", "NA", and count of occurrence
        if cli_rs_id == "NA":
            count= count_list.count(combnd_rs_id) + 1
            count_list.append(combnd_rs_id)
            unique_id = f"{combnd_rs_id}|T{trait_class}|NA{count}"
            QC_rsids.at[index, "rs_uniq_id"] = unique_id

        # Condition 3: If "cli_rs_id_" column is not 'NA', form unique id with "|T" and count of occurrence
        else:
            count= count_list.count(combnd_rs_id) + 1
            count_list.append(combnd_rs_id)
            unique_id = f"{combnd_rs_id}|T{trait_class}|{count}"
            QC_rsids.at[index, "rs_uniq_id"] = unique_id


# In[75]:


#QC_rsids


# ### Step 5: Create first version of QC file (QC_file1)

# In[76]:


#Copy the contents of QC_rsids into QC_file1
QC_file1 = QC_rsids.copy()


# In[77]:


#Copy the direct contents of trait class, gene, effect and scoretype from curation into the QC_file1
QC_file1 = pd.concat([QC_file1,new_cur_mast[['cur_trt','cur_gene','cur_effect', 'cur_scortyp']]],axis =1)


# In[78]:


#Rename the columns as supposed to in QC_file1
QC_file1 = QC_file1.rename(columns={'cur_trt':'trait','cur_gene': 'gene','cur_effect':'effect_b_or_r','cur_scortyp':'scoretype'})


# ### Step 4 : Export the QC_rsids and QC_file1

# In[79]:


# Form a list of dataframes to be converted to parquet files
# dataframes = [QC_rsids,QC_file1]


# In[80]:


# Form a list of file names in parquet format
# filenames = ['QC_rsids.parquet','QC_file1.parquet']


# In[81]:


# Call the function to write the DataFrames to Parquet files
# write_dataframes_to_parquet(dataframes, filenames, glb_04_temp_input)


# # ******* END OF STAGE3 ********

# # Stage 4: Filling in Proxy, NA and VA into QC file2

# ### Step 1: Create Proxy column in QC file

# In[82]:


#Create a column called proxy in QC_file1
QC_file1.loc[:,'Proxy'] = ''


# In[83]:


# Apply the condition to create Proxy column
QC_file1.loc[QC_file1['proxy_rs_id'] =='','Proxy'] = "N"
QC_file1.loc[QC_file1['proxy_rs_id'] !='','Proxy'] = "Y"


# In[84]:


QC_file1.head(2)


# ### Step 2: NA or major allele and VA or minor allele in one column in QC_file

# ### a. Add the unique rsid column in curation_trait_match file

# In[85]:


cur_rs_trt_match = cur_rs_trt_match.copy()


# In[86]:


#Add the unique rsid column to curation_trait_match file
cur_rs_trt_match["cur_uniq_id"]= QC_file1["rs_uniq_id"]


# ### b. Create a new NA-VA dataframe with only major allele and minor allele
#  - NA-VA has only proxy NA and proxy VA for corresponding proxy_rsid

# In[87]:


# Create the NA_VA dataframe with the specified columns
NA_VA = pd.DataFrame(columns=['prxy_rs_id', 'rs_uniq_id', 'NA', 'VA'])


# - NA_VA: proxy_rsid column

# In[88]:


# Get the proxy rsids into NA_VA by filtering the client rsid with 'NA' from QC_file2
filtered_values = QC_file1.loc[QC_file1['cli_rs_id'] == 'NA','proxy_rs_id']
NA_VA['prxy_rs_id'] = filtered_values.reset_index(drop=True)


# In[89]:


NA_VA = NA_VA[NA_VA['prxy_rs_id'].notnull()]


#  - NA_VA: unique_rsid column

# In[90]:


# Get the unique rsids into NA_VA by filtering the client rsid with 'NA' from QC_file2
filtered_values = QC_file1.loc[QC_file1['cli_rs_id'] == 'NA', 'rs_uniq_id']
NA_VA['rs_uniq_id'] = filtered_values.reset_index(drop=True)


#  - NA_VA: NA column

# In[91]:


# Create a dictionary mapping 'prxy_prx_rs_id' to 'prxy_mjr_alle'
mapping_dict_na = clnt_prx_mtch1.set_index('prxy_prx_rs_id')['prxy_mjr_alle'].to_dict()


# In[92]:


# Map the values from 'proxy_rs_id' to create the 'NA' column in NA_VA DataFrame and replace NaN values with 'No Proxy'
NA_VA['NA'] = NA_VA['prxy_rs_id'].map(mapping_dict_na).fillna("No Proxy")


#  - NA_VA: NA column

# In[93]:


# Create a dictionary mapping 'prxy_prx_rs_id' to 'prxy_mjr_alle'
mapping_dict_va = clnt_prx_mtch1.set_index('prxy_prx_rs_id')['prxy_mnr_alle'].to_dict()


# In[94]:


# Map the values from 'proxy_rs_id' to create the 'VA' column in NA_VA DataFrame and replace NaN values with 'No Proxy'
NA_VA['VA'] = NA_VA['prxy_rs_id'].map(mapping_dict_va).fillna("No Proxy")


# ## c. Create a new non_NA-VA dataframe with only curation NA and VA
#  - non_NA-VA has all the non-proxy NA and non-proxy VA

# In[95]:


#Create nonprxy_na_va with specified columns
nonprxy_na_va = pd.DataFrame(columns=['cur_rs_id', 'rs_uniq_id', 'NA', 'VA'])


# - nonprxy_na_va: curation rsid column

# In[96]:


# Into the 'curation_rs_id' column of nonprxy_na_va, filtered values of client rsid without NA is added
filtered_values = QC_file1.loc[QC_file1['cli_rs_id'] != 'NA','cur_rs_id']
nonprxy_na_va['cur_rs_id'] = filtered_values.reset_index(drop=True)


# - nonprxy_na_va: unique_rsid column

# In[97]:


# Into the 'unique_id' column of nonprxy_na_va, filtered values of client rsid without NA is added
filtered_values = QC_file1.loc[QC_file1['cli_rs_id'] != 'NA','rs_uniq_id']
nonprxy_na_va['rs_uniq_id'] = filtered_values.reset_index(drop=True)


#  - nonprxy_na_va: NA column

# In[98]:


# Create a dictionary mapping 'cur_uniq_id' to 'cur_na'
mapping_dict_na1 = cur_rs_trt_match.set_index('cur_uniq_id')['cur_na'].to_dict()


# In[99]:


# Map the values from 'rs_uniq_id' to create the 'na' column in nonprxy_na_va DataFrame and replace NaN values with 'No Proxy'
nonprxy_na_va['NA'] = nonprxy_na_va['rs_uniq_id'].map(mapping_dict_na1).fillna("No Proxy")


#  - nonprxy_na_va: VA column

# In[100]:


# Create a dictionary mapping 'cur_uniq_id' to 'cur_na'
mapping_dict_va1 = cur_rs_trt_match.set_index('cur_uniq_id')['cur_va'].to_dict()


# In[101]:


# Map the values from 'rs_uniq_id' to create the 'na' column in nonprxy_na_va DataFrame and replace NaN values with 'No Proxy'
nonprxy_na_va['VA'] = nonprxy_na_va['rs_uniq_id'].map(mapping_dict_va1).fillna("No Proxy")


# ### d. Create another dataframe 'NA_VA_combi' that concatenates all the uniq ids in na_va and nonprxy_na_va and their corresponding NA and VA

# In[102]:


# Concatenate the unique IDs and corresponding NA and VA values from both na_va and nonprxy_na_va dataframes
NA_VA_combi = pd.concat([nonprxy_na_va[['rs_uniq_id', 'NA', 'VA']], NA_VA[['rs_uniq_id', 'NA', 'VA']]], ignore_index=True)


# ### e. Enter the values of NA and VA from NA_VA combi dataframe into QC_file1 dataframe

# In[103]:


merge_on_columns = ["rs_uniq_id"]


# In[104]:


# Call the function to merge the DataFrames
QC_file1  = merge_dataframes_same_colnames(QC_file1, NA_VA_combi[['rs_uniq_id', 'NA', 'VA']], merge_on_columns, how='left')


# In[105]:


#QC_file1.head(10)


# # ******* END OF STAGE4 ********

# # Stage 5: Create Complement column

# In[106]:


#os.chdir(glb_05_mapping_files)


# #### input of tbl_lup_complement from mapping _files folder

# In[107]:


file_name = "tbl_lup_complement.csv"
file_path = os.path.join(glb_05_mapping_files, file_name)


# In[108]:


tbl_lup_complement = pd.read_csv(file_path, engine='python')


# In[109]:


# Create a new column 'complement' in QC_files2 by looking up for concatenated NA and VA in tbl_lup_complement
QC_file1['complement'] = QC_file1.apply(lambda row: tbl_lup_complement.loc[(tbl_lup_complement['NA/VA'] == row['NA'] + row['VA']), 'Value'].values[0] if any(tbl_lup_complement['NA/VA'] == row['NA'] + row['VA']) else 'N', axis=1)


# # ******* END OF STAGE5 ********

# # Stage 6: Genotype and Alleles columns into QC_file1

# In[110]:


print(left_on_columns)
left_on = "combnd_rs_id"


# In[111]:


right_on = "cli_rs_id"


# In[112]:


# QC_file1.head()


# In[113]:


# Reset the index of 'new_cli_rw' DataFrame before performing the merge
# new_clnt_rw_reset = new_clnt_rw.reset_index().rename(columns={'cli_rs_id': 'new_cli_rs_id'})


# In[114]:


#new_clnt_rw[['cli_geno']]


# In[115]:


left_on_columns


# In[116]:


right_on_columns


# In[117]:


result_df = merge_dataframes_differ_colnames(QC_file1, new_clnt_rw, left_on, right_on, how='left')


# In[118]:


QC_file1['genotype'] = result_df['cli_geno'].fillna("No Proxy")


# In[119]:


#QC_file1


# In[120]:


# Create new allele1 column by extracting first character
QC_file1['allele_1'] = QC_file1['genotype'].str[0]


# In[121]:


# Create new allele2 column by extracting second character
QC_file1['allele_2'] = QC_file1['genotype'].str[1]


# In[122]:


# Replace 'No Proxy' with 'NA'
QC_file1.loc[(QC_file1['genotype'] == 'No Proxy')| (QC_file1['genotype'] =='--'), ['allele_1', 'allele_2']] = 'NA'


# # ******* END OF STAGE6 ********

# # Stage 7:Switching: switch and va_switched columns in QC_file1

# ### Step 1: Switch column

# ##### input tbl_lup_switch file

# In[123]:


file_name = "tbl_lup_switch.csv"
file_path = os.path.join(glb_05_mapping_files, file_name)


# In[124]:


tbl_lup_switch = pd.read_csv(file_path, engine='python')


# In[125]:


# Lookup function to check if concatenated value exists in tbl_lup_switch
def lookup_switch(row):
    concatenated_value = f"{row['VA']}_{row['complement']}_{row['allele_1']}_{row['allele_2']}"
    if concatenated_value in tbl_lup_switch['Switch'].values:
        return 'Y'
    else:
        return 'N'


# In[126]:


# Create a new column 'switch' in QC_file3 based on the lookup function
QC_file1['switch'] = QC_file1.apply(lookup_switch, axis=1)


# ### Step 2: VA_switched column

# ##### input tbl_lup_swi_va

# In[127]:


file_name = "tbl_lup_swi_va.csv"
file_path = os.path.join(glb_05_mapping_files, file_name)


# In[128]:


tbl_lup_swi_va = pd.read_csv(file_path, engine='python')


# In[129]:


tbl_lup_swi_va


# In[130]:


left_on = QC_file1['switch'] + QC_file1['VA']


# In[131]:


right_on='Switch_VA'


# In[132]:


# Merge the dataframes based on concatenated 'switch' and 'VA' columns
merged_df = merge_dataframes_differ_colnames(QC_file1, tbl_lup_swi_va, left_on, right_on, how='left')


# In[133]:


# Create the 'va_switched' column in QC_file3 based on the merge result
QC_file1['va_switched'] = merged_df['Value'].fillna('NA')


# ### Step 3: Creating combnd_va in QC_file1

# In[134]:


# Create the 'combnd_va' column in QC_file3 based on 'va_switched' and 'VA' values
QC_file1['combnd_va'] = QC_file1.apply(lambda x: x['VA'] if x['va_switched'] == 'NA' else x['va_switched'], axis=1)


# # ******* END OF STAGE7 ********

# ### Step 1: Create condition columns for scores in QC_file1

# In[135]:


# Create the 'VA_A1' column in QC_file1 based on 'va_switched' and 'VA' values
QC_file1['VA_A1'] = QC_file1.apply(lambda x: 1 if x['VA'] == x['allele_1'] else 0, axis=1)


# In[136]:


# Create the 'VA_A2' column in QC_file1 based on 'va_switched' and 'VA' values
QC_file1['VA_A2'] = QC_file1.apply(lambda x: 1 if x['VA'] == x['allele_2'] else 0, axis=1)


# In[137]:


# Create the 'VA_A2' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['A1_A2'] = QC_file1.apply(lambda x: 1 if x['allele_1'] == x['allele_2'] else 0, axis=1)


# In[138]:


# Create the 'VA_NoPrxy' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['VA_NoPrxy'] = QC_file1.apply(lambda x: 1 if x['VA'] == "No Proxy" else 0, axis=1)


# In[139]:


# Create the 'Geno_--' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['Geno_--'] = QC_file1.apply(lambda x: 1 if x['genotype'] == "--" else 0, axis=1)


# In[140]:


# Create the 'eff_R' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['eff_R'] = QC_file1.apply(lambda x: 1 if x['effect_b_or_r'] == "R" else 0, axis=1)


# In[141]:


# Create the 'eff_B' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['eff_B'] = QC_file1.apply(lambda x: 1 if x['effect_b_or_r'] == "B" else 0, axis=1)


# In[142]:


# Create the 'swi_y' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['swi_y'] = QC_file1.apply(lambda x: 1 if x['switch'] == "Y" else 0, axis=1)


# In[143]:


# Create the 'sco_ty_x' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['sco_ty_x'] = QC_file1.apply(lambda x: 1 if x['scoretype'] == "X" else 0, axis=1)


# In[144]:


# Create the 'sco_ty_y' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['sco_ty_y'] = QC_file1.apply(lambda x: 1 if x['scoretype'] == "Y" else 0, axis=1)


# In[145]:


# Create the 'sco_ty_z' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file1['sco_ty_z'] = QC_file1.apply(lambda x: 1 if x['scoretype'] == "Z" else 0, axis=1)


# In[146]:


file_name = "tbl_lup_scores.csv"
file_path = os.path.join(glb_05_mapping_files, file_name)


# In[147]:


tbl_lup_scores = pd.read_csv(file_path, engine='python')


# ### Step 2: Creation of score column

# In[148]:


QC_file1['combnd_score'] = QC_file1.apply(lambda row: tbl_lup_scores.loc[(tbl_lup_scores['Lup'] == "SK" + str(row['VA_A1']) + str(row['VA_A2']) + str(row['A1_A2']) + str(row['VA_NoPrxy']) + str(row['Geno_--']) + str(row['eff_R']) + str(row['eff_B']) + str(row['swi_y']) + str(row['sco_ty_x']) + str(row['sco_ty_y']) + str(row['sco_ty_z'])), 'value'].values[0] if any(tbl_lup_scores['Lup'] == "SK" + str(row['VA_A1']) + str(row['VA_A2']) + str(row['A1_A2']) + str(row['VA_NoPrxy']) + str(row['Geno_--']) + str(row['eff_R']) + str(row['eff_B']) + str(row['swi_y']) + str(row['sco_ty_x']) + str(row['sco_ty_y']) + str(row['sco_ty_z'])) else 'X', axis=1)


# In[149]:


#QC_file1['combnd_score']


# In[150]:


#QC_file1.head(3)


# # ******* END OF STAGE8 ********

# ### Step 1: Average score for each trait class

# ##### Create QC_file2 having trait information without duplicates

# In[151]:


columns = ['trait_class', 'trait']


# In[152]:


QC_file2= pd.DataFrame(columns=columns)


# In[153]:


#QC_file2


# In[154]:


QC_file2 = remove_duplicates_and_create_column(QC_file1, 'trait_class', QC_file2, 'trait_class')


# In[155]:


QC_file2 = QC_file2.sort_values(by='trait_class', ascending=True)


# In[156]:


QC_file2  = remove_duplicates_and_create_column(QC_file1, 'trait', QC_file2, 'trait')


# In[157]:


#QC_file2


# ##### Calculate mean score for each trait class and implement it into QC_file2

# In[158]:


# Check for non-numeric values in the combnd_score column of QC_file1_df
QC_file1['combnd_score'] = pd.to_numeric(QC_file1['combnd_score'], errors='coerce')


# In[159]:


# Calculate the mean of 'combnd_score' for each 'trait_class' in QC_file1
mean_score = QC_file1.groupby('trait_class')['combnd_score'].mean().reset_index()


# In[160]:


on_columns=['trait_class']


# In[161]:


#Merge QC_file2 to mean_score
QC_file2 = merge_dataframes_same_colnames(QC_file2, mean_score, on_columns, how='left')


# In[162]:


# Rename combnd score column in QC_file2 to mean_score
QC_file2 = QC_file2.rename(columns={'combnd_score': 'mean_score'})


# In[163]:


print(QC_file2)

####################################################
# Stage 3: Creating of combined rsid and direct inputs from "Curation master"
####################################################
#import the cur_mast1 from stage 1
new_cur_mast1=pd.read_parquet(glb_04_temp_input + '\\new_cur_mast.parquet')
#import the new_clnt_rw from stage 1
new_clnt_rw=pd.read_parquet(glb_04_temp_input + '\\new_clnt_rw.parquet')
#import the clnt_prx_mtch1 from stage 2
clnt_prx_mtch1=pd.read_parquet(glb_04_temp_input + '\\clnt_prx_mtch1.parquet')
#Step 2: Match rsids in Client raw file with rsids in Curation master
# Bring in the "curation rsid" column from the Curation master into a new df "Cur_cli_rsid_mtch"
Cur_cli_rsid_mtch=new_cur_mast1["cur_rs_id"]
#Merge the client rs_id to the curation rsid into the file and bring every corresponding value of curation master
Cur_cli_rsid_mtch = pd.merge(new_cur_mast1, new_clnt_rw, left_on = "cur_rs_id", right_on = "cli_rs_id", how= "left")
#Create a dataframe QC_rsids with only the rsid columns from Cur_cli_rsid_match
QC_rsids = Cur_cli_rsid_mtch[["cur_rs_id", "cli_rs_id"]]
#Fill with "NA" where ever the match between rsids were not found
QC_rsids= QC_rsids.fillna("NA")
#create the proxy_rs_id column
QC_rsids["proxy_rs_id"]=''
# Iterate over each row in QC_rsids
for index, row in QC_rsids.iterrows():
    # Check if client_rs_id value is "NA"
    if row['cli_rs_id'] == 'NA':
        Curation_rs_id = row['cur_rs_id']
        # Look up corresponding value of Curation_rs_id in prxy_rs_id of clnt_prx_mtch1
        match_value = clnt_prx_mtch1.loc[clnt_prx_mtch1['prxy_rs_id']== Curation_rs_id, "prxy_prx_rs_id"].values
        # If a match is found, assign corresponding value from prxy_prx_rs_id of clnt_prx_mtch1 to proxy_rs_id of QC_rsids
        if len(match_value) >0:
            QC_rsids.at[index,'proxy_rs_id']= match_value[0]
        else:
            QC_rsids.at[index,'proxy_rs_id']= "No Proxy"

# create the proxy_rs_id column
QC_rsids["combnd_rs_id"]=''
# Iterate over each row in df1
for index, row in QC_rsids.iterrows():
    if row['proxy_rs_id'] == "":
        QC_rsids.at[index, 'combnd_rs_id'] = row['cli_rs_id']
    else:
        QC_rsids.at[index, 'combnd_rs_id'] = row['proxy_rs_id']

#Step 4: Create unique_IDs
# create the unique_rs_id column
QC_rsids["rs_uniq_id"]=''
QC_rsids["trait_class"] = new_cur_mast1["cur_trt_clss"]
# Create a list to keep track of the count for each unique value in combnd_rs_id column
count_list = []

## Initialize counter
no_proxy_count = 0

for index, row in QC_rsids.iterrows():
    combnd_rs_id = row["combnd_rs_id"]
    cli_rs_id = row["cli_rs_id"]
    trait_class = row["trait_class"]

    # Condition 1: If "combnd_rs_id" column has "No Proxy", fill "No Proxy" in "rs_uniq_id"
    if combnd_rs_id == "No Proxy":
        no_proxy_count += 1
        QC_rsids.at[index, "rs_uniq_id"] = "No Proxy" + str(no_proxy_count)

    else:
        # Condition 2: If "cli_rs_id_" column has 'NA', form unique id with "|T", "NA", and count of occurrence
        if cli_rs_id == "NA":
            count= count_list.count(combnd_rs_id) + 1
            count_list.append(combnd_rs_id)
            unique_id = f"{combnd_rs_id}|T{trait_class}|NA{count}"
            QC_rsids.at[index, "rs_uniq_id"] = unique_id

        # Condition 3: If "cli_rs_id_" column is not 'NA', form unique id with "|T" and count of occurrence
        else:
            count= count_list.count(combnd_rs_id) + 1
            count_list.append(combnd_rs_id)
            unique_id = f"{combnd_rs_id}|T{trait_class}|{count}"
            QC_rsids.at[index, "rs_uniq_id"] = unique_id

#Copy the contents of QC_rsids into QC_file1
QC_file1 = QC_rsids.copy()
#Copy the direct contents of trait class, gene, effect and scoretype from curation into the QC_file1
QC_file1 = pd.concat([QC_file1,new_cur_mast1[['cur_trt','cur_gene','cur_effect', 'cur_scortyp']]],axis =1)
#Rename the columns as supposed to in QC_file1
QC_file1 = QC_file1.rename(columns={'cur_trt':'trait','cur_gene': 'gene','cur_effect':'effect_b_or_r','cur_scortyp':'scoretype'})
# the QC_rsids dataframe is exported into the temp input folder
QC_rsids.to_parquet(glb_04_temp_input + '\\QC_rsids.parquet', index=False)
# the QC_traitdata dataframe is exported into the temp input folder
QC_file1.to_parquet(glb_04_temp_input + '\\QC_file1.parquet', index=False)

###############Stage 4: Filling in Proxy, NA and VA into QC file2
#import the clent_proxy_match from stage 2
clnt_prx_mtch1=pd.read_parquet(glb_04_temp_input + '\\clnt_prx_mtch1.parquet')
#import the QC_file1 dataframe from stage 3
QC_file1 = pd.read_parquet(glb_04_temp_input + '\\QC_file1.parquet')
#import the cur_rs_trt_match from stage 2
cur_rs_trt_match=pd.read_parquet(glb_04_temp_input + '\\cur_rs_trt_match.parquet')
QC_file2 = QC_file1.copy()
#Create a column called proxy in QC_file2
QC_file2.loc[:,'Proxy'] = ''
# Apply the condition to create Proxy column
QC_file2.loc[QC_file2['proxy_rs_id'] =='','Proxy'] = "N"
QC_file2.loc[QC_file2['proxy_rs_id'] !='','Proxy'] = "Y"
#Add the unique rsid column to curation_trait_match file
cur_rs_trt_match["cur_uniq_id"]= QC_file2["rs_uniq_id"]
# Create the NA_VA dataframe with the specified columns
NA_VA = pd.DataFrame(columns=['prxy_rs_id', 'rs_uniq_id', 'NA', 'VA'])
# Get the proxy rsids into NA_VA by filtering the client rsid with 'NA' from QC_file2
filtered_values = QC_file2.loc[QC_file2['cli_rs_id'] == 'NA','proxy_rs_id']
NA_VA['prxy_rs_id'] = filtered_values.reset_index(drop=True)
NA_VA = NA_VA[NA_VA['prxy_rs_id'].notnull()]
# Get the unique rsids into NA_VA by filtering the client rsid with 'NA' from QC_file2
filtered_values = QC_file2.loc[QC_file2['cli_rs_id'] == 'NA', 'rs_uniq_id']
NA_VA['rs_uniq_id'] = filtered_values.reset_index(drop=True)
# Create a dictionary mapping 'prxy_prx_rs_id' to 'prxy_mjr_alle'
mapping_dict_na = clnt_prx_mtch1.set_index('prxy_prx_rs_id')['prxy_mjr_alle'].to_dict()
# Map the values from 'proxy_rs_id' to create the 'NA' column in NA_VA DataFrame and replace NaN values with 'No Proxy'
NA_VA['NA'] = NA_VA['prxy_rs_id'].map(mapping_dict_na).fillna("No Proxy")
# Create a dictionary mapping 'prxy_prx_rs_id' to 'prxy_mjr_alle'
mapping_dict_va = clnt_prx_mtch1.set_index('prxy_prx_rs_id')['prxy_mnr_alle'].to_dict()
# Map the values from 'proxy_rs_id' to create the 'VA' column in NA_VA DataFrame and replace NaN values with 'No Proxy'
NA_VA['VA'] = NA_VA['prxy_rs_id'].map(mapping_dict_va).fillna("No Proxy")
#Create nonprxy_na_va with specified columns
nonprxy_na_va = pd.DataFrame(columns=['cur_rs_id', 'rs_uniq_id', 'NA', 'VA'])
# Into the 'curation_rs_id' column of nonprxy_na_va, filtered values of client rsid without NA is added
filtered_values = QC_file2.loc[QC_file2['cli_rs_id'] != 'NA','cur_rs_id']
nonprxy_na_va['cur_rs_id'] = filtered_values.reset_index(drop=True)
# Into the 'unique_id' column of nonprxy_na_va, filtered values of client rsid without NA is added
filtered_values = QC_file1.loc[QC_file2['cli_rs_id'] != 'NA','rs_uniq_id']
nonprxy_na_va['rs_uniq_id'] = filtered_values.reset_index(drop=True)
# Create a dictionary mapping 'cur_uniq_id' to 'cur_na'
mapping_dict_na1 = cur_rs_trt_match.set_index('cur_uniq_id')['cur_na'].to_dict()
# Map the values from 'rs_uniq_id' to create the 'na' column in nonprxy_na_va DataFrame and replace NaN values with 'No Proxy'
nonprxy_na_va['NA'] = nonprxy_na_va['rs_uniq_id'].map(mapping_dict_na1).fillna("No Proxy")
# Create a dictionary mapping 'cur_uniq_id' to 'cur_na'
mapping_dict_va1 = cur_rs_trt_match.set_index('cur_uniq_id')['cur_va'].to_dict()
# Map the values from 'rs_uniq_id' to create the 'na' column in nonprxy_na_va DataFrame and replace NaN values with 'No Proxy'
nonprxy_na_va['VA'] = nonprxy_na_va['rs_uniq_id'].map(mapping_dict_va1).fillna("No Proxy")
# Concatenate the unique IDs and corresponding NA and VA values from both na_va and nonprxy_na_va dataframes
NA_VA_combi = pd.concat([nonprxy_na_va[['rs_uniq_id', 'NA', 'VA']], NA_VA[['rs_uniq_id', 'NA', 'VA']]], ignore_index=True)
# Merge NA_VA_combi with QCfile2 based on 'cur_trt_clss' and 'rs_trtcls' columns
merged_df = pd.merge(QC_file2, NA_VA_combi[['rs_uniq_id', 'NA', 'VA']], on='rs_uniq_id', how='left')
QC_file2['NA'] = merged_df['NA']
QC_file2['VA'] = merged_df['VA']
# the QC_file2 dataframe is exported into the temp input folder
QC_file2.to_parquet(glb_04_temp_input + '\\QC_file2.parquet', index=False)
################Stage 5: Create Complement column
#import 'tbl_lup_complement' sheet from Caffeine Master into cur_mast datafame
tbl_lup_complement=pd.read_csv(glb_05_mapping_files + '\\tbl_lup_complement.csv')
#import the QC_file2 dataframe from stage 3
QC_file2 = pd.read_parquet(glb_04_temp_input + '\\QC_file2.parquet')
# Create a new column 'complement' in QC_files2 by looking up for concatenated NA and VA in tbl_lup_complement
QC_file2['complement'] = QC_file2.apply(lambda row: tbl_lup_complement.loc[(tbl_lup_complement['NA/VA'] == row['NA'] + row['VA']), 'Value'].values[0] if any(tbl_lup_complement['NA/VA'] == row['NA'] + row['VA']) else 'N', axis=1)
# the QC_file1 dataframe is exported into the temp input folder
QC_file2.to_parquet(glb_04_temp_input + '\\QC_file2.parquet', index=False)
###############Stage 6: Genotype and Alleles columns into QC_file2
#import the new_clnt_rw from stage 1
new_clnt_rw=pd.read_parquet(glb_04_temp_input + '\\new_clnt_rw.parquet')
#import the QC_file2 from stage 5
QC_file2 = pd.read_parquet(glb_04_temp_input + '\\QC_file2.parquet')
#Rename QC_file2 to QC_file3
QC_file3 = QC_file2.copy()
### Merge the dataframes(QC_file3 and client file) based on the matching client rs_ids
merged_df = QC_file3.merge(new_clnt_rw[['cli_rs_id','cli_geno']], left_on='combnd_rs_id', right_on='cli_rs_id', how='left')
# Create a new column 'genotype' in QCFile1 and fill it with corresponding cli_geno values
QC_file3['genotype'] = merged_df['cli_geno'].fillna("No Proxy")
# Create new allele1 column by extracting first character
QC_file3['allele_1'] = QC_file3['genotype'].str[0]
# Create new allele2 column by extracting second character
QC_file3['allele_2'] = QC_file3['genotype'].str[1]
# Replace 'No Proxy' with 'NA'
QC_file3.loc[(QC_file3['genotype'] == 'No Proxy')| (QC_file3['genotype'] =='--'), ['allele_1', 'allele_2']] = 'NA'
# the QC_file1 dataframe is exported into the temp input folder
QC_file3.to_parquet(glb_04_temp_input + '\\QC_file3.parquet', index=False)
################Stage 7:Switching: switch and va_switched columns in QC_file3
#import QC_file3
QC_file3 = pd.read_parquet(glb_04_temp_input + '\\QC_file3.parquet')
#import 'tbl_lup_complement' sheet from Caffeine Master into cur_mast datafame
tbl_lup_switch=pd.read_csv(glb_05_mapping_files + '\\tbl_lup_switch.csv')
# Lookup function to check if concatenated value exists in tbl_lup_switch
def lookup_switch(row):
    concatenated_value = f"{row['VA']}_{row['complement']}_{row['allele_1']}_{row['allele_2']}"
    if concatenated_value in tbl_lup_switch['Switch'].values:
        return 'Y'
    else:
        return 'N'

# Create a new column 'switch' in QC_file3 based on the lookup function
QC_file3['switch'] = QC_file3.apply(lookup_switch, axis=1)
#import 'tbl_lup_swi_va' file for creating va_switched column
tbl_lup_swi_va=pd.read_csv(glb_05_mapping_files + '\\tbl_lup_swi_va.csv')
# Merge the dataframes based on concatenated 'switch' and 'VA' columns
merged_df = pd.merge(QC_file3, tbl_lup_swi_va, left_on=(QC_file3['switch'] + QC_file3['VA']), right_on='Switch_VA', how='left')
# Create the 'va_switched' column in QC_file3 based on the merge result
QC_file3['va_switched'] = merged_df['Value'].fillna('NA')
# Create the 'combnd_va' column in QC_file3 based on 'va_switched' and 'VA' values
QC_file3['combnd_va'] = QC_file3.apply(lambda x: x['VA'] if x['va_switched'] == 'NA' else x['va_switched'], axis=1)
# the QC_file3 dataframe is exported into the temp input folder
QC_file3.to_parquet(glb_04_temp_input + '\\QC_file3.parquet', index=False)
##########Stage 8: Generating scores
#import the QC_file3 from stage 7
QC_file3 = pd.read_parquet(glb_04_temp_input + '\\QC_file3.parquet')
#Make a copy of QC_file3 in QC_file4
QC_file4 = QC_file3.copy()
# Create the 'VA_A1' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['VA_A1'] = QC_file4.apply(lambda x: 1 if x['VA'] == x['allele_1'] else 0, axis=1)
# Create the 'VA_A2' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['VA_A2'] = QC_file4.apply(lambda x: 1 if x['VA'] == x['allele_2'] else 0, axis=1)
# Create the 'VA_A2' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['A1_A2'] = QC_file4.apply(lambda x: 1 if x['allele_1'] == x['allele_2'] else 0, axis=1)
# Create the 'VA_NoPrxy' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['VA_NoPrxy'] = QC_file4.apply(lambda x: 1 if x['VA'] == "No Proxy" else 0, axis=1)
# Create the 'Geno_--' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['Geno_--'] = QC_file4.apply(lambda x: 1 if x['genotype'] == "--" else 0, axis=1)
# Create the 'eff_R' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['eff_R'] = QC_file4.apply(lambda x: 1 if x['effect_b_or_r'] == "R" else 0, axis=1)
# Create the 'eff_B' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['eff_B'] = QC_file4.apply(lambda x: 1 if x['effect_b_or_r'] == "B" else 0, axis=1)
# Create the 'swi_y' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['swi_y'] = QC_file4.apply(lambda x: 1 if x['switch'] == "Y" else 0, axis=1)
# Create the 'sco_ty_x' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['sco_ty_x'] = QC_file4.apply(lambda x: 1 if x['scoretype'] == "X" else 0, axis=1)
# Create the 'sco_ty_y' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['sco_ty_y'] = QC_file4.apply(lambda x: 1 if x['scoretype'] == "Y" else 0, axis=1)
# Create the 'sco_ty_z' column in QC_file4 based on 'va_switched' and 'VA' values
QC_file4['sco_ty_z'] = QC_file4.apply(lambda x: 1 if x['scoretype'] == "Z" else 0, axis=1)
#import 'tbl_lup_scores'
tbl_lup_scores=pd.read_csv(glb_05_mapping_files + '\\tbl_lup_scores.csv')
QC_file4['combnd_score'] = QC_file4.apply(lambda row: tbl_lup_scores.loc[(tbl_lup_scores['Lup'] == "SK" + str(row['VA_A1']) + str(row['VA_A2']) + str(row['A1_A2']) + str(row['VA_NoPrxy']) + str(row['Geno_--']) + str(row['eff_R']) + str(row['eff_B']) + str(row['swi_y']) + str(row['sco_ty_x']) + str(row['sco_ty_y']) + str(row['sco_ty_z'])), 'value'].values[0] if any(tbl_lup_scores['Lup'] == "SK" + str(row['VA_A1']) + str(row['VA_A2']) + str(row['A1_A2']) + str(row['VA_NoPrxy']) + str(row['Geno_--']) + str(row['eff_R']) + str(row['eff_B']) + str(row['swi_y']) + str(row['sco_ty_x']) + str(row['sco_ty_y']) + str(row['sco_ty_z'])) else 'X', axis=1)
# the QC_file3 dataframe is exported into the temp input folder
QC_file4.to_parquet(glb_04_temp_input + '\\QC_file4.parquet', index=False)
QC_file4.to_csv(glb_04_temp_input + '\\QC_file4.csv', index=False)
#####################Stage 9: Generate mean score in QC_file5
#import the QC_file4 from stage 8
QC_file4 = pd.read_parquet(glb_04_temp_input + '\\QC_file4.parquet')
#Get a list of the columns trait and trait class
columns = ['trait_class', 'trait']
#Create a new dataframe named QC_file5
QC_file5= pd.DataFrame(columns=columns)

def remove_duplicates_and_create_column(source_df, source_column_name, target_df, target_column_name):
    unique_values = source_df.drop_duplicates(subset=source_column_name, keep='first')[source_column_name]
    target_df[target_column_name] = unique_values.reset_index(drop=True)
    return target_df

QC_file5 = remove_duplicates_and_create_column(QC_file4, 'trait_class', QC_file5, 'trait_class')
QC_file5 = QC_file5.sort_values(by='trait_class', ascending=True)
QC_file5  = remove_duplicates_and_create_column(QC_file4, 'trait', QC_file5, 'trait')
##Calculate mean score for each trait class and implement it into QC_file5
# Check for non-numeric values in the combnd_score column of QC_file1_df
QC_file5['combnd_score'] = pd.to_numeric(QC_file4['combnd_score'], errors='coerce')
# Calculate the mean of 'combnd_score' for each 'trait_class' in QC_file1
mean_score = QC_file5.groupby('trait_class')['combnd_score'].mean().reset_index()

def merge_dataframes_same_colnames(left_df, right_df, on_columns, how='left', keep_only_left=True):
    merged_df = pd.merge(left_df, right_df, on=on_columns, how=how, suffixes=(None, '_y' if keep_only_left else '_x'))

    if not keep_only_left:
        # Rename columns from the right DataFrame to match the left DataFrame
        suffix = '_x'
        for col in right_df.columns:
            if col in on_columns:
                continue
            new_col = col + suffix
            while new_col in merged_df.columns:
                suffix += '_'
                new_col = col + suffix
            merged_df.rename(columns={col: new_col}, inplace=True)

    return merged_df

on_columns=['trait_class']
#Merge QC_file2 to mean_score
QC_file5 = merge_dataframes_same_colnames(QC_file5, mean_score, on_columns, how='left')
# Rename combnd score column in QC_file2 to mean_score
QC_file5 = QC_file5.rename(columns={'combnd_score': 'mean_score'})
QC_file5.drop('combnd_score_y', axis =1, inplace = True)
# the QC_file3 dataframe is exported into the temp input folder
QC_file5.to_parquet(glb_04_temp_input + '\\QC_file5.parquet', index=False)
###############Stage 10: Generate Outcomes and Recommendations
#import the QC_file4 from stage 8
QC_file4 = pd.read_parquet(glb_04_temp_input + '\\QC_file4.parquet')
#import the QC_file5 from stage 9
QC_file5 = pd.read_parquet(glb_04_temp_input + '\\QC_file5.parquet')
#import 'Curation' sheet from Caffeine Master into cur_mast datafame
outcome_recommd = pd.read_excel(glb_02_master + '\\Caffeine_Master.xlsx', sheet_name = "Outcomes and Recommendations")
# Add the "trait_class" column
outcome_recommd.insert(0, 'trait_class', range(1, 13))
# Columns to transform with hardcoded bounds
cols_to_transform = {
    "0.66 (slow metabolizer - high risk)": (0, 0.66),
    "0.67-1.33 (moderate metabolizer - moderate risk)": (0.67, 1.33),
    "1.34 (fast metabolizer - low risk)": (1.34, 10)
}

new_data = []

# iterate over each specified column
for col, bounds in cols_to_transform.items():
    # extract the outcomes
    outcome = re.findall(r"\((.*?)\)", col)[0]

    # extract the hardcoded bounds
    lower, upper = bounds

    # iterate over each row in the column
    for i in outcome_recommd.index:
        trait_class = outcome_recommd.loc[i, 'trait_class']
        recommendation = outcome_recommd.loc[i, col]
        trait_name = outcome_recommd.loc[i, 'trait name']  # assuming 'trait_name' is the other column you want to keep
        description = outcome_recommd.loc[i, 'trait description']
        genes_analyzed = outcome_recommd.loc[i, 'genes']
        gene_markers_analyzed = outcome_recommd.loc[i, 'num_markers']

        # append the extracted data to new_data
        new_data.append([trait_class, lower, upper, outcome, recommendation, trait_name, description, genes_analyzed, gene_markers_analyzed])

# create a new DataFrame from new_data
mapping_df = pd.DataFrame(new_data, columns=['trait_class', 'ref_score_lower', 'ref_score_upper', 'outcomes', 'recommendations', 'trait_name','description', 'genes_analyzed', 'gene_markers_analyzed'])

# sort the DataFrame based on 'trait class' and reset the index
mapping_df = mapping_df.sort_values(by='trait_class').reset_index(drop=True)

# Extracting the text
pattern = r'\] (.*?)(?= =)|\](?:[^\:]*\:)([^\:]*)(?=:)'
# pattern = r'\] (.*?)(?= =)|\](?:[^\:]*\:)([^\:]*)(?=:)|\] ([^.=]+)(?=[.=])'
mapping_df['Subheading'] = mapping_df['recommendations'].str.extract(pattern).fillna('').sum(axis=1).str.strip()

value_map = {'Caffeine sensitivity': 'Caffeine metabolism', 'Sprint activity/ Sprinting performance-enhancing effects of caffeine': 'Physical performance and caffeine', 'caffeine and appetite': 'Caffeine and appetite', 'High consumption':'Caffeine overconsumption'}
QC_file5['trait'] = QC_file5['trait'].replace(to_replace=value_map)
QC_file6 = QC_file5.copy()

def assign_values(row):
    # Filter mapping_df for the specific trait_class
    subset = mapping_df[mapping_df['trait_class'] == row['trait_class']]

    # Check if the mean_score falls within the range for any of the rows in the subset
    mask = (subset['ref_score_lower'] <= row['mean_score']) & (subset['ref_score_upper'] >= row['mean_score'])

    # If there's a match, return the corresponding outcome
    if mask.sum() > 0:
        matched_row = subset[mask].iloc[0]
        return pd.Series({
            'recommendations': matched_row['recommendations'],
            'description': matched_row['description'],
            'genes_analyzed': matched_row['genes_analyzed'],
            'gene_markers_analyzed': matched_row['gene_markers_analyzed'],
            'Subheading': matched_row['Subheading']
            })
    else:
        return pd.Series({
            'recommendations': np.nan,
            'description': np.nan,
            'genes_analyzed': np.nan,
            'gene_markers_analyzed': np.nan,
            'Subheading': np.nan
        })

outputs = QC_file6.apply(assign_values, axis=1)
QC_file6 = pd.concat([QC_file6, outputs], axis=1)
QC_file4["cli_count"] = np.where(QC_file4['cli_rs_id'] !='NA',1, 0)
agg_data = QC_file4.groupby('trait_class')['cli_count'].sum().reset_index()
QC_file6 = pd.merge(QC_file6, agg_data, on='trait_class', how='left')
QC_file6.rename(columns={'cli_count': 'cli_markers'}, inplace=True)
# the QC_file3 dataframe is exported into the temp input folder
QC_file6.to_parquet(glb_04_temp_input + '\\QC_file5.parquet', index=False)







#####################################################
#####################################################
#REPORT GENERATION
#####################################################
#####################################################
QC_file6= pd.read_parquet(glb_04_temp_input + '\\QC_file5.parquet')

print (QC_file6['Subheading'][0])



# Assuming QC_file5 is your DataFrame and 'recommendations' is your column name
QC_file6['recommendations'] = QC_file6['recommendations'].apply(remove_before_angle_bracket)
df = QC_file6

# Read the TOC template from the file
with open(glb_template_root + "\\toc_template.html", "r") as file:
    original_toc_template = file.read()



#Function to process summary pages as html

def process_all_files():
    files = [glb_template_root + "\\Batch_2_1_v3.htm", glb_template_root + "\\Batch_2_2_v3.htm", glb_template_root + "\\Batch_2_3_v3.htm", glb_template_root + "\\Batch_2_4_v3.htm"]

    # This assumes you always process 3 rows for each file. Adjust as necessary.
    chunks = [df.iloc[i:i+3] for i in range(0, len(df), 3)]

    for file_name, rows_to_process in zip(files, chunks):
        gen_summary_for_file(file_name, rows_to_process)

######### Create new allout html
with open(glb_03_output + "\\allout.html", "w", encoding='utf-8') as file:
    file.write(original_toc_template)
    file.write(" ")

process_all_files()

# Read the template from the file
with open(glb_template_root + "\\report_long_template.htm", "r") as file:
    original_template = file.read()

# Read the Disclaimer template from the file
with open(glb_template_root + "\\disclaimer_template.html", "r", encoding="utf-8") as file:
    original_disclaimer_template = file.read()


from bs4 import BeautifulSoup

# Font sizes - you can adjust these values as per your requirement
trait_font_size = "22px"
subheading_font_size = "20px"
description_font_size = "17px"
recommendations_font_size = "28px"

### edited by senthil
### Before creating the trait files, start the file by inserting the toc html template.
### in the loop later, we will insert the trait data
#with open("allout.html", "w", encoding='utf-8') as file:  # added encoding parameter here
#    file.write(original_toc_template)
#    file.write(" ")

# Loop through the DataFrame rows and create individual HTML files
page_count=7
#for index, row in df.iterrows():
for index, row in df.iloc[:-1].iterrows():
    # Parse the original template for each row so we start fresh
    soup = BeautifulSoup(original_template, 'html.parser')

     # Extract color and image based on subheading
    color, risk_image = get_risk_details(row['Subheading'])

    print ("lastline>>" + row['Subheading'])
    #sys.exit();

    # Create the nested table for details
    nested_table = soup.new_tag("table", id="inner_table")

    # Create first row for tick, trait, and subheading
    tr1 = soup.new_tag("tr")

    # Cell for risk symbol (image)
    td1 = soup.new_tag("td", id="image_cell")
    tick_img = soup.new_tag("img", src=risk_image, alt="Image Description")
    td1.append(tick_img)
    tr1.append(td1)

    # Cell for trait (in bold) and subheading (below the trait)
    td2_style = f"line-height: 1.5; background-color: {get_bg_color(row['Subheading'])};"
    td2 = soup.new_tag("td", id="content_cell", style=td2_style)
    bold_trait = soup.new_tag("strong", style=f"font-size: {trait_font_size};")
    bold_trait.string = row["trait"]
    td2.append(bold_trait)
    td2.append(soup.new_tag("br"))
    subheading = soup.new_tag("span", style=f"font-size: {subheading_font_size};")
    subheading.string = row["Subheading"]
    td2.append(subheading)
    tr1.append(td2)
    nested_table.append(tr1)

    # Create second row for description
    tr2 = soup.new_tag("tr")
    td3 = soup.new_tag("td")
    tr2.append(td3)
    td4_style = f"line-height: 1.5; background-color: {row.get('color_code', '#F5F5F5')};"
    td4 = soup.new_tag("td", style=td4_style)
    description_span = soup.new_tag("span", id="description", style=f"line-height: 1.5; font-size: {description_font_size};")
    description_span.string = row["description"]
    td4.append(description_span)
    tr2.append(td4)
    nested_table.append(tr2)

    # Create third row for recommendations
    tr3 = soup.new_tag("tr")
    td5 = soup.new_tag("td")
    tr3.append(td5)
    td6_style = f"line-height: 1.5; background-color:{get_bg_color(row['Subheading'])};"
    td6 = soup.new_tag("td", style=td6_style)
    recommendations_header = soup.new_tag("b", style=f"font-size: {recommendations_font_size};")
    recommendations_header.string = "Recommendations:"
    td6.append(recommendations_header)
    td6.append(soup.new_tag("br"))

    # Replace '*' with '<br/>' in the recommendation text and then append it
    recommendations_html_content = row["recommendations"].replace("*", "<br/>")
    recommendations_html = BeautifulSoup(recommendations_html_content, 'html.parser')
    #recommendations_html = BeautifulSoup(row["recommendations"], 'html.parser')
    td6.append(recommendations_html)
    tr3.append(td6)
    nested_table.append(tr3)

    # Create fourth row for additional details
    tr4 = soup.new_tag("tr")
    td7 = soup.new_tag("td")
    tr4.append(td7)
    td8 = soup.new_tag("td", bgcolor="#F5F5F5", style="line-height: 1.5;")

    genes_analyzed_label = soup.new_tag("b")
    genes_analyzed_label.string = "Genes Analyzed: "
    genes_value = soup.new_tag("span")
    genes_value.string = str(row["genes_analyzed"])
    td8.append(genes_analyzed_label)
    td8.append(genes_value)
    td8.append(soup.new_tag("br"))

    cli_markers_label = soup.new_tag("b")
    cli_markers_label.string = "Number of Gene Markers Found: "
    cli_value = soup.new_tag("span")
    cli_value.string = str(row["cli_markers"])
    td8.append(cli_markers_label)
    td8.append(cli_value)
    td8.append(soup.new_tag("br"))

    gene_markers_analyzed_label = soup.new_tag("b")
    gene_markers_analyzed_label.string = "Number of Gene Markers Analyzed: "
    gene_analyzed_value = soup.new_tag("span")
    gene_analyzed_value.string = str(row["gene_markers_analyzed"])
    td8.append(gene_markers_analyzed_label)
    td8.append(gene_analyzed_value)
    td8.append(soup.new_tag("br"))

    tr4.append(td8)
    nested_table.append(tr4)

    # Appending the nested table to the main table
    main_tr = soup.new_tag("tr")
    main_td = soup.new_tag("td")
    main_td.append(nested_table)
    main_tr.append(main_td)
    soup.table.append(main_tr)

    # Remove the old placeholder row from the soup to avoid duplication
    placeholder_row = soup.find("tr", id="trait_placeholder")
    if placeholder_row:
        placeholder_row.decompose()



     # Set page number dynamically. Assuming you have some logic to generate this number.
    page_num = index + 1  # Or any logic you have to get the page number
    footer_div = soup.find("div", id="page_number")
    if footer_div:
        footer_div.string = f"Page {page_count}"
    page_count = page_count + 1

     # Ensure Table of Contents remains unchanged
    #toc_div = soup.find("div", id="toc")
    #if toc_div:
    #     toc_div.string = "Table of Contents"

    # Save the updated HTML to a unique file based on the trait/index
    save_trait_to_file(index, soup)


with open(glb_03_output + "\\allout.html", "a", encoding='utf-8') as file:  # added encoding parameter here
    file.write(" ")
    file.write(" ")
    file.write("<div style='page-break-after: always;'></div>")
    file.write(original_disclaimer_template)

#directory_path = r'C:\Users\sangeetha\OneDrive\01-XCode\02-Savundariya_shared_docs\Caffeine\Python_automation\Caffeine_Python_automation\02_notebooks'  # Replace with the path to your directory containing the HTML files
#trait_files, summary_html_files  = get_all_html_files(directory_path)

convert_html_to_pdf(glb_03_output + "\\allout.html",glb_03_output + "\\output_stage.pdf")

## Try using Ghostscript for pdf merge
command = [
            ghostscript_bin,
            "-q", "-dNOPAUSE", "-dBATCH", "-sDEVICE=pdfwrite",
            "-sOutputFile=" + glb_03_output + "\\final_output.pdf",
            glb_template_root + "\\page_1.pdf",glb_03_output + "\\output_stage.pdf",glb_template_root + "\\last_page.pdf"
]
subprocess.run(command, shell=True)

### Delete residues
delete_files_in_folder(glb_04_temp_input)
os.remove(glb_03_output + "\\allout.html")
os.remove(glb_03_output + "\\output_stage.pdf")
#if os.path.exists(r"C:\Users\voice\Desktop\prefinal\output_stage.pdf"):
