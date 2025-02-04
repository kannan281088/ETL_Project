#Imported required modules
import wget as wg
import os as fileOper
import shutil as sUtilFileOper
import zipfile as zipHandle
import pandas as pd
from IPython.display import display
import glob
import datetime

#Declared requied variables 
downloadFrom = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module 6/Lab - Extract Transform Load/data/source.zip"
downloadTo="F:/Learn/Data_Engineering/Projects/ETL_Project/Download/"
zipFileName = "source.zip"
zipExtractLoc = "F:/Learn/Data_Engineering/Projects/ETL_Project/Download/Data/"

#Writing a log files
file = open("log_file.txt", "a")
file.write('\n-----------------------------' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + '-----------------------------')
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'ETL process started')
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'File download started')

#Download zip files
try:
    if fileOper.path.isfile(downloadTo + zipFileName):
         sUtilFileOper.rmtree(downloadTo)
    fileOper.mkdir(downloadTo)     
    wg.download(downloadFrom, downloadTo + zipFileName)    
except Exception as e:
    file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + 'Error while Downloading - ' + str(e))
    

file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'File download completed')
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Zip extraction started')

#Extracting zip files
try:
    if fileOper.path.isfile(downloadTo + zipFileName):
        with zipHandle.ZipFile(downloadTo + zipFileName, 'r') as zip_ref:
            zip_ref.extractall(zipExtractLoc)        
except Exception as e:
    file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + 'Error while Extracting - ' + str(e))

file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Zip extraction completed')

file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Data extraction phase started')

#CSV Data processing method
def exportCSVData():
    try:        
        file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Started processing CSV files')
        csvDF = pd.DataFrame()
        for name in glob.glob(str(zipExtractLoc) + '*.csv'):    
            file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Processing - ' + fileOper.path.basename(name))
            csvDF = pd.concat([csvDF, pd.read_csv(name)],ignore_index=True)
        file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Processing CSV files completed')
        return csvDF
    except Exception as e:        
        file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + str(e) + " - CSV Exception")

#JSON Data processing method
def exportJsonData():
    try:
        file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Started processing JSON files')
        jsonDF=pd.DataFrame()
        for name in glob.glob(str(zipExtractLoc) + '*.json'):  
            file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Processing - ' + fileOper.path.basename(name))
            jsonDF = pd.concat([jsonDF, pd.read_json(name, lines=True)],ignore_index=True)        
        file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Processing JSON files completed')
        return jsonDF        
    except Exception as e:        
        file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + str(e) + " - JSON Exception")

#XML Data processing method
def exportXMLData():
    try:
        file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Started processing XML files')
        xmlDF=pd.DataFrame()
        for name in glob.glob(str(zipExtractLoc) + '*.xml'): 
            file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Processing - ' + fileOper.path.basename(name))
            xmlDF = pd.concat([xmlDF, pd.read_xml(name)],ignore_index=True)
        file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Processing XML files completed')
        return xmlDF
    except Exception as e:        
        file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + str(e) + " - XML Exception")

#Combining CSV, JSON, XML Data 
combinedDF = pd.DataFrame()
combinedDF = pd.concat([exportCSVData(), exportJsonData(), exportXMLData()], ignore_index=True)

file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Combined CSV, JSON, XML files data')
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Data extraction phase completed')
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Data transformation phase started')

#Converting height, weight column to float
combinedDF['height'] = combinedDF['height'].astype(float)
combinedDF['weight'] = combinedDF['weight'].astype(float)

file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Converted height, weight column to float')

#Transforming Heights from inches to meters
combinedDF['height'] = combinedDF['height']/39.37
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Transformed Heights from inches to meters')

#Transforming Weights from pounds to kilograms
combinedDF['weight'] = combinedDF['weight']/2.205
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Transformed Weights from pounds to kilograms')

#Checking and removing duplicates
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Checking for duplicates')
dupDF = combinedDF.duplicated()
if combinedDF[dupDF].shape[0] != 0:
    file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Removing the duplicate data')
    combinedDF.drop_duplicates(inplace=True)

#Reseting the index
combinedDF = combinedDF.reset_index(drop=True)
combinedDF.index = combinedDF.index + 1

#Data loading to CSV
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Data loading phase started')
curDate = datetime.datetime.today().strftime('%Y_%m_%d_%H_%M_%S')
csvFilename = 'transformed_data_' + curDate + '.csv'
combinedDF.to_csv(csvFilename, index_label='S.no')
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Data loaded to - ' + csvFilename)
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'Data loading phase completed')
file.write('\n' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' -- ' + 'ETL process completed')
file.write('\n---------------------------------------------------------------------------------------------------------')

#Closing the file object
file.close()