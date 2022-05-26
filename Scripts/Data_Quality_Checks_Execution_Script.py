import logging
import boto3
from botocore.exceptions import ClientError
from datetime import date, datetime
import random
import pandas as pd
import io
import json


def checkvalues(ruleid, flag, desc, filedval, rowid):
    # RelID
    if(ruleid == 'RAW_01' and flag == 1):
        if(len(str(filedval)) < 8):
            return desc+" Record-"+str(rowid)
        else:
            return 1

    # Provider Id
    if(ruleid == 'RAW_02' and flag == 1):
        if not filedval.startswith("P"):
            return desc+" Record-"+str(rowid)
        else:
            return 1

        # First Name
    if(ruleid == 'RAW_03' and flag == 1):
        if(filedval == ''):
            return desc+" Record-"+str(rowid)
        else:
            return 1

    if(ruleid == 'RAW_04' and flag == 1):
        if(len(filedval) > 50):
            return desc+" Record-"+str(rowid)
        else:
            return 1

    if(ruleid == 'RAW_05' and flag == 1):
        if(filedval.isalpha() == False):
            return desc+" Record-"+str(rowid)
        else:
            return 1

    if(ruleid == 'RAW_06' and flag == 1):
        if " " in filedval:
            return desc+" Record-"+str(rowid)
        else:
            return 1

        # Last Name
    if(ruleid == 'RAW_07' and flag == 1):
        if(filedval == ''):
            return desc+" Record-"+str(rowid)
        else:
            return 1

    if(ruleid == 'RAW_08' and flag == 1):
        if(len(filedval) > 50):
            return desc+" Record-"+str(rowid)
        else:
            return 1

    if(ruleid == 'RAW_09' and flag == 1):
        if(filedval.isalpha() == False):
            return desc+" Record-"+str(rowid)
        else:
            return 1

    if(ruleid == 'RAW_10' and flag == 1):
        if " " in filedval:
            return desc+" Record- "+str(rowid)
        else:
            return 1

    # Speciality Code
    if(ruleid == 'RAW_11' and flag == 1):
        if (len(filedval) > 2):
            return desc+" Record-"+str(rowid)
        else:
            return 1


today = date.today()
folder_date = today.strftime("%m-%d-%y")
now = datetime.now()
current_time = now.strftime("%H_%M_%S")
n = random.randint(0, 22)
s3 = boto3.client('s3')
bucket_name = 'dsp-data-lake-demo'
data = s3.get_object(Bucket=bucket_name,
                     Key='DQM/raw/outbound/CLIENT_SW_PP_DEMO_Raw.txt')
rules = s3.get_object(Bucket='dsp-data-lake-dev',
                      Key='demo/rules/rules.json')
jsonObject = json.load(rules["Body"])

a_dataframe = pd.read_csv(io.BytesIO(
    data['Body'].read()), sep='|', engine='python')
rules_dataframe = pd.DataFrame(jsonObject)
headerVal = a_dataframe.columns.tolist()
hederjoin = '|'.join(headerVal)
faildata = passdata = ''
error = []
i = 0
folder_name = "Run-Date" + folder_date + "Time" + current_time
for key, value in a_dataframe.iterrows():
    i = i+1
    Rel_id = value['Rel_ID']
    Provider_ID = value['Provider_ID']
    First_Name = value['First_Name']
    Last_Name = value['Last_Name']
    Specialty_Code = value['Specialty_Code']
    Zip_Code = value['Zip_Code']
    Address = value['Address']
    City = value['City']
    State = value['State']
    errorData = ''
    for k, v in rules_dataframe.iterrows():
        rule_id = v['rule_id']
        flag = v['flag']
        desc = v['rule_des']
        if(rule_id == 'RAW_01'):
            rule1 = checkvalues(rule_id, flag, desc, Rel_id, i)
            if(rule1 != 1):
                errorData = 1
                error.append(str(rule1))
        if(rule_id == 'RAW_02'):
            rule2 = checkvalues(rule_id, flag, desc, Provider_ID, i)
            if(rule2 != 1):
                errorData = 1
                error.append(str(rule2))
        if(rule_id == 'RAW_03'):
            rule3 = checkvalues(rule_id, flag, desc, First_Name, i)
            if(rule3 != 1):
                errorData = 1
                error.append(str(rule3))
        if(rule_id == 'RAW_04'):
            rule4 = checkvalues(rule_id, flag, desc, First_Name, i)
            if(rule4 != 1):
                errorData = 1
                error.append(str(rule4))
        if(rule_id == 'RAW_05'):
            rule5 = checkvalues(rule_id, flag, desc, First_Name, i)
            if(rule5 != 1):
                errorData = 1
                error.append(str(rule5))
        if(rule_id == 'RAW_06'):
            rule6 = checkvalues(rule_id, flag, desc, First_Name, i)
            if(rule6 != 1):
                errorData = 1
                error.append(str(rule6))
        if(rule_id == 'RAW_07'):
            rule7 = checkvalues(rule_id, flag, desc, Last_Name, i)
            if(rule7 != 1):
                errorData = 1
                error.append(str(rule7))
        if(rule_id == 'RAW_08'):
            rule8 = checkvalues(rule_id, flag, desc, Last_Name, i)
            if(rule8 != 1):
                errorData = 1
                error.append(str(rule8))
        if(rule_id == 'RAW_09'):
            rule9 = checkvalues(rule_id, flag, desc, Last_Name, i)
            if(rule9 != 1):
                errorData = 1
                error.append(str(rule9))
        if(rule_id == 'RAW_10'):
            rule10 = checkvalues(rule_id, flag, desc, Last_Name, i)
            if(rule10 != 1):
                errorData = 1
                error.append(str(rule10))
        if(rule_id == 'RAW_11'):
            rule11 = checkvalues(rule_id, flag, desc, Specialty_Code, i)
            if(rule11 != 1):
                errorData = 1
                error.append(str(rule11))

    if(errorData == 1):
        failedrecodrs = value.tolist()
        failedrecordswithpipe = (
            '|'.join(map(str, failedrecodrs))).replace("nan", '')
        faildata = faildata+failedrecordswithpipe+'\n'
    else:
        passesrecords = value.tolist()
        passedrecordswithpipe = (
            '|'.join(map(str, passesrecords))).replace("nan", '')
        passdata = passdata+passedrecordswithpipe+'\n'

        # print(passedrecordswithpipe)

failedRecords = hederjoin + '\n' + faildata
passedRecords = hederjoin + '\n' + passdata

errorList = '\n'.join([s for s in error if not s.isdigit()])
print(errorList)
s3.put_object(Body=passedRecords, Bucket=bucket_name,
              Key="DQM/staging/inbound/"+folder_name+"/"+"1_raw_filter_data.txt")
s3.put_object(Body=failedRecords, Bucket=bucket_name,
              Key="DQM/staging/inbound/"+folder_name+"/"+"2_raw_failed_records.txt")
s3.put_object(Body=errorList, Bucket=bucket_name,
              Key="DQM/staging/inbound/"+folder_name+"/"+"3_raw_error_log.txt")
s3.put_object(Body=passedRecords, Bucket=bucket_name,
              Key="DQM/staging/outbound/CLIENT_SW_PP_DEMO_Staging.txt")
s3.put_object(Body=failedRecords, Bucket=bucket_name,
              Key="DQM/staging/outbound/CLIENT_SW_PP_DEMO_Staging_dq_failed.txt")
