#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys,os,os.path,io,json,math,re,time,uuid,random,pytz
import numpy as np
from datetime import datetime
# Set Rucio virtual environment configuration 

os.environ['RUCIO_HOME']=os.path.expanduser('~/Rucio-v2/rucio')

from rucio.client.client import *
from rucio.client.rseclient import *
from rucio.rse import rsemanager as rsemgr
from rucio.client.client import Client
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.client.downloadclient import DownloadClient
import rucio.rse.rsemanager as rsemgr
from rucio.client.ruleclient import RuleClient
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import (AccountNotFound, Duplicate, RucioException, DuplicateRule, InvalidObject, DataIdentifierAlreadyExists, FileAlreadyExists, RucioException,
                                    AccessDenied, InsufficientAccountLimit, RuleNotFound, AccessDenied, InvalidRSEExpression,
                                    InvalidReplicationRule, RucioException, DataIdentifierNotFound, InsufficientTargetRSEs,
                                    ReplicationRuleCreationTemporaryFailed, InvalidRuleWeight, StagingAreaRuleRequiresLifetime)
from rucio.common.utils import adler32, detect_client_location, execute, generate_uuid, md5, send_trace, GLOBALLY_SUPPORTED_CHECKSUMS

sys.path.append("/usr/lib64/python3.6/site-packages/")
import gfal2
from gfal2 import Gfal2Context, GError

gfal = Gfal2Context()

# Rucio settings 
 
RSE_origin = 'PIC-INJECT'
RSE_destiny = 'PIC-DCACHE'
RSE_QOS = 'QOS=FAST'

RSEs = {'RSE_destiny':RSE_destiny, 'RSE_QOS':RSE_QOS}

account = 'bruzzese'
auth_type = 'x509_proxy'
Default_Scope = 'MAGIC_PIC_BRUZZESE'
Client = Client(account=account)
uploadClient = UploadClient()
rulesClient = RuleClient()
downloadClient = DownloadClient()

print(json.dumps(Client.whoami(), indent=2))


# In[2]:


############################

# Functions

############################

def generate_random_file(size, copies = 1, filename=None):
    """
    generate big binary file with the specified size in bytes
    :param filename: the filename
    :param size: the size in bytes
    :param copies: number of output files to generate
    
    """
    n_files = []
    n_files = np.array(n_files, dtype = np.float32)   
    for i in range(copies):
        
        if filename == None :      
            date = str(datetime.today().strftime('%Y%m%d'))
            run = str(random.randint(10000000,99999999))
            file = date + '_M1_' + run + '.005_D_1ES1959_650-W0.40_000.root'
        
        if os.path.exists(file) : 
            print ("File %s already exist" %file)

        else:
            print ("File %s not exist" %file)    
            try : 
                newfile = open(file, "wb")
                newfile.seek(size)
                newfile.write(b"\0")
                newfile.close ()
                os.stat(file).st_size
                print('random file with size %f generated ok'%size)
                n_files = np.append(n_files, file)
            except :
                print('could not be generate file %s'%file)

    return(n_files)

def look_for_run(fileName) :  

    try :
        run = re.search('\d{8}\.', fileName)
        if not run :
            run = re.search('_\d{8}', fileName)
            run = run[0].replace('_','')
        elif (type(run).__module__, type(run).__name__) == ('_sre', 'SRE_Match') : 
            run = run.group(0)
            run = run.replace('.','')
        else :
            run = run[0].replace('.','')
            
        return(str(run))
    except : 
        pass
    try :
        if not run :
            run = re.findall('\d{8}\_', fileName)
            run = run[0].replace('_','')
        return(str(run))
    except : 
        pass
    
def createDataset(new_dataset, new_scope=Default_Scope) :         
    try:
        Client.add_dataset(scope=new_scope, name=new_dataset)
        return(True)
    except DataIdentifierAlreadyExists:
        print("|  -  -  - Dataset %s already exists" % new_dataset)
        return(False)
    except Duplicate as error:
        return generate_http_error_flask(409, 'Duplicate', error.args[0])
    except AccountNotFound as error:
        return generate_http_error_flask(404, 'AccountNotFound', error.args[0])
    except RucioException as error:
        return generate_http_error_flask(500, error.__class__.__name__, error.args[0])

def registerIntoGroup(n_file, new_dataset, new_scope=Default_Scope):

    type_1 = Client.get_did(scope=new_scope, name=new_dataset)
    type_2 = Client.get_did(scope=new_scope, name=n_file)

    try:
        Client.attach_dids(scope=new_scope, name=new_dataset, dids=[{'scope':new_scope, 'name':n_file}])
    except :
        PrintException()
        
def addReplicaRule(destRSE, group, new_scope=Default_Scope):

    if destRSE:
        try:
            #rule = rulesClient.add_replication_rule([{"scope":new_scope,"name":group}],copies=1, lifetime=5, rse_expression=destRSE, grouping='ALL', account=account, purge_replicas=True, source_replica_expression=RSE_destiny)
            rule = rulesClient.add_replication_rule([{"scope":new_scope,"name":group}],copies=1, rse_expression=destRSE, grouping='ALL', account=account, purge_replicas=True, source_replica_expression=RSE_destiny)
            return(rule[0])
        
        except DuplicateRule:
            PrintException()
            rules = list(Client.list_account_rules(account=account))
            if rules : 
                for rule in rules :
                    if rule['rse_expression'] == destRSE and rule['scope'] == scope and rule['name'] == group:
                        print('| - - - - Rule already exists %s which contains the following DID %s:%s' % (rule['id'],scope, group))
    
def getchecksum(name_file): 
    try :
        checksum = gfal.checksum(name_file, 'md5')
    except : 
        checksum = gfal.checksum(os.path.join('file:///'+os.getcwd(), name_file), 'md5')       
    return(checksum)


# In[3]:


############################

# Check existence of json File

############################

def json_write(data, filename='Rucio-bkp.json'): 
    with io.open(filename, 'w') as f: 
        json.dump(data, f, ensure_ascii=False, indent=4)
        
def json_check(json_file_name='Rucio-bkp.json') :
    # checks if file exists
    if not os.path.isfile(json_file_name) : 
        return(False)
    
    elif os.stat(json_file_name).st_size == 0 :
        os.remove(json_file_name)
        return(False)
    
    elif os.path.isfile(json_file_name) and os.access(json_file_name, os.R_OK) :
        return(True)

def stateCheck(json_file='Rucio-bkp.json'):
      
    with open(json_file) as f : 
        data_keys  = json.load(f)
        return(data_keys)


# In[4]:


list_files = generate_random_file(size=random.randint(10,99), copies=1)     

print(list_files)


# In[5]:


result_dict = dict()
for n in range(0, len(list_files)) :
    
    name_file = list_files[n]    
    print(name_file)
    
    """
    generate a dictionary with the information for the upload
    :param path: the filename
    :param rse: the destination RSE name
    :param did_scope: The scope of the DID.
    :param lifetime: Seconds of DID lifetime 
    """
    
    file = {'path': "./"+name_file, 'rse': RSE_destiny, 'did_scope': Default_Scope, 'lifetime':5}
    
    result_dict[name_file] = {} 
    result_dict[name_file]['Scope'] = Default_Scope
    result_dict[name_file]['Replicated'] = {'Local' : {'registered': datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),'checksum':getchecksum(name_file)}}

    # Perform upload
    client_upload = uploadClient.upload([file])

    # Create a dataset
    dataset_name = look_for_run(name_file)
    
    createDataset(new_dataset = dataset_name, new_scope = Default_Scope)
    
    registerIntoGroup(n_file = name_file, new_dataset = str(dataset_name))

    for rse in RSEs :
        # Contruct a dictionary with the destiny RSE 
        if name_file in result_dict :   
            temp_dict = dict()
            temp_dict[name_file] = {} 
            temp_dict[name_file]['Replicated'] = {RSEs[rse] : {'registered': datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}}

            result_dict[name_file]['Replicated'].update(temp_dict[name_file]['Replicated'])
        
        rule = addReplicaRule(destRSE = RSEs[rse], group = dataset_name, new_scope = Default_Scope)
        
        # update a rule so it 'll be automatically be deleted once it has been successfully replicated
        update = Client.update_replication_rule(rule_id=rule, options={'lifetime': 60, 'child_rule_id':rule, 'purge_replicas':True})
    
    
    if json_check() == True :
        result_dict.update(stateCheck())

# Save the uploads, replication and time into a json file
json_write(result_dict)


# In[6]:



# Download a file fomr a datalake, and perform a checksum. 
# Then save it in a json file
for n in range(0, len(list_files)) :
    
    name_file = list_files[n]
    dataset_name = str(look_for_run(name_file))
    download = downloadClient.download_dids(items=[{'did':'{}:{}'.format(Default_Scope,name_file)}], num_threads=2, trace_custom_fields={}, traces_copy_out=None)
    
    result_dict = stateCheck()
    if json_check() == True :
        if name_file in result_dict.keys() :
            temp_dict = dict()
            temp_dict[name_file] = {} 
            temp_dict[name_file]['Replicated'] = {download[0]['sources'][0]['rse'] : {'downloaded': datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),'checksum':gfal.checksum('file:///'+download[0]['dest_file_paths'][0],'md5')}}
            result_dict[name_file]['Replicated'][download[0]['sources'][0]['rse']].update(temp_dict[name_file]['Replicated'][download[0]['sources'][0]['rse']])
            
    json_write(result_dict)
    os.remove(download[0]['dest_file_paths'][0]) 
    


# In[ ]:




