import requests as rs
import xmltodict
from dict2xml import dict2xml
from json import JSONDecodeError
import pandas as pd
from pathlib import Path
import json
import boto3
import io
from io import StringIO

class freewheel_auth():
    def __init__(self,username,password):
        try:
            filetype = 'xml'
            headers = {'accept': 'application/json','content-type': 'application/x-www-form-urlencoded',}
            data = { 'grant_type': 'password','username': username,'password': password,}
            response = rs.post('https://api.freewheel.tv/auth/token', headers=headers, data=data).json()
            response['access_token']
            token = 'Bearer ' + response['access_token']
            headers = {'accept': 'application/'+str(filetype), 'authorization' : token, 'Content-Type': 'application/'+str(filetype),} 
            self.xml = headers
            filetype = 'json'
            headers = {'accept': 'application/json','content-type': 'application/x-www-form-urlencoded',}
            data = { 'grant_type': 'password','username': username,'password': password,}
            response = rs.post('https://api.freewheel.tv/auth/token', headers=headers, data=data).json()
            response['access_token']
            token = 'Bearer ' + response['access_token']
            headers = {'accept': 'application/'+str(filetype), 'authorization' : token, 'Content-Type': 'application/'+str(filetype),} 
            self.json = headers
        except JSONDecodeError:
            raise Exception('Invalid Credentials')
            
class PlacementStringQuery():
    
    def __init__(self,freewheel_auth,query,Dict = None):
        self.freewheel_auth = freewheel_auth
        headers = self.freewheel_auth.xml
        get = rs.get(f"https://api.freewheel.tv/services/v3/placements?name={query}&status=ACTIVE&per_page=50&page=1",headers=headers).text
        get = xmltodict.parse(get,dict_constructor = dict)
        placements = get['placements']['placement']
        listy = []
        pids = []
        pages = int(get['placements']['@total_pages'])
        for i in range(pages):
            get = rs.get(f"https://api.freewheel.tv/services/v3/placements?name={query}&status=ACTIVE&per_page=50&page={i+1}",headers=headers).text
            get = xmltodict.parse(get,dict_constructor = dict)
            placements = get['placements']['placement']
            for item in placements:
                if Dict.lower() == 'yes':
                    PID = item['id']
                    pids.append(PID)
                    attach = {'attach' : 'Yes', "PID" : PID}
                    placement = Placement(freewheel_auth = freewheel_auth,attach = attach)
                    placement.Get()
                    listy.append(placement.dict)
                else:
                    PID = item['id']
                    pids.append(PID)
                    attach = {'attach' : 'Yes', "PID" : PID}
                    placement = Placement(freewheel_auth = freewheel_auth,attach = attach)
                    placement.Get()
                    listy.append(placement)

        
        self.PIDs = pids           
        self.list = listy
        

        
        
        
    def GetPacing(self):
        
        nightly = []
        headers = self.freewheel_auth.json
        for item in self.PIDs:
            url = f'https://api.freewheel.tv/services/v4/placements/{item}/forecasts?type=nightly'
            get = rs.get(url,headers=headers).json()
            nightly.append(get)
            
        self.Pacing = nightly
        
        
        
    def ExportJSONToS3(self,S3Auth):
        S3Auth.s3c.put_object(
         Body=json.dumps(self.list),
         Bucket=S3Auth.bucketName ,
         Key=S3Auth.bucketKey)
        
class PacingSnapShot():
    
    def __init__(self,PlacementStringQuery):
    
        PlacementStringQuery.GetPacing()
        pacing_dict = {}
        for i in range(len(PlacementStringQuery.list)):
            budget = PlacementStringQuery.Pacing[i]['budget']
    
            if ',' in budget:
                budget = int("".join(PlacementStringQuery.Pacing[i]['budget'].replace('imps', '').replace(' ', "").split(',')))
            else:
                budget = int(budget)
                
            try:
                
                start = PlacementStringQuery.list[i]['placement']['schedule']['start_time']
                end = PlacementStringQuery.list[i]['placement']['schedule']['end_time']
                
            except KeyError:
                start = 'None'
                end  = 'None'
        
            pacing_dict[PlacementStringQuery.list[i]['placement']['name']] = {'OSI' : PlacementStringQuery.Pacing[i]['on_schedule_indicator']/100,
    'FFDR': PlacementStringQuery.Pacing[i]['forecast_final_delivery_rate']/100,
    'grossDeliveredImps' : PlacementStringQuery.Pacing[i]['delivered_impressions'],
    'budgetedImps' : budget,
    'startDate': start, 'endDate': end}
            
            
            self.SnapShot = pacing_dict
            
    def ExportToCSV(self,path):

        df = pd.DataFrame()
        name_list = []
        osi_list = []
        ffdr_list = []
        g_imps = []
        b_imps = []
        start_list = []
        end_list = []

        for item in self.SnapShot.keys():
            name_list.append(item)
            start_list.append(self.SnapShot[item]['startDate'])
            end_list.append(self.SnapShot[item]['endDate'])
            osi_list.append(self.SnapShot[item]['OSI'])
            ffdr_list.append(self.SnapShot[item]['FFDR'])
            g_imps.append(self.SnapShot[item]['grossDeliveredImps'])
            b_imps.append(self.SnapShot[item]['budgetedImps'])

        df['placementName'] = name_list
        df['placementStartDate']  = start_list
        df['placementEndDate'] = end_list
        df['placementOSI'] = osi_list
        df['placementFFDR'] = ffdr_list
        df['placementGrossDeliveredImpressions'] = g_imps
        df['PlacementBudgetedImpressions'] = b_imps

        df.to_csv(path,index=False)
        print('pushedToCSV')

    def ExportToJSON(self,path):

        file = open(path,'w')
        file.write(json.dumps(self.SnapShot))
        file.close()


        print('pushedToJSON')
        
        
    def ExportJSONToS3(self,S3Auth):
        S3Auth.s3c.put_object(
         Body=json.dumps(self.SnapShot),
         Bucket=S3Auth.bucketName ,
         Key=S3Auth.bucketKey)
        
    def ExportCSVToS3(self,S3Auth):        
        df = pd.DataFrame()
        name_list = []
        osi_list = []
        ffdr_list = []
        g_imps = []
        b_imps = []
        start_list = []
        end_list = []

        for item in self.SnapShot.keys():
            name_list.append(item)
            start_list.append(self.SnapShot[item]['startDate'])
            end_list.append(self.SnapShot[item]['endDate'])
            osi_list.append(self.SnapShot[item]['OSI'])
            ffdr_list.append(self.SnapShot[item]['FFDR'])
            g_imps.append(self.SnapShot[item]['grossDeliveredImps'])
            b_imps.append(self.SnapShot[item]['budgetedImps'])

        df['placementName'] = name_list
        df['placementStartDate']  = start_list
        df['placementEndDate'] = end_list
        df['placementOSI'] = osi_list
        df['placementFFDR'] = ffdr_list
        df['placementGrossDeliveredImpressions'] = g_imps
        df['PlacementBudgetedImpressions'] = b_imps
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index = False)
        S3Auth.s3r.Object(S3Auth.bucketName,S3Auth.bucketKey).put(Body=csv_buffer.getvalue())
        
        
    
    
                    

    
    
        
class Placement():
    def __init__(self,freewheel_auth,IO= None,attach= None):
        try:
            if attach['attach'] ==  'Yes':
                self.freewheel_auth = freewheel_auth
                headers = self.freewheel_auth.xml
                self.PID = attach['PID'] 
                node = 'all'
                get_url = f'https://api.freewheel.tv/services/v3/placements/{str(self.PID)}?show={node}'
                get_placement = rs.get(get_url,headers=headers).text
                self.dict = xmltodict.parse(get_placement,dict_constructor=dict)
                self.Name  = self.dict['placement']['name']
            elif attach['attach'] == 'No':

                try:
                    self.freewheel_auth = freewheel_auth
                    self.IO = IO
                except:
                    raise Exception("invalid username or password")
                
        except:
            raise Exception("invalid username or password")
                
            

        
        
    def Get(self):
        try:
            PID = self.PID
            headers = self.freewheel_auth.xml
            node = 'all'
            get_url = f'https://api.freewheel.tv/services/v3/placements/{str(PID)}?show={node}'
            get_placement = rs.get(get_url,headers=headers).text
            self.dict = xmltodict.parse(get_placement,dict_constructor=dict)
            self.aud =self.dict['placement']['audience_targeting']
            self.content = self.dict['placement']['content_targeting']
            include = 'include'
            exclude = 'exclude'
            self.contentInclusions= self.dict['placement']['content_targeting'][include]
            self.contentExclusions = self.dict['placement']['content_targeting'][exclude]
            self.sets = self.contentInclusions['set']
            self.numberOfSets = len(self.contentInclusions['set'])
            try:
                self.exclusionFwItems = list(self.contentExclusions.keys())
            except AttributeError:
                self.exclusionFwItems = 'None'

            try:
                keys = list(self.sets.keys())
                for item in keys:
                    print(item)
                self.set = self.sets
                self.numberOfsets = 1
            except AttributeError:

                keys = self.sets

                if len(keys) == 3:
                    self.setOne = keys[0]
                    self.setOneFwItems = list(keys[0].keys())
                    self.setTwo = keys[1]
                    self.setTwoFwItems = list(keys[1].keys())
                    self.setThree = keys[2]
                    self.setThreeFwItems = list(keys[2].keys())
                if len(keys) ==2: 
                    self.setOne = keys[0]
                    self.setOneFwItems =list(keys[0].keys())               
                    self.setTwo = keys[1]
                    self.setTwoFwItems = list(keys[1].keys())
        except KeyError:
            PID = self.PID
            headers = self.freewheel_auth.xml
            node = 'all'
            get_url = f'https://api.freewheel.tv/services/v3/placements/{str(PID)}?show={node}'
            get_placement = rs.get(get_url,headers=headers).text
            self.dict = xmltodict.parse(get_placement,dict_constructor=dict)
                


            
    
    def Create(self,Name):
        headers = self.freewheel_auth.xml
        IO = self.IO
        placement = {}
        placement['placement'] = {'insertion_order_id' : IO}
        placement['placement']['name'] = Name
        createpid = rs.post('https://api.freewheel.tv/services/v3/placement/create',headers=headers,data=dict2xml(placement))
        pid = xmltodict.parse(createpid.text,dict_constructor=dict)['placement']['id']
        self.PID = pid
        self.Name = Name
    

        
    
    
class FreewheelAudience():
    def __init__(self,Name,ID):
        
        self.Name = Name
        self.ID = ID
class FreewheelAudienceList():
    def __init__(self,freewheel_auth):
    
        fw = freewheel_auth
        url = f'https://api.freewheel.tv/services/v4/audience_items?status=ACTIVE&page=1&per_page=100'
        get = rs.get(url,headers = fw.json).json()

        audience_list = []
        numberOfPages = get['audience_items']['total_page']

        for i in range(numberOfPages):
            url = f'https://api.freewheel.tv/services/v4/audience_items?status=ACTIVE&page={i+1}&per_page=100'
            get = rs.get(url,headers = fw.json).json()
            for item in get['audience_items']['items']:
                FWA = FreewheelAudience(Name = item['name'], ID = item['id'])
                audience_list.append(FWA)
                
        self.list = audience_list
        
        
    def ToDict(self):
        dicty = {}
        for item in self.list:
            dicty[item.Name] = item.ID
            
        self.dict = dicty
        
    def ExportToCSV(self,path):
        self.ToDict()
        df = pd.DataFrame()
        df['audienceName'] = self.dict.keys()
        df['audienceId'] = self.dict.values()
        df.to_csv(path,index = False)
        
    def ExportToJSON(self,path):
        self.ToDict()
        file = open(path,'w')
        file.write(json.dumps(self.dict))
        file.close()
        
    def ExportJSONToS3(self,S3Auth):
        self.ToDict()
        S3Auth.s3c.put_object(
         Body=json.dumps(self.dict),
         Bucket=S3Auth.bucketName ,
         Key=S3Auth.bucketKey)
        
    def ExportCSVToS3(self,S3Auth):        
        self.ToDict()
        df = pd.DataFrame()
        df['audienceName'] = self.dict.keys()
        df['audienceId'] = self.dict.values()
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index = False)
        S3Auth.s3r.Object(S3Auth.bucketName,S3Auth.bucketKey).put(Body=csv_buffer.getvalue())
        
        
        
        
        
class S3Auth():
    def __init__(self,s3r,s3c,bucketName, bucketKey):
        self.s3r = s3r
        self.s3c = s3c
        self.bucketName = bucketName
        self.bucketKey = bucketKey
        
        
            
            
