



import requests as rs
import json
from json import JSONDecodeError
import xmltodict
from dict2xml import dict2xml


class fw():
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

            
class placementObj():
    def __init__(self ,**kwargs):
        dictObj  = {}
        for key, value in kwargs.items():
            
            dictObj[key] = value
            

        self.data = {'placement' : dictObj}
        self.id = self.data['placement']['id']
        
        
    def Get(self,fw):
        node = 'all'
        try:
            get_url = f"https://api.freewheel.tv/services/v3/placements/{self.data['placement']['id']}?show={node}"
        except KeyError:
            raise placementMissingIdError('''placement is missing ID from its data''')
        get_placement = rs.get(get_url,headers=fw.xml).text
        self.data = xmltodict.parse(get_placement,dict_constructor=dict)
        
        return self
    
    def getPacing(self,fw):
        get = rs.get(f"https://api.freewheel.tv/services/v4/placements/{self.data['placement']['id']}/forecasts?type=nightly",headers =fw.json).json()
        self.pacing = get

    def getInsertionOrder(self,fw):

        get= XML(rs.get(f"https://api.freewheel.tv/services/v3/insertion_orders/{self.data['placement']['insertion_order_id']}",headers = fw.xml))

        self.insertionOrderId= get['insertion_order']['id']
        self.campaignId = get['insertion_order']['campaign_id']

    def getAdvertiserAgency(self,fw):
        get = XML(rs.get(f"https://api.freewheel.tv/services/v3/campaign/{self.campaignId}",headers = fw.xml))
        self.advertiserId = get['campaign']['advertiser_id']
        self.agencyId = get['campaign']['agency_id']

        

        


def getPlacements(fw):
    counter = 0
    queryGet = xmltodict.parse(rs.get(f"https://api.freewheel.tv/services/v3/placements?status=ACTIVE&per_page=50&page=1",headers = fw.xml).text,dict_constructor = dict)
    print(queryGet)
    placementList = []
    for i in range(int(queryGet['placements']['@total_pages'])):
        counter =counter+1
        loopGet = xmltodict.parse(rs.get(f"https://api.freewheel.tv/services/v3/placements?status=ACTIVE&per_page=50&page={i+1}",headers = fw.xml).text,dict_constructor = dict)
        if type(loopGet['placements']['placement']) == list :
            for item in loopGet['placements']['placement']:
                placementList.append(placementObj(id = item['id'], name = item['name'] ))
                counter =counter+1
        elif type(loopGet['placements']['placement']) == dict:
            placementList.append(placementObj(id = loopGet['id'], name = loopGet['name'] ))
            
            counter =counter+1
    
    return placementList

        
        
    
    

        
        
  
        
    



    
def placementNameQuery(fw, query): 
    

    queryGet = xmltodict.parse(rs.get(f"https://api.freewheel.tv/services/v3/placements?name={query}&status=ACTIVE&per_page=50&page=1",headers = fw.xml).text,dict_constructor = dict)
    print(queryGet)
    placementList = []
    for i in range(int(queryGet['placements']['@total_pages'])):
        loopGet = xmltodict.parse(rs.get(f"https://api.freewheel.tv/services/v3/placements?name={query}&status=ACTIVE&per_page=50&page={i+1}",headers = fw.xml).text,dict_constructor = dict)
        if type(loopGet['placements']['placement']) == list :
            for item in loopGet['placements']['placement']:
                placementList.append(placementObj(id = item['id'], name = item['name'] ))
        elif type(loopGet['placements']['placement']) == dict:
            placementList.append(placementObj(id = loopGet['id'], name = loopGet['name'] ))
#     elif status != 'active':
    
#         queryGet = xmltodict.parse(rs.get(f"https://api.freewheel.tv/services/v3/placements?name={query}&per_page=50&page=1",headers = fw.xml).text,dict_constructor = dict)
#         placementList = []
#         for i in range(int(queryGet['placements']['@total_pages'])):
#             loopGet = xmltodict.parse(rs.get(f"https://api.freewheel.tv/services/v3/placements?name={query}&per_page=50&page={i+1}",headers = fw.xml).text,dict_constructor = dict)
#             if type(loopGet['placements']['placement']) == list :
#                 for item in loopGet['placements']['placement']:
#                     placementList.append(placementObj(id = item['id'], name = item['name'] ))
#             elif type(loopGet['placements']['placement']) == dict:
#                 placementList.append(placementObj(id = loopGet['id'], name = loopGet['name'] ))
                
    return placementList



class campaignObj():
    
    def __init__(self ,**kwargs):
        dictObj  = {}
        for key, value in kwargs.items():
            
            dictObj[key] = value
            
        
        self.data = {'campaign' : dictObj}
        self.id = self.data['campaign']['id']

def campaignDateQuery(fw, startDate,endDate): 
    

    queryGet = XML(rs.get(f'https://api.freewheel.tv/services/v3/campaigns?start_date={startDate}..{endDate}&per_page=100&page=1',headers= fw.xml))
    campaignList = []
    for i in range(int(queryGet['campaigns']['@total_pages'])):
        loopGet = XML(rs.get(f'https://api.freewheel.tv/services/v3/campaigns?start_date={startDate}..{endDate}&per_page=100&page={i+1}',headers= fw.xml))
        if type(loopGet['campaigns']['campaign']) == list :
            for item in loopGet['campaigns']['campaign']:
                if item['status'] == 'ACTIVE':
                    campaignList.append(campaignObj(id = item['id'], name = item['name'] ))
        elif type(loopGet['campaigns']['campaign']) == dict:
                if loopGet['status'] == 'ACTIVE':
                    campaignList.append(campaignObj(id = loopGet['id'], name = loopGet['name'] ))
    
    return campaignList()


def getCampaignAdvertiser(self,fw):
    
    self.advertiserId = XML(rs.get(f'https://api.freewheel.tv/services/v3/campaign/{self.id}',headers=fw.xml))['campaign']['advertiser_id']


def getCampaignInsertionOrders(self,fw):
    ioList = []
    queryGet = XML(rs.get(f'https://api.freewheel.tv/services/v3/campaign/{self.id}/insertion_orders?per_page=50&page=1',headers = FW.xml))    
    for i in range(int(queryGet['insertion_orders']['@total_pages'])):
        loopGet = XML(rs.get(f'https://api.freewheel.tv/services/v3/campaign/{self.id}/insertion_orders?per_page=50&page={i+1}',headers = FW.xml)) 
        if type(loopGet['insertion_orders']['insertion_order']) == list :
            for item in loopGet['insertion_orders']['insertion_order']:
                if item['status'] == 'ACTIVE':
                    ioList.append(insertionOrderObj(id = item['id'], name = item['name'] ))
        elif type(loopGet['insertion_orders']['insertion_order']) == dict:
                if loopGet['insertion_orders']['insertion_order']['status'] == 'ACTIVE':
                    ioList.append(insertionOrderObj(id = loopGet['insertion_orders']['insertion_order']['id'], name = loopGet['insertion_orders']['insertion_order']['name'] ))
                    
    self.insertionOrders = ioList
    
class insertionOrderObj:

    def __init__(self ,**kwargs):
        dictObj  = {}
        for key, value in kwargs.items():
            
            dictObj[key] = value
            
        
        self.data = {'insertion_order' : dictObj}
        self.id = self.data['insertion_order']['id']
