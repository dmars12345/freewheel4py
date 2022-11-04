import xmltodict
from dict2xml import dict2xml
import requests as rs
class fw():
    def __init__(self,username,password):

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


            
class placementObj():
    def __init__(self ,**kwargs):
        dictObj  = {}
        for key, value in kwargs.items():
            
            dictObj[key] = value
            

        self.data = {'placement' : dictObj}
        
        
    def Get(self,fw):
        node = 'all'
        get_url = f"https://api.freewheel.tv/services/v3/placements/{self.data['placement']['id']}?show={node}"
        get_placement = rs.get(get_url,headers=fw.xml).text
        self.data = xmltodict.parse(get_placement,dict_constructor=dict)
    
        return self
    
    def getPacing(self,fw):
        get = rs.get(f"https://api.freewheel.tv/services/v4/placements/{self.data['placement']['id']}/forecasts?type=nightly",headers =fw.json).json()
        get['budget'] = get['budget'].replace(',','').replace(' imps',"")
        self.data['placement']['pacing'] = get
        
    def updateGrossImpressionCap(self, cap , fw):
    
        if self.data['placement']['budget']['budget_model'] == 'DEMOGRAPHIC_IMPRESSION_TARGET':
            self.data['placement']['budget']['gross_impression_cap'] = cap
            budgetDict = {}
            for item in self.data['placement']['budget']:
                if self.data['placement']['budget'][item] is not None:
                    budgetDict[item] = self.data['placement']['budget'][item]
                elif self.data['placement']['budget'][item] is None:
                    continue
            
            self.put = XML({'placement': {'budget' : budgetDict }})
            put = XML(rs.put(f"https://api.freewheel.tv/services/v3/placements/{self.data['placement']['id']}",headers = fw.xml,data = self.put))
            return put
    
        

        

        


def XML (req):
    if type(req) == dict:
        item = dict2xml(req)
    else:
        item = xmltodict.parse(req.text,dict_constructor=dict)
    return item
