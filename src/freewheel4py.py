import io
import json
from io import StringIO
from json import JSONDecodeError
from pathlib import Path
import boto3
import pandas as pd
import requests as rs
import xmltodict
from dict2xml import dict2xml


def FreewheelDmaCodes ():
    dma_dict = {'Juneau, AK': 210,
 'Greenwood-Greenville, MS': 209,
 'Spokane, WA': 208,
 'Chico-Redding, CA': 207,
 'Fresno-Visalia, CA': 206,
 'Sacramento-Stockton-Modesto, CA': 205,
 'Santa Barbara-San Marcos-San Luis Obispo, CA': 204,
 'Las Vegas, NV': 203,
 'Monterey-Salinas, CA': 202,
 'San Diego, CA': 201,
 'Bend, OR': 200,
 'Portland, OR': 199,
 'Seattle-Tacoma, WA': 198,
 'Medford-Klamath Falls, OR': 197,
 'Reno, NV': 196,
 'Yakima-Pasco-Richland-Kennewick, WA': 195,
 'San Francisco-Oakland-San Jose, CA': 194,
 'Palm Springs, CA': 193,
 'Los Angeles, CA': 192,
 'Eureka, CA': 191,
 'Eugene, OR': 190,
 'Bakersfield, CA': 189,
 'Glendive, MT': 188,
 'Albuquerque-Santa Fe, NM': 187,
 'Tucson-Sierra Vista, AZ': 186,
 'Grand Junction-Montrose, CO': 185,
 'Yuma-El Centro, AZ': 184,
 'Salt Lake City, UT': 183,
 'Casper-Riverton, WY': 182,
 'Helena, MT': 181,
 'El Paso-Las Cruces, TX': 180,
 'Rapid City, SD': 179,
 'Missoula, MT': 178,
 'Twin Falls, ID': 177,
 'Cheyenne-Scottsbluff, WY': 176,
 'Idaho Falls-Pocatello-Jackson, ID': 175,
 'Boise, ID': 174,
 'Billings, MT': 173,
 'Great Falls, MT': 172,
 'Butte-Bozeman, MT': 171,
 'Phoenix-Prescott, AZ': 170,
 'Colorado Springs-Pueblo, CO': 169,
 'Denver, CO': 168,
 'Laredo, TX': 167,
 'Biloxi-Gulfport, MS': 166,
 'Fairbanks, AK': 165,
 'Honolulu, HI': 164,
 'Anchorage, AK': 163,
 'North Platte, NE': 162,
 'Mankato, MN': 161,
 'Bowling Green, KY': 160,
 'Jonesboro, AR': 159,
 'Sioux Falls-Mitchell, SD': 158,
 'Fargo-Valley City, ND': 157,
 'Lincoln-Hastings-Kearny, NE': 156,
 'Jackson, MS': 155,
 'Quincy-Hannibal-Keokuk, IL': 154,
 'Baton Rouge, LA': 153,
 'Meridian, MS': 152,
 'Hattiesburg-Laurel, MS': 151,
 'Tyler-Longview-Jacksonville(Lufkin-Nacogdoches), TX': 150,
 'Wausau-Rhinelander, WI': 149,
 'La Crosse-Eau Claire, WI': 148,
 'Montgomery-Selma, AL': 147,
 'Little Rock-Pine Bluff, AR': 146,
 'Beaumont-Port Arthur, TX': 145,
 'Huntsville-Decatur, AL': 144,
 'Minot-Bismarck-Dickinson, ND': 143,
 'Mobile, AL-Pensacola, FL': 142,
 'Davenport-Rock Island-Moline, IL': 141,
 'Des Moines-Ames, IA': 140,
 'Wichita-Hutchinson, KS': 139,
 'Duluth-Superior, MN': 138,
 'Peoria-Bloomington, IL': 137,
 'Columbus-Tupelo-West Point-Houston, MS': 136,
 'Tulsa, OK': 135,
 'Ft. Smith-Fayetteville-Springfield-Rogers, AR': 134,
 'Madison, WI': 133,
 'Abilene-Sweetwater, TX': 132,
 'San Angelo, TX': 131,
 'Nashville, TN': 130,
 'Green Bay-Appleton, WI': 129,
 'Sherman, TX-Ada, OK': 128,
 'Panama City, FL': 127,
 'Omaha, NE': 126,
 'Lubbock, TX': 125,
 'Oklahoma City, OK': 124,
 'Evansville, IN': 123,
 'Champaign-Springfield-Decatur, IL': 122,
 'Alexandria, LA': 121,
 'Lake Charles, LA': 120,
 'Lafayette, LA': 119,
 'San Antonio, TX': 118,
 'Memphis, TN': 117,
 'Jackson, TN': 116,
 'St Joseph, MO': 115,
 'Cedar Rapids-Waterloo-Iowa City-Dubuque, IA': 114,
 'Harlingen-Weslaco-Brownsville-McAllen, TX': 113,
 'Austin, TX': 112,
 'Amarillo, TX': 111,
 'Odessa-Midland, TX': 110,
 'Paducah, KY-Cape Girardeau, MO-Harrisburg-Mt Vernon, IL': 109,
 'Ottumwa-Kirksville, IA': 108,
 'Birmingham-Anniston-Tuscaloosa, AL': 107,
 'Monroe, LA-El Dorado, AR': 106,
 'Wichita Falls-Lawton, TX': 105,
 'Victoria, TX': 104,
 'Waco-Temple-Bryan, TX': 103,
 'Sioux City, IA': 102,
 'Dallas-Fort Worth, TX': 101,
 'New Orleans, LA': 100,
 'Springfield, MO': 99,
 'Houston, TX': 98,
 'Milwaukee, WI': 97,
 'Kansas City, MO': 96,
 'Minneapolis-St Paul, MN': 95,
 'Shreveport, LA': 94,
 'Rochester-Mason City-Austin, MN': 93,
 'Rockford, IL': 92,
 'St Louis, MO': 91,
 'Dothan, AL': 90,
 'Topeka, KS': 89,
 'Columbia-Jefferson City, MO': 88,
 'Joplin-Pittsburg, MO': 87,
 'Chicago, IL': 86,
 'Corpus Christi, TX': 85,
 'Clarksburg-Weston, WV': 84,
 'Parkersburg, WV': 83,
 'Zanesville, OH': 82,
 'Gainesville, FL': 81,
 'South Bend-Elkhart, IN': 80,
 'Charlottesville, VA': 79,
 'Alpena, MI': 78,
 'Lafayette, IN': 77,
 'Terre Haute, IN': 76,
 'Wilkes Barre-Scranton-Hazleton, PA': 75,
 'Salisbury, MD': 74,
 'Chattanooga, TN': 73,
 'Johnstown-Altoona-State College, PA': 72,
 'Roanoke-Lynchburg, VA': 71,
 'Ft. Myers-Naples, FL': 70,
 'Myrtle Beach-Florence, SC': 69,
 'Harrisonburg, VA': 68,
 'Greenville-Spartanburg-Asheville, SC': 67,
 'Harrisburg-Lancaster-Lebanon-York, PA': 66,
 'Elmira-Corning, NY': 65,
 'Charleston-Huntington, WV': 64,
 'Grand Rapids-Kalamazoo-Battle Creek, MI': 63,
 'Jacksonville, FL': 62,
 'Raleigh-Durham-Fayetteville, NC': 61,
 'Bluefield-Beckley-Oak Hill, WV': 60,
 'Lima, OH': 59,
 'Knoxville, TN': 58,
 'Richmond-Petersburg, VA': 57,
 'Syracuse, NY': 56,
 'Wheeling, WV-Steubenville, OH': 55,
 'Marquette, MI': 54,
 'Presque Isle, ME': 53,
 'Lansing, MI': 52,
 'Wilmington, NC': 51,
 'Watertown, NY': 50,
 'West Palm Beach-Ft. Pierce, FL': 49,
 'Toledo, OH': 48,
 'Columbia, SC': 47,
 'Greenville-New Bern-Washington, NC': 46,
 'Norfolk-Portsmouth, VA': 45,
 'Springfield-Holyoke, MA': 44,
 'Dayton, OH': 43,
 'Lexington, KY': 42,
 'Traverse City-Cadillac, MI': 41,
 'Tampa-St. Petersburg-Sarasota, FL': 40,
 'Rochester, NY': 39,
 'Bangor, ME': 38,
 'Youngstown-Warren, OH': 37,
 'Columbus, OH': 36,
 'Orlando-Daytona Beach-Melbourne, FL': 35,
 'Hartford-New Haven, CT': 34,
 'Albany-Schenectady-Troy, NY': 33,
 'Tri-Cities, TN': 32,
 'Tallahassee-Thomasville, FL': 31,
 'Louisville, KY': 30,
 'Miami-Ft. Lauderdale, FL': 29,
 'Indianapolis, IN': 28,
 'Utica-Rome, NY': 27,
 'Albany, GA': 26,
 'Atlanta, GA': 25,
 'Burlington, VT-Plattsburgh, NY': 24,
 'Columbus, GA-Opelika, AL': 23,
 'Providence-New Bedford, RI': 22,
 'Augusta-Aiken, GA-SC': 21,
 'Charleston, SC': 20,
 'Winston Salem-Greensboro-High Point, NC': 19,
 'Charlotte, NC-SC': 18,
 'Erie, PA': 17,
 'Cincinnati, OH': 16,
 'Buffalo, NY': 15,
 'Flint-Saginaw-Bay City, MI': 14,
 'Baltimore, MD': 13,
 'Washington-Hagerstown, DC': 12,
 'Cleveland-Akron-Canton, OH': 11,
 'Ft Wayne, IN': 10,
 'Pittsburgh, PA': 9,
 'Savannah, GA-SC': 8,
 'Boston-Manchester, MA': 7,
 'Detroit, MI': 6,
 'Philadelphia, PA': 5,
 'Macon, GA': 4,
 'Binghamton, NY': 3,
 'New York, NY': 2,
 'Portland-Auburn, ME': 1}
    return dma_dict

import io
import json
from io import StringIO
from json import JSONDecodeError
from pathlib import Path
import boto3
import pandas as pd
import requests as rs
import xmltodict
from dict2xml import dict2xml


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
                    placement = Placement(freewheel_auth = fw,attach = attach)
                    placement.Get()
                    listy.append(placement.dict)
                else:
                    PID = item['id']
                    pids.append(PID)
                    attach = {'attach' : 'Yes', "PID" : PID}
                    placement = Placement(freewheel_auth = fw,attach = attach)
                    placement.Get()
                    listy.append(placement)

        
        self.PIDs = pids           
        self.list = listy
        

        
        
        
    def GetPacing(self):
        
        nightly = []
        headers = self.freewheel_auth.json
        for item in query.PIDs:
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
        for i in range(len(query.list)):
            budget = query.Pacing[i]['budget']
    
            if ',' in budget:
                budget = int("".join(query.Pacing[i]['budget'].replace('imps', '').replace(' ', "").split(',')))
            else:
                budget = int(budget)
                
            try:
                
                start = PlacementStringQuery.list[i]['placement']['schedule']['start_time']
                end = PlacementStringQuery.list[i]['placement']['schedule']['end_time']
                
            except KeyError:
                start = 'None'
                end  = 'None'
        
            pacing_dict[query.list[i]['placement']['name']] = {'OSI' : query.Pacing[i]['on_schedule_indicator']/100,
    'FFDR': query.Pacing[i]['forecast_final_delivery_rate']/100,
    'grossDeliveredImps' : query.Pacing[i]['delivered_impressions'],
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

        PID = self.PID
        headers = self.freewheel_auth.xml
        node = 'all'
        get_url = f'https://api.freewheel.tv/services/v3/placements/{str(PID)}?show={node}'
        get_placement = rs.get(get_url,headers=headers).text
        self.dict = xmltodict.parse(get_placement,dict_constructor=dict)
        self.aud =self.dict['placement']['audience_targeting']
        self.content = self.dict['placement']['content_targeting']
#             include = 'include'
#             exclude = 'exclude'
#             self.contentInclusions= self.dict['placement']['content_targeting'][include]
#             self.contentExclusions = self.dict['placement']['content_targeting'][exclude]
#             self.sets = self.contentInclusions['set']
#             self.numberOfSets = len(self.contentInclusions['set'])
#             try:
#                 self.exclusionFwItems = list(self.contentExclusions.keys())
#             except AttributeError:
#                 self.exclusionFwItems = 'None'

#             try:
#                 keys = list(self.sets.keys())
#                 for item in keys:
#                     print(item)
#                 self.set = self.sets
#                 self.numberOfsets = 1
#             except AttributeError:

#                 keys = self.sets

#                 if len(keys) == 3:
#                     self.setOne = keys[0]
#                     self.setOneFwItems = list(keys[0].keys())
#                     self.setTwo = keys[1]
#                     self.setTwoFwItems = list(keys[1].keys())
#                     self.setThree = keys[2]
#                     self.setThreeFwItems = list(keys[2].keys())
#                 if len(keys) ==2: 
#                     self.setOne = keys[0]
#                     self.setOneFwItems =list(keys[0].keys())               
#                     self.setTwo = keys[1]
#                     self.setTwoFwItems = list(keys[1].keys())
#         except KeyError:
#             PID = self.PID
#             headers = self.freewheel_auth.xml
#             node = 'all'
#             get_url = f'https://api.freewheel.tv/services/v3/placements/{str(PID)}?show={node}'
#             get_placement = rs.get(get_url,headers=headers).text
#             self.dict = xmltodict.parse(get_placement,dict_constructor=dict)
                


            

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
        
        
            
            
        
        
        
        
        
    
class FreewheelAdvertiser():
    def __init__(self,Name,ID):
        self.Name = Name
        self.ID = ID
        
class  FreewheelAdvertiserList():
    def __init__(self,freewheel_auth):
    
        fw = freewheel_auth
        url = 'https://api.freewheel.tv/services/v3/advertisers?status=ACTIVE&page=1&per_page=50'
        get = xmltodict.parse(rs.get(url,headers = fw.xml).text,dict_constructor = dict)

        audience_list = []
        numberOfPages = int(get['advertisers']['@total_pages'])
        print(numberOfPages)

        for i in range(numberOfPages):
            try:
                url = f'https://api.freewheel.tv/services/v3/advertisers?status=ACTIVE&page={i+1}&per_page=50'
                get = xmltodict.parse(rs.get(url,headers = fw.xml).text,dict_constructor = dict)
                for item in get['advertisers']['advertiser']:
                    FWA = FreewheelAdvertiser(Name = item['name'], ID = item['id'])
                    audience_list.append(FWA)
            except ConnectionError:
                continue

        self.list = audience_list
        
        
    
class FreewheelCampaign():
    def __init__(self,Name,ID):
        self.Name = Name
        self.ID = ID
        
class  FreewheelCampaignList():
    def __init__(self,freewheel_auth):
    
        fw = freewheel_auth
        url = 'https://api.freewheel.tv/services/v3/campaigns?status=ACTIVE&page=1&per_page=50'
        get = xmltodict.parse(rs.get(url,headers = fw.xml).text,dict_constructor = dict)

        audience_list = []
        numberOfPages = int(get['campaigns']['@total_pages'])
        print(numberOfPages)

        for i in range(numberOfPages):
            url = f'https://api.freewheel.tv/services/v3/campaigns?status=ACTIVE&page={i+1}&per_page=50'
            try:
                get = xmltodict.parse(rs.get(url,headers = fw.xml).text,dict_constructor = dict)
                for item in get['campaigns']['campaign']:
                    FWA = FreewheelCampaign(Name = item['name'], ID = item['id'])
                    audience_list.append(FWA)
            except ConnectionError:
                continue 

        self.list = audience_list
        
        
        
class FreewheelIO():
    def __init__(self,Name,ID):
        self.Name=  Name
        self.ID = ID
        
    

def PushIO(freewheel_auth,FreewheelAdvertiser,FreewheelCampaign,name,status):
    fw = freewheel_auth
    adv = FreewheelAdvertiser
    camp =FreewheelCampaign

    url = f'https://api.freewheel.tv/services/v3/campaign/{camp.ID}/insertion_order'
    data = {'insertion_order': {'name' : name, 'status' : status, 'advertiser_id' : adv.ID}}
    io = xmltodict.parse(rs.post(url,headers=fw.xml,data=dict2xml(data)).text,dict_constructor = dict)

    IO = FreewheelIO(Name = name, ID = io['insertion_order']['id'])

    IO.status = status
    IO.campaign = io['insertion_order']['campaign_id']

    return IO


def CopyPlacement(freewheel_auth,name,FreewheelIO,targetPID,targets):
    fw = freewheel_auth
    IO = FreewheelIO
    get_url = f'https://api.freewheel.tv/services/v3/placements/{str(targetPID)}?show=all'
    get = xmltodict.parse(rs.get(get_url,headers=fw.xml).text,dict_constructor = dict)
    placement = {'placement': {'name': name,
                              'insertion_order_id' : IO.ID}}
    for ITEM in targets:
        print(ITEM)

        if ITEM == 'content_targeting':
            content = get['placement'][ITEM]
            if type(content['include']['set']) == dict:
                del content['include']['relation_between_sets']
            if type(content['include']['remaining_items']) ==  type(None):
                del content['include']['remaining_items']

            placement['placement']['content_targeting'] = content 

        elif ITEM == 'ad_product':
            adUnitList = []
            ad_product = get['placement'][ITEM]
            for item in ad_product['ad_unit_node']:
                nodeDict = {}
                for key in item.keys():
                    if type(item[key]) == type(None):
                        continue
                    elif key != 'ad_unit_node_id':
                        nodeDict[key] = item[key]

                adUnitList.append(nodeDict)
            linkMethod = get['placement']['ad_product']['link_method']

            placement['placement']['ad_product'] = {'link_method': linkMethod, 'ad_unit_node': adUnitList }
        elif ITEM == 'budget':
            copyBudget = get['placement']['budget']
            if copyBudget['budget_model'] == 'IMPRESSION_TARGET':
                targetBudget = {}
                for item in copyBudget.keys():
                    if type(None) == type(copyBudget[item]):
                        continue
                    else:
                        targetBudget[item] = copyBudget[item]
            
            placement['placement']['budget'] = targetBudget

            
        elif ITEM == 'delivery':
            copyDelivery = get['placement']['delivery']
            targetDelivery = {}
            for item in copyDelivery.keys():
                if type(None) == type(copyDelivery[item]):
                    continue
                else:
                    targetDelivery[item] = copyDelivery[item]
                    
            placement['placement']['delivery'] = targetDelivery
            
        
    createpid = rs.post('https://api.freewheel.tv/services/v3/placement/create',headers=fw.xml,data=dict2xml(placement))
    pid = xmltodict.parse(createpid.text,dict_constructor=dict)['placement']['id']

    copyPlacement =  Placement(fw,IO= IO ,attach= {'attach' : 'No'})
    copyPlacement.Name = name
    copyPlacement.PID = pid


    if 'ad_product' in targets:
        getCopyUrl = f'https://api.freewheel.tv/services/v3/placements/{copyPlacement.PID}?show=all'
        getCopy = xmltodict.parse(rs.get(getCopyUrl,headers=fw.xml).text,dict_constructor = dict)
        creativeMap = []

        for au in getCopy['placement']['ad_product']['ad_unit_node']:

            creativeMap.append({'node' : au['ad_unit_node_id'], 'unit': au['ad_unit_id'] , 'creative' : []})



    copyPlacement.creativeMap = creativeMap
    copyPlacement.Budget = targetBudget
    copyPlacement.Delivery = targetDelivery

    return copyPlacement



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
            
            
    def UpdateFlight(self,startDate,endDate):
        time = {'time_zone': '(GMT-05:00) America - New York',
 'start_time':startDate,
 'end_time': endDate}

        data = dict2xml({'placement':{'schedule' : time}})

        put = rs.put(f'https://api.freewheel.tv/services/v3/placements/{self.PID}',headers=self.freewheel_auth.xml,data=data)
        return xmltodict.parse(put.text,dict_constructor = dict)
                
    def AttachCreatives(self):
        putList = []
        for item in self.creativeMap:
            aun = item['node']
            for cre in item['creative']:
                cput = rs.put(f'https://api.freewheel.tv/services/v3/ad_unit_nodes/{aun}/creatives/{cre}.xml',headers=fw.xml).text
                putList.append(xmltodict.parse(cput,dict_constructor = dict))

        return putList
    
    
    def includeDmaTargeting(self,list_of_codes):

        headers = self.freewheel_auth.xml
        geo_type = 'dma'
        inc_exc  = 'include'
        dict_obj = {}
        dict_obj['geography_targeting'] = {inc_exc :{geo_type: list_of_codes }}
        data = dict2xml({'placement' : dict_obj})
        put = rs.put(f'https://api.freewheel.tv/services/v3/placements/{self.PID}',headers=headers,data=data).text
        return xmltodict.parse(put,dict_constructor = dict)

    def excludeDmaTargeting(self,list_of_codes):

        headers = self.freewheel_auth.xml
        geo_type = 'dma'
        inc_exc  = 'exclude'
        dict_obj = {}
        dict_obj['geography_targeting'] = {inc_exc :{geo_type: list_of_codes }}
        data = dict2xml({'placement' : dict_obj})
        put = rs.put(f'https://api.freewheel.tv/services/v3/placements/{self.PID}',headers=headers,data=data).text
        return xmltodict.parse(put,dict_constructor = dict)


    def excludeStateTargeting(self,list_of_codes):

        headers = self.freewheel_auth.xml
        geo_type = 'state'
        inc_exc  = 'exclude'
        dict_obj = {}
        dict_obj['geography_targeting'] = {inc_exc :{geo_type: list_of_codes }}
        data = dict2xml({'placement' : dict_obj})
        put = rs.put(f'https://api.freewheel.tv/services/v3/placements/{self.PID}',headers=headers,data=data).text
        return xmltodict.parse(put,dict_constructor = dict)

    def includeStateTargeting(self,list_of_codes):

        headers = self.freewheel_auth.xml
        geo_type = 'state'
        inc_exc = 'include'
        dict_obj = {}
        dict_obj['geography_targeting'] = {inc_exc :{geo_type: list_of_codes }}
        data = dict2xml({'placement' : dict_obj})
        put = rs.put(f'https://api.freewheel.tv/services/v3/placements/{self.PID}',headers=headers,data=data).text
        return xmltodict.parse(put,dict_constructor = dict)


