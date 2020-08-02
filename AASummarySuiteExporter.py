import datetime
import requests
import sys
import jwt
import httplib2

config = {
"apiKey":"3ec159485be87ed8fk6f9g37j79d67153b31e6",
"technicalAccountId":"6JCD048F50A6495F35C8D9D4D2@techacct.adobe.com",
"orgId":"25DB24210614E744C980A8A7@AdobeOrg",
"secret":"d033109-fd7a71ba2-489-9cf455-f2f87f4298ab",
"metascopes":"ent_analytics_bulk_ingest_sdk",
"imsHost":"ims-na1.adobelogin.com",
"imsExchange":"https://ims-na1.adobelogin.com/ims/exchange/jwt",
"discoveryUrl":"https://analytics.adobe.io/discovery/me",
"key":b'-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAN7wGu1P3aNA3yjqGA==\n-----END PRIVATE KEY-----',
"startdate": (datetime.datetime.today() - datetime.timedelta(days=8)).strftime("%Y-%m-%d"),
"enddate": (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
"sourcersids":["suite1","suite2"],
"targetrsid":"suite3",
"datasourcename":"Summary Import"
}

def get_jwt_token(config):
    return jwt.encode({
        "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=30),
        "iss": config["orgId"],
        "sub": config["technicalAccountId"],
        "https://{}/s/{}".format(config["imsHost"], config["metascopes"]): True,
        "aud": "https://{}/c/{}".format(config["imsHost"], config["apiKey"])
    }, config["key"], algorithm='RS256')

def get_access_token(config, jwt_token):
    post_body = {
        "client_id": config["apiKey"],
        "client_secret": config["secret"],
        "jwt_token": jwt_token
    }
    response = requests.post(config["imsExchange"], data=post_body)
    return response.json()["access_token"]

def get_first_global_company_id(config, access_token):
    response = requests.get(
        config["discoveryUrl"],
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apiKey"]
        }
    )
    return response.json().get("imsOrgs")[0].get("companies")[0].get("globalCompanyId")

jwt_token = get_jwt_token(config)
access_token = get_access_token(config, jwt_token)
global_company_id = get_first_global_company_id(config, access_token)

resultrows = []

for rsid in config["sourcersids"]:
    print("Fetching data for", rsid)
    result = requests.post(
            "https://analytics.adobe.io/api/"+global_company_id+"/reports",
            headers={
                "Authorization": "Bearer {}".format(access_token),
                "x-api-key": config["apiKey"],
                "x-proxy-global-company-id": global_company_id
            }, 
            json={
                "rsid": rsid,
                "globalFilters":[
                    {
                        "type":"dateRange",
                        "dateRange":config["startdate"]+"T00:00:00.000/"+config["enddate"]+"T23:59:59.999"
                    }
                ],
                "metricContainer": {
                    "metrics": [
                        {
                            "columnId": "Visitors",
                            "id": "metrics/visitors"
                        },
                        {
                            "columnId": "Visits",
                            "id": "metrics/visits"
                        },
                        {
                            "columnId": "Page Views",
                            "id": "metrics/pageviews"
                        }
                    ]
                },
                "dimension":"variables/daterangeday",
                "settings":{
                    "dimensionSort":"asc",
                    "limit":"50000"
                }
            }
        ).json()
        
    for row in result["rows"]:
        values = []
        for value in row["data"]:
            values.append(str(value))
        date = datetime.datetime.strptime(row["value"],"%b %d, %Y").strftime("%m/%d/%Y/00/00/00")
        values.insert(0,rsid)
        values.insert(0,date)
        resultrows.append(values)

dataSources = requests.post(
        "https://api.omniture.com/admin/1.4/rest/?method=DataSources.Get",
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apiKey"],
            "x-proxy-global-company-id": global_company_id
        }, 
        data={'reportSuiteID':config["targetrsid"]}
    ).json()

for dataSource in dataSources:
    if dataSource["name"] == config["datasourcename"]:
        dataSourceID = dataSource["id"]
        print("Found Data Source ID")
        break

jobresponse = requests.post(
    "https://api.omniture.com/admin/1.4/rest/?method=DataSources.UploadData",
    headers={
        "Authorization": "Bearer {}".format(access_token),
        "x-api-key": config["apiKey"],
        "x-proxy-global-company-id": global_company_id
    }, 
    json={
        "columns": ["Date","Evar 1","Event 1","Event 2","Event 3"],
        'reportSuiteID': config["targetrsid"],
        'dataSourceID':dataSourceID,
        "finished": True,
        "jobName": "Summary Import",
        "rows": resultrows
    }
)

print(jobresponse.json())
