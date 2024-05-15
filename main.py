import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
import requests
import pandas as pd
import json

global data,page_no,combined_df
combined_df = pd.DataFrame()
page_no = 1




app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
@repeat_every(seconds=60 * 10)  # 1 hour
async def refresh_the_api():
    import pandas as pd
    import json
    global data,page_no,combined_df
    print("data fetcher running.....")

    # Update the payload for each page
    url = "https://report.transexpress.lk/api/orders/delivery-success-rate/return-to-client-orders?page=1&per_page=10000"
        
    payload = {}
    headers = {
      'Cookie': 'development_trans_express_session=NaFDGzh5WQCFwiortxA6WEFuBjsAG9GHIQrbKZ8B'
    }
    
    response = requests.request("GET", url, headers=headers, data=payload)
    print(response.status_code)
    
    data = pd.json_normalize(response.json()["return_to_client_orders"]['data'])
        

 
    #data = combined_df[combined_df['status.name'].isin(['RETURN TO CLIENT', 'Delivered'])]
    data['status_name'] = data['status_name'].replace('Partially Delivered', 'Delivered')
    data['status_name'] = data['status_name'].replace('Received by Client', 'Returned to Client')
    data = data[['probability','status_name']]
    data = data[data['probability'].between(0, 100)]

    combined_df = pd.concat([combined_df, data], ignore_index=True)

    page_no = page_no + 1
    print("data collected....",page_no)
    return "data collected....",page_no
        





@app.get("/kpi_results")
async def kpi_result():


    global combined_df
    status_counts_more_than_80 = combined_df[combined_df['probability'] > 80]['status_name'].value_counts()
    
    status_counts_50_to_80 = combined_df[(combined_df['probability'] >= 50) & (combined_df['probability'] <= 80)]['status_name'].value_counts()
    
    status_counts_30_to_49 = combined_df[(combined_df['probability'] >= 30) & (combined_df['probability'] <= 49)]['status_name'].value_counts()
    
    status_counts_below_30 = combined_df[combined_df['probability'] < 30]['status_name'].value_counts()
    print(status_counts_more_than_80,status_counts_50_to_80,status_counts_30_to_49,status_counts_below_30)
    
    try:    
        status_counts_more_than_80_0 = int(status_counts_more_than_80[0])
    except:
        status_counts_more_than_80_0 = 0
        
    try:    
        status_counts_more_than_80_1 = int(status_counts_more_than_80[1])
    except:
        status_counts_more_than_80_1 = 0

        

    try:    
        status_counts_50_to_80_0 = int(status_counts_50_to_80[0])
    except:
        status_counts_50_to_80_0 = 0
        
    try:    
        status_counts_50_to_80_1 = int(status_counts_50_to_80[1])
    except:
        status_counts_50_to_80_1 = 0



        

    try:    
        status_counts_30_to_49_0 = int(status_counts_30_to_49[0])
    except:
        status_counts_30_to_49_0 = 0
        
    try:    
        status_counts_30_to_49_1 = int(status_counts_30_to_49[1])
    except:
        status_counts_30_to_49_1 = 0


        

    try:    
        status_counts_below_30_0 = int(status_counts_below_30[0])
    except:
        status_counts_below_30_0 = 0
        
    try:    
        status_counts_below_30_1 = int(status_counts_below_30[1])
    except:
        status_counts_below_30_1 = 0
        
    kpi_result = {
            "kpi_result": {
                "status_counts_more_than_80": {  
                    "correct_values": status_counts_more_than_80_0,
                    "incorrect_values": status_counts_more_than_80_1
                },
                "status_counts_50_to_80": {
                    "correct_values": status_counts_50_to_80_0,
                    "incorrect_values": status_counts_50_to_80_1
                },
                "status_counts_30_to_49": {
                    "correct_values": status_counts_30_to_49_0,
                    "incorrect_values": status_counts_30_to_49_1
                },
                "status_counts_below_30": {
                    "correct_values": status_counts_below_30_0,
                    "incorrect_values": status_counts_below_30_1
                }
            }
        }
    return kpi_result 

