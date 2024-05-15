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


@app.get("/trigger_the_data_fecher_for_kpi")
async def get_data(page: str,paginate: str):

            
    print("data fetcher running.....")
            
    # Initialize an empty DataFrame to store the combined data
    combined_df = pd.DataFrame()
            
    # Update the payload for each page
    url = "https://report.transexpress.lk/api/orders/delivery-success-rate/return-to-client-orders?page="+page+"&per_page="+paginate
    
    payload = {}
    headers = {
      'Cookie': 'development_trans_express_session=NaFDGzh5WQCFwiortxA6WEFuBjsAG9GHIQrbKZ8B'
    }
            
    response = requests.request("GET", url, headers=headers, data=payload)
            
    # Sample JSON response
    json_response = response.json()
    # Extracting 'data' for conversion
    data = json_response["return_to_client_orders"]['data']

    data_count = len(data)  
    
    df = pd.json_normalize(data)
    
            
    df['status_name'] = df['status_name'].replace('Partially Delivered', 'Delivered')
    df['status_name'] = df['status_name'].replace('Received by Client', 'Returned to Client')
    df = df[['probability','status_name']]
    df = df[df['probability'].between(0, 100)]
    
    print("data collected from page : "+page)
    #return "done"
    try:
        file_path = 'data/data1.csv'  # Replace with your file path
        source_csv = pd.read_csv(file_path)
        new_data = df
        combined_df_final = pd.concat([source_csv,new_data], ignore_index=True)
    
        combined_df_final.to_csv("data/data1.csv")
        print("data added")
    except:
        
        df.to_csv("data/data1.csv")
        print("data created")

    print({"page_number":page,"data_count":data_count})
    return {"page_number":page,"data_count":data_count}

        





@app.get("/kpi_results")
async def kpi_result():
    file_path = 'data/data1.csv'  # Replace with your file path
    combined_df = pd.read_csv(file_path)

    
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

