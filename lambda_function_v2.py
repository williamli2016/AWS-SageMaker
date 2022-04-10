
import boto3
import math
import dateutil
import json
import os

ENDPOINT_NAME  = os.environ["ENDPOINT_NAME"]
client = boto3.client(service_name="sagemaker-runtime")
target_col_name = "HeartDisease"

col_names_text = "HeartDisease,BMI,Smoking,AlcoholDrinking,Stroke,PhysicalHealth,MentalHealth,DiffWalking,Sex,AgeCategory,Race,Diabetic,PhysicalActivity,GenHealth,SleepTime,Asthma,KidneyDisease,SkinCancer"
col_names = col_names_text.split(",")

def map_age(age_category):
    age_mapper = {
        "18-24":0,
        "25-29":1,
        "30-34":2,
        "35-39":3,
        "40-44":4,
        "45-49":5,
        "50-54":6,
        "55-59":7,
        "60-64":8,
        "65-69":9,
        "70-74":10,
        "75-79":11,
        "80 or older":12
    }
    return age_mapper[age_category]

def map_race(race):
    race_mapper = {
        "American Indian/Alaskan Native": 0,
        "Asian": 1,
        "Black": 2,
        "Hispanic": 3,
        "Other": 4,
        "White": 5 
    }
    return race_mapper[race]

def map_gen_health(gen_health):
    gen_health_mapper = {
        "Excellent": 0,
        "Fair": 1,
        "Good": 2,
        "Poor": 3,
        "Very good": 4
    }
    return gen_health_mapper[gen_health]
    
def inverse_map_heart_disease(heart_disease_num):
    if heart_disease_num == 1:
        return "Yes"
    else:
        return "No"

def transform_data(data):
    try:
        HeartDisease = 1 if data[0] == "Yes" else 0
        BMI = data[0]
        Smoking = 1 if data[1] == "Yes" else 0
        AlcoholDrinking = 1 if data[2] == "Yes" else 0
        Stroke = 1 if data[3] == "Yes" else 0
        PhysicalHealth = data[4]
        MentalHealth = data[5]
        DiffWalking = 1 if data[6] == "Yes" else 0
        Sex = 1 if data[7] == 'Male' else 0
        AgeCategory = map_age(data[8])
        Race = map_race(data[9])
        Diabetic = 1 if data[10] == 'Yes' else 0
        PhysicalActivity = 1 if data[11] == 'Yes' else 0
        GenHealth = map_gen_health(data[12])
        SleepTime = data[13]
        Asthma = 1 if data[14] == 'Yes' else 0
        KidneyDisease = 1 if data[15] == 'Yes' else 0
        SkinCancer = 1 if data[16] == 'Yes' else 0
        features = [
            HeartDisease,
            BMI,
            Smoking,
            AlcoholDrinking,
            Stroke,
            PhysicalHealth,
            MentalHealth,
            DiffWalking,
            Sex,
            AgeCategory,
            Race,
            Diabetic,
            PhysicalActivity,
            GenHealth,
            SleepTime,
            Asthma,
            KidneyDisease,
            SkinCancer
        ]
        return ",".join([str(f) for f in features[1:]])
                  
    except Exception as err:
        print("Error when transforming: {0}, {1}".format(data, err))
        raise Exception("Error when transforming: {0}, {1}".format(data, err))
    
def lambda_handler(event, context):
    try:
        print("Received event: " + json.dumps(event, indent=2))
        request = json.loads(json.dumps(event))
        
        transformed_data = [transform_data(instance["features"]) for instance in request["instances"]]
        
        result = client.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                        Body=("\n".join(transformed_data).encode("utf-8")),
                                        ContentType="text/csv")
                                        
        result = result["Body"].read().decode("utf-8")
        print(result)
        # predictions = [inverse_map_heart_disease(float(r)) for r in result]
        predictions = [inverse_map_heart_disease(float(r)) for r in result.split(",")]
        
        return {
            "statusCode": 200,
            "isBase64Encoded": False,
            "body": json.dumps(predictions)
        }
        
    except Exception as err:
        return {
            "statusCode": 400,
            "isBase64Encoded": False,
            "body": "Call Failed {0}".format(err)
        }