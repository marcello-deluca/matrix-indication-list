from google import genai 
from google.genai import types
from io import StringIO
import pandas as pd
from functools import cache
import os
from tqdm import tqdm

test = True
limit = 1000

@cache
def generate(input_text):
    client = genai.Client(api_key=os.environ['GOOGLE_API_KEY'])
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=input_text,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=0) # no thinking
        ),
    )
    return response.text

def evaluate_sufficiency(df: pd.DataFrame, prompt: str) -> pd.DataFrame:
    
    new_col = []
    if test:
        df = df.head(limit)
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="assessing sufficiency of mondo"):
        if not test or idx < limit:
            extracted_concept = row['disease name']
            onto_concept = row['final normalized disease label']
            overall_prompt = f"{prompt}. CONCEPT 1 (extracted from label): {extracted_concept}. CONCEPT 2 (ontology concept): {onto_concept}."
            print(overall_prompt)
            try:
                response = generate(overall_prompt)
                print(response)
                new_col.append(response)
            except Exception as e:
                print(e)
                new_col.append("Error")


    df['disease ontology sufficient']=new_col
    return df