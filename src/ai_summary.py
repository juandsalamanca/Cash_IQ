from openai import OpenAI
import json
import pandas as pd
from dotenv import load_dotenv
import os
import streamlit as st

load_dotenv()

if os.getenv("OPENAI_API_KEY") is None:
    os.environ["OPENAI_API_KEY"] = st.secrets["OPENAI_API_KEY"]


def send_data_to_llm(projection_data, date):

    client = OpenAI()

    prompt = f"""I'll provide for you transaction data for the aggregated and projected cash flow for a business. Taking into account today is {date},
    please provide a summary of the data, including any insights or trends you can identify. Additionally, highlight any potential areas for improvement or opportunities 
    for growth based on the data provided.
    
    Here is the data:
    {projection_data}"""

    messages = [
        {"role": "system", "content": "You are a data analyst that is able to understand financial data and provide insights."},
        {"role": "user", "content": prompt}
    ]

    response = client.responses.create(
        model="gpt-5",
        input=messages,
        #reasoning={"effort": "high"}
    )

    return response.output[1].content[0].text

def get_summary(date, OUTPUT_XLSX):
    # Option 1: records format (most common)

    projection_df = pd.read_excel(OUTPUT_XLSX, sheet_name="Projections (Table)")

    projections_json = projection_df.to_dict(orient="records")

    payload = {
        "metadata": {
            "columns": projection_df.columns.tolist(),
            "row_count": len(projection_df)
        },
        "data": projections_json
    }
    summary = send_data_to_llm(json.dumps(payload), date)

    #summary_df = pd.DataFrame({"Summary": [summary]})
    #with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl", mode="a", if_sheet_exists="new") as writer:
    #    summary_df.to_excel(writer, sheet_name="Summary", index=False)
    return summary

if __name__ == "__main__":

    print(send_data_to_llm("_", ""))