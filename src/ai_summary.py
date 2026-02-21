from openai import OpenAI
import json
import pandas as pd
from dotenv import load_dotenv
import os
from pydantic import BaseModel
import streamlit as st

load_dotenv()

if os.getenv("OPENAI_API_KEY") is None:
    os.environ["OPENAI_API_KEY"] = st.secrets["OPENAI_API_KEY"]

class InsightSuggestion(BaseModel):
    name: str
    observation: str
    recommendation: str

class CashIQSummary(BaseModel):
    summary: str
    insights_suggestions: list[InsightSuggestion]


def send_data_to_llm(projection_data, date):

    client = OpenAI()

    prompt = f"""I'll provide for you transaction data for the aggregated and projected cash flow for a business. Taking into account today is {date},
    please provide a summary of the data with the following format: a 1 parragraph sumamry plus 4 or 5 suggestions, each with an observation,
    a recommendation based on said observation and a name for the suggestion.
    
    Here is the data:
    {projection_data}"""

    messages = [
        {"role": "system", "content": "You are a data analyst that is able to understand financial data and provide insights."},
        {"role": "user", "content": prompt}
    ]

    response = client.responses.parse(
        model="gpt-5",
        input=messages,
        text_format=CashIQSummary,
        #reasoning={"effort": "high"}
    )

    return response.output_parsed

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
    summary_json = send_data_to_llm(json.dumps(payload), date).model_dump_json()
    summary_json = json.loads(summary_json)
    summary_text = ""
    summary_text += f"Summary:\n {summary_json['summary']}\n\n"
    for suggestion in summary_json["insights_suggestions"]:
        summary_text += f"{suggestion["name"]}:\n {suggestion["observation"]}\n {suggestion["recommendation"]}\n\n"
        #summary_df = pd.DataFrame({"Summary": [summary]})
    #with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl", mode="a", if_sheet_exists="new") as writer:
    #    summary_df.to_excel(writer, sheet_name="Summary", index=False)
    return summary_text

if __name__ == "__main__":

    print(send_data_to_llm("_", ""))