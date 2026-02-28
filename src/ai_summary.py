from openai import OpenAI
import json
import pandas as pd
from dotenv import load_dotenv
import os
from pydantic import BaseModel
from docx import Document
from docx.shared import Pt, RGBColor, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
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

    if os.getenv("server") == "qa":
        model = "gpt-5-mini"
    else:
        model = "gpt-5"

    print("Using model:", model)

    response = client.responses.parse(
        model=model,
        input=messages,
        text_format=CashIQSummary,
        #reasoning={"effort": "high"}
    )

    return response.output_parsed


def turn_summary_into_word_doc(summary_json):

    summary_path = "cash_iq_summary.docx"

    doc = Document()

    #--------------------------
    # Add Title
    #--------------------------

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER

    # Add run (text container)
    run = p.add_run("CashIQ")
    run.bold = True
    run.font.size = Pt(36)

    # Set font to Inter
    run.font.name = "Inter"
    run._element.rPr.rFonts.set(qn('w:eastAsia'), "Inter")

    # Set color #298478
    run.font.color.rgb = RGBColor(0x29, 0x84, 0x78)

    #--------------------------
    # Add each section
    #--------------------------

    # Add heaedr

    for section in summary_json:

        # Main headers

        p = doc.add_paragraph()

        run = p.add_run(section)
        run.bold = True
        run.font.size = Pt(24)
        run.font.name = "Inter"
        run._element.rPr.rFonts.set(qn('w:eastAsia'), "Inter")
        run.font.color.rgb = RGBColor(0x29, 0x84, 0x78)

        if isinstance(summary_json[section], str):
            p = doc.add_paragraph()
            p.paragraph_format.left_indent = Inches(0.5)
            run = p.add_run(summary_json[section])
            run.font.size = Pt(12)
            run.font.name = "Inter"
            run._element.rPr.rFonts.set(qn('w:eastAsia'), "Inter")

        # Insights and suggestions

        elif isinstance(summary_json[section], list):
            for n, insight in enumerate(summary_json[section]):
                for subsection in insight:

                    # Subsection header
                    if subsection == "name":
                        p = doc.add_paragraph()
                        run = p.add_run(f"{n+1}.{insight[subsection]}")
                        run.bold = True
                        run.font.size = Pt(18)
                        run.font.name = "Inter"
                        run._element.rPr.rFonts.set(qn('w:eastAsia'), "Inter")
                        run.font.color.rgb = RGBColor(0x29, 0x84, 0x78)

                    # Subsection content
                    elif subsection in ["observation", "recommendation"]:

                        spl = insight[subsection].split(". ")

                        for sentence in spl:
                            bullet_style = doc.styles["List Bullet"]
                            bullet_style.font.name = "Inter"
                            bullet_style.font.size = Pt(12)
                            doc.add_paragraph(sentence, style="List Bullet")
                            

    doc.save(summary_path)

    return summary_path


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
    new_summary_json = {}
    new_summary_json["Summary"] = summary_json["summary"]
    new_summary_json["Insights and Suggestions"] = summary_json["insights_suggestions"]

    summary_path = turn_summary_into_word_doc(new_summary_json)

    with open(summary_path, "rb") as f:
        summary_bytes = f.read()

    return summary_bytes


if __name__ == "__main__":

    print(send_data_to_llm("_", ""))