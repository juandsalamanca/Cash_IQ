from openai import OpenAI
import json
import pandas as pd
from dotenv import load_dotenv
import os
from pydantic import BaseModel
from docx import Document
from docx.shared import Pt, RGBColor, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.section import WD_SECTION
from docx.oxml.ns import qn
from PIL import Image, ImageDraw, ImageFont
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


def send_data_to_llm(projection_data, date, data_path):

    client = OpenAI()

    system_prompt = """You are a highly skilled financial analyst. You are interpreting monthly data looking for points for
    improvement and also where things are going well. You have a professional, yet kind and supportive tone with expert insights."""

    prompt = f"""I'll provide for you transaction data for the aggregated and projected cash flow for a business. Taking into account today is {date},
    please provide a summary of the data with the following format: a 1 parragraph sumamry plus 4 or 5 suggestions, each with an observation,
    a recommendation based on said observation and a name for the suggestion.
    I'll also provide you with the file csv file so you can run code on it and get the analytics that you consider relevant

    Here is the data:
    {projection_data}"""

    upload_file = client.files.create(file=open(data_path, "rb"), purpose="user_data",
      expires_after={"anchor": "created_at", "seconds": 43200})

    file_id = upload_file.id
    print(file_id)

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt}
    ]

    response = client.responses.parse(
        model="gpt-5",
        input=messages,
        tools=[{"type":"code_interpreter", "container": {"type":"auto", "file_ids":[file_id]}}],
        text_format=CashIQSummary,
        #reasoning={"effort": "high"}
    )

    client.files.delete(file_id)

    return response.output_parsed

def create_front_page_image(client_name):

    # Load images
    background = Image.open("summary_data/4.png")  # full-page image
    overlay = Image.open("summary_data/Teal + name Crop.jpg")      # smaller image

    # Resize overlay if needed
    overlay = overlay.resize((1100, 300))  # adjust size

    # Get position (bottom-right)
    bg_width, bg_height = background.size
    ov_width, ov_height = overlay.size

    position = (bg_width - ov_width - 250, bg_height - ov_height - 250)  # 20px padding

    # Paste overlay
    background.paste(overlay, position, overlay if overlay.mode == "RGBA" else None)

    draw = ImageDraw.Draw(background)

    # Load font (make sure the font file exists)
    font = ImageFont.truetype("summary_data/Inter_28pt-Bold.ttf", 250)

    text_width, text_height = draw.textbbox((0, 0), client_name, font=font)[2:]

    position = (
        (bg_width - text_width - 800),
        (bg_height - text_height - 1200)
    )

    # Draw text
    draw.text(position, client_name, font=font, fill=(0, 0, 0))

    # Save result
    front_page_path = "summary_data/front_page.png"
    background.save(front_page_path)
    return front_page_path

def turn_summary_into_word_doc(client, summary_json):

    summary_path = "cash_iq_summary.docx"

    doc = Document()

    #--------------------------
    # Add Cover Page
    #--------------------------

    section = doc.sections[0]

    # Remove margins
    section.left_margin = 0
    section.right_margin = 0
    section.top_margin = 0
    section.bottom_margin = 0

    official_client_name_mapping = {
        "Trinity": "Trinity Logistics", 
        "Parisi": "Parisi", 
        "Luna": "Luna Locums", 
        "Strivewell": "Strivewell", 
        "Continuum": "Continuum", 
        "SupafitGrow": "SupafitGrow", 
        "Gamechanger": "Gamechanger"
    }

    client_name = official_client_name_mapping.get(client, client)

    front_page_path = create_front_page_image(client_name)

    # Add full-page image
    doc.add_picture(front_page_path, width=section.page_width, height=section.page_height)

    # --- Create new section for normal content ---
    new_section = doc.add_section(WD_SECTION.NEW_PAGE)

    # Reset margins for new section
    new_section.left_margin = Inches(1)
    new_section.right_margin = Inches(1)
    new_section.top_margin = Inches(1)
    new_section.bottom_margin = Inches(1)

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
                        run = p.add_run(f"{n+1}. {insight[subsection]}")
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


def get_summary(client, date, OUTPUT_XLSX):
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
    csv_path = OUTPUT_XLSX.replace(".xlsx", ".csv")
    projection_df.to_csv(csv_path, index=False)
    summary_json = send_data_to_llm(json.dumps(payload), date, csv_path).model_dump_json()
    summary_json = json.loads(summary_json)
    new_summary_json = {}
    new_summary_json["Summary"] = summary_json["summary"]
    new_summary_json["Insights and Suggestions"] = summary_json["insights_suggestions"]

    summary_path = turn_summary_into_word_doc(client, new_summary_json)

    with open(summary_path, "rb") as f:
        summary_bytes = f.read()

    return summary_bytes


if __name__ == "__main__":

    print(send_data_to_llm("_", "", "_"))