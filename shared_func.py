import matplotlib.pyplot as plt
import numpy as np

def generate_html_report(title, content):
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{title}</title>
    </head>
    <body>
        <h1>{title}</h1>
        {content}
    </body>
    </html>
    """
    return html


def add_section(title, content):
    section = f"""
    <h2>{title}</h2>
    <p>{content}</p>
    """
    return section

def column_dropper(df, threshold):
  # Takes a dataframe and threshold for missing values. Returns a dataframe.
  total_records = df.count()
  for col in df.columns:
    # Calculate the percentage of missing values
    missing = df.where(df[col].isNull()).count()
    missing_percent = missing / total_records
    # Drop column if percent of missing is more than threshold
    if missing_percent > threshold:
      df = df.drop(col)
  return df
