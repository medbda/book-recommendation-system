import pandas as pd

df = pd.read_parquet("/home/vaibhavi/spark-ml-venv/book_review_pipeline/data/book_word_frequencies.parquet")

print(df.columns)

print(df.head(10))