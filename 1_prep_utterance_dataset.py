"""
Goal: Parse the TEIs and generate a dataset of all utterances with some metadata
"""

from pathlib import Path
import polars as pl

pl.Config.set_tbl_cols(-1)
# pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_width_chars(500)
pl.Config.set_fmt_str_lengths(100)
from utils import extract_utterances, find_audio

TEIs = list(Path("Gos.TEI/").glob("Artur-*/*.xml"))
results = []
for p in TEIs:
    data = extract_utterances(p)
    results.extend(data)
    2 + 2
df = pl.DataFrame(results).with_columns(duration=pl.col("end_s") - pl.col("start_s"))

print(df)
print("Adding filenames:")

files = pl.DataFrame(df["filename"].unique()).with_columns(
    audio_file=pl.col("filename").map_elements(find_audio, return_dtype=pl.String)
)
df = df.join(files, on="filename", how="left")
print(df)
print("Filtering out instances with no words:")
df = df.filter(pl.col("w_count") > 0)
print(df)
2 + 2
df.write_ndjson("1_utterances.jsonl")
