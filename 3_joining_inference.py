import polars as pl
from utils import get_starts, get_ends
from pathlib import Path

pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(25)
pl.Config.set_tbl_width_chars(500)
pl.Config.set_fmt_str_lengths(50)


def translate_times(row: dict) -> list:
    start = row["segment_start"]
    fps = row["filled_pauses"]
    results = []
    for fp in fps:
        results.append([round(i + start, 3) for i in fp])
    return results


def inhibit_hits_that_start_at_zero(L: list) -> list:
    L = list(L)
    if L == []:
        return L
    if L[0][0] == 0.0:
        L.pop(0)
    return L


def filter_intervals_by_length(L: list):
    LIM = 0.08
    L = list(L)
    if L == []:
        return L
    else:
        return [i for i in L if i[1] - i[0] > LIM]


def join_elements(L: list) -> list:
    results = []
    for l in L:
        results.extend(l)
    return results


def dedup_elements(l: list) -> list:
    if l.shape[0] == 0:
        return []
    l = l.to_list()
    l.sort(key=lambda x: x[0])

    merged = []
    current_interval = l[0]

    for interval in l[1:]:
        if interval[0] <= current_interval[1]:  # Check for overlap
            # Merge intervals by taking the minimum start and maximum end
            current_interval[1] = max(current_interval[1], interval[1])
        else:
            # No overlap, add the current interval to the result
            merged.append(current_interval)
            current_interval = interval

    # Add the last interval
    merged.append(current_interval)

    return merged


speeches = pl.read_csv(
    "Gos.TEI/Gos-speeches.tsv", separator="\t", truncate_ragged_lines=True
)

speakers = pl.read_csv(
    "Gos.TEI/Gos-speakers.tsv", separator="\t", truncate_ragged_lines=True
)

fps = (
    pl.read_ndjson("/ceph/home/ivanp/parla_fp/scripts/3_filled_pauses.jsonl")
    .with_columns(
        segment_file=pl.col("segment_path").map_elements(
            lambda s: Path(s).name, return_dtype=pl.String
        ),
        segment_start=pl.col("segment_path").map_elements(
            lambda s: get_starts(s), return_dtype=pl.Float32
        ),
        filled_pauses=pl.col("filled_pauses")
        .map_elements(
            inhibit_hits_that_start_at_zero, return_dtype=pl.List(pl.List(pl.Float64))
        )
        .map_elements(
            filter_intervals_by_length, return_dtype=pl.List(pl.List(pl.Float64))
        ),
    )
    .with_columns(
        pl.struct(["filled_pauses", "segment_start"])
        .map_elements(translate_times, return_dtype=pl.List(pl.List(pl.Float64)))
        .alias("filled_pauses_real_time")
    )
)

df = pl.read_ndjson("/ceph/home/ivanp/parla_fp/scripts/2_utterances_segmented.jsonl")

print(f"Before merging: {df.shape=}")
audio_mapper = (
    df.select(["audio_file", "segments", "ident"])
    .explode("segments")
    .rename({"audio_file": "master_audio_file", "segments": "segment_file"})
)

fps = fps.join(audio_mapper, how="left", on="segment_file")
gb = (
    fps.group_by(["ident"])
    .agg(
        pl.col("filled_pauses_real_time")
        .map_elements(join_elements, return_dtype=pl.List(pl.List(pl.Float64)))
        .map_elements(dedup_elements, return_dtype=pl.List(pl.List(pl.Float64)))
    )
    .rename({"filled_pauses_real_time": "filled_pauses"})
)

df = df.join(gb.select(["ident", "filled_pauses"]), on="ident", how="left")
# ivans = pl.read_ndjson("/ceph/home/ivanp/parla_fp/4_infered_fp_TEST.jsonl")
# for row in df.filter(~pl.col("filled_pauses").is_null()).iter_rows(named=True):
#     print("My:   ", row["filled_pauses"])
#     ivans_row = ivans.filter(pl.col("ident").eq(row["ident"]))[
#         "filled_pauses"
#     ].to_list()
#     for r in ivans_row:
#         print(
#             "Ivans:",
#             r,
#         )
#     2 + 2
print(df.shape)
df = df.select(
    ["ident", "who", "w_count", "path", "audio_file", "filled_pauses", "duration"]
).with_columns(pl.col("filled_pauses").list.len().alias("FP_count"))
print(df.shape)
df = df.join(
    speakers.select(["PRS-ID", "SEX"]).unique(),
    how="left",
    right_on="PRS-ID",
    left_on="who",
)
print(f"After merging: {df.shape=}")
df.write_ndjson("3_inferred_FP.jsonl")
