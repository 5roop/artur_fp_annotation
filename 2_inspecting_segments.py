import polars as pl
from tqdm import tqdm
from pydub import AudioSegment
import numpy as np

df = pl.read_ndjson(
    "/ceph/home/ivanp/parla_fp/artur_fp_annotation/2_utterances_segmented.jsonl"
)


def prep_segments(start, end, chunk_duration=30, overlap=5):
    """
    Prepares start and end times for audio chunks using numpy.

    Args:
        start (int): Start time in seconds.
        end (int): End time in seconds.
        chunk_duration (int): Duration of each chunk in seconds (default: 30).
        overlap (int): Overlap between chunks in seconds (default: 5).

    Returns:
        starts (numpy.ndarray): Array of start times for each chunk.
        ends (numpy.ndarray): Array of end times for each chunk.
    """
    if end - start <= chunk_duration:
        return np.array([start]), np.array([end])
    # Calculate the step size (chunk duration minus overlap)
    step = chunk_duration - overlap

    # Generate start times using numpy.arange
    starts = np.arange(start, end, step)

    # Calculate end times
    ends = np.minimum(starts + chunk_duration, end)

    durations = ends - starts
    c = durations > overlap

    return starts[c], ends[c]


def get_starts(s: str):
    import re

    pattern = r".*avd_(\d*\.\d*)-(\d*\.\d*).*"
    start = float(re.match(pattern, s).group(1))
    return start


def get_ends(s: str):
    import re

    pattern = r".*avd_(\d*\.\d*)-(\d*\.\d*).*"
    end = float(re.match(pattern, s).group(2))
    return end


def validate_segments_duration(row) -> bool:
    """_summary_

    :param (dict) row: A polars row
    :return Bool: True if no errors were found, raises errors othervise
    """
    segments = row["segments"]
    import numpy as np

    starts = np.array([get_starts(i) for i in segments])
    ends = np.array([get_ends(i) for i in segments])
    for s, e in zip(starts, ends):
        assert e - s <= 30.1

    expected_starts, expected_ends = prep_segments(row["start_s"], row["end_s"])

    assert np.allclose(starts, expected_starts)
    assert np.allclose(ends, expected_ends)

    assert round(ends[-1], 3) == round(row["end_s"], 3)
    return True


def validate_segments_existance(row) -> bool:
    """Checks for existance of the segment wavs, as well as their duration
    (it should correspond within 20ms)

    :param (dict) row: A polars row
    :return True if all checks pass
    """
    segments = row["segments"]
    from pathlib import Path

    for segment in segments:
        p = Path(
            "/ceph/home/ivanp/parla_fp/artur_fp_annotation/2_audio_segments", segment
        )
        assert p.exists()
        s, e = get_starts(segment), get_ends(segment)
        audio = AudioSegment.from_file(p)
        2 + 2
        assert abs((e - s) * 1000 - len(audio)) < 20

    return True


2 + 2

for row in tqdm(
    df.iter_rows(named=True),
    total=df.shape[0],
):
    validate_segments_existance(row)
    validate_segments_duration(row)
