# artur_fp_annotation

For now, this repo documents the download and preprocessing procedure for
annotation of Filled Pauses on GOS2.1/Artur-* utterances.

Brief rundown:
* `0_download_data.sh`: downloads and decompresses Gos2.1 TEI files as well as
  Artur audio files
* `1_prep_utterance_dataset.py`: Parse Gos2.1/Artur* TEIs, extract utterances
and their temporal boundaries, filter out the utterances with no words, find
corresponding audio, and serialize it in jsonl.
* `2_inspecting_segments.py`: After Ivan performed this splits, this script
  validated the data.
* `3_joining_inference.py`: Given the inferred results, perform the
  postprocessing (joining and deduplicating the predictions) and join different
  data sources to get to a dataset we can analyze.

