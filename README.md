# Description
CLEF eHealth 2017 Task 2: Technologically Assisted Reviews in Empirical Medicine
https://sites.google.com/site/clefehealth2017/task-2

Author: Dan Li (d.li@uva.nl)

# Requirement
- Download chromedriver from https://sites.google.com/a/chromium.org/chromedriver/
- Install selenium: pip install selenium

# Functions
- batch_download_pid           --- Download pids for all the systematic reviews
- extract_pid                  --- Extract pids from downloaded xml and rewrite to new dir
- batch_download_title         --- Download title for all the systematic reviews
- make_release_file            --- Make release files: topic file or qrel file
- download_abstract            --- Download abstract for all the pids
- trec_format_abstract         --- Make the downloaded abstracts TRECTEXT format
- statistics                   --- Statistics of the released data
