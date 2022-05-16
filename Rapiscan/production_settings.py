# Value of platofrm.node() which triggers production functionality
nodename="dfwpl0app256.ent.covance.com"

# Credentials for the production datamart (Postgres, "CMR")
datamart_credentials_filename="~/credentials/cdw-prod-credentials.json"
# This is the study_ref in the production datamart
study_ref=2

# Where to find data from the perspective of the production server
input_folder="/sdw_share/FilesForSDW3/ANJI/ANJI-PROD"

# Subfolders created to stage data for upload to cmr daabase
# These files can be erased after upload is complete 
# Subfolders will be dated.
datamart_staging_base="/data/mart-upload-work-dir/ANJ900D3501"
