

DATA_TYPE=$1
YEAR=$2

URL_PREFIX='https://github.com/DataTalksClub/nyc-tlc-data/releases/download'

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`
    URL="${URL_PREFIX}/${DATA_TYPE}/${DATA_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

    LOCAL_PREFIX="../data/raw/${DATA_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${DATA_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    if [ ! -d "$LOCAL_PREFIX" ]; then

        mkdir -p "$LOCAL_PREFIX"
        echo "Directory created: $LOCAL_PREFIX"
    else
        echo "Directory already exists: $LOCAL_PREFIX"
    fi

    wget -nv "$URL" -O "$LOCAL_PATH"

    if [ $? -eq 0 ]; then
        echo "Download successful!"
    else
        echo "Failed to download file. Check the URL and try again."
    fi

done