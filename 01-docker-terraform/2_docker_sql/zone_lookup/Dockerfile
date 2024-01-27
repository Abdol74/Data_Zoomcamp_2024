# build your image on base of python3.9
FROM python:3.9
RUN apt-get update && apt-get install -y curl

# install pandas library on your image
RUN pip install pandas sqlalchemy psycopg2

# define the working directory inside the image
WORKDIR /app

# define what to do 

copy ingest_zone_lookup.py ingest_zone_lookup.py

#define the bash to be the entry point for your image
#ENTRYPOINT [ "bash" ]

#you can customize your entrypoint say u need to run the pipeline.py 

ENTRYPOINT ["python", "ingest_zone_lookup.py"]