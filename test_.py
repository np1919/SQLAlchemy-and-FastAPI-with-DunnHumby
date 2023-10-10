
import sqlite3
import glob
import os
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#### before running db setup, set up your virtual environment:
####    python -m venv venv
####    venv/Scripts/Activate.ps1

#### install necessary packages:
####    pip install sqlmodel, sqlalchemy, pandas, pydantic, fastapi


#### DB SETUP AND INITIAL DATA INGESTION STEP 
#### this file performs a local setup for the raw data we are using for the project, sourced from kaggle; DunnHumby: the Complete Journey





#### this data ingestion step could be done in several ways:



if __name__ == "__main__":

    # #### 1. SQLALCHEMY DB SETUP


    # SQLALCHEMY_DATABASE_URL = "sqlite:///./dunnhumby.db"
    # # SQLALCHEMY_DATABASE_URL = "postgresql://user:password@postgresserver/db"

    # engine = create_engine(
    #     SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
    # )
    # SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # Base = declarative_base()


    # #### 2. SQLITE3 DB SETUP
    con = sqlite3.connect("dunnhumby.db")

    cur = con.cursor()


    #### INITIAL/BULK DATA INGESTION (directly from .csv files in /data/ folder)


        #### using pd.DataFrame.to_sql() function (file in active memory)
    # for file in glob.glob('data/*.csv'):
    #     try:
                ### read filename
    #         print('*'*50)
    #         filename = file.split("\\")[1][:-4]
    #         print(filename)

    #           ### load data
    #         df = pd.read_csv(file)
    #         #display(df.head(5))
    #         #print(df.info())

                ### write to db
    #         df.to_sql(filename, con, if_exists='fail', index=False)
    #         print(f'{filename} written to database')
    #         print('*'*50)

    #     except:
    #         print(f"{filename} already exists")




    ##### read each csv file using a closure

    for file in glob.glob('data/*.csv')[:1]:
        filename = file.split("\\")[1][:-4]
        print(filename)

        ### create an iterator for each file?
        with open(file) as data:
            columns = data.readline().split(',') # emulating the df.columns array...
            # cur.execute(f"DROP TABLE IF EXISTS {filename}")
           
            #### this code needs to define datatypes for each dataframe
            print(columns)


            statement = f"CREATE TABLE IF NOT EXISTS {filename} ({','.join(columns)})"
            cur.execute(statement)
            print(statement)
            con.commit()

            for row in data.readlines():
                statement_2 = f"INSERT INTO {filename} VALUES({",".join(row)})"
                print(statement_2)

                cur.execute(statement_2)
            # con.commit()
            # df = pd.read_csv(file)
            # columns = ",".join(df.columns)
            # data = [tuple(x) for x in df.values]
