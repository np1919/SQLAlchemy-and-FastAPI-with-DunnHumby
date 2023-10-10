from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd
from fastapi import Depends
import models
import pprint
from database import SessionLocal, engine
#from table_loader import TableLoader
import csv 



if __name__ == '__main__':


    # the sessionmaker function creates a new Session for our database interactions, and is the preferred method of transacting with the db when using SQLAlchemy
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    # a wrapper to instantiate a session allows us to ensure the database connection is closed after our transaction; that a new Session is generated each time we interact with the db.
    # Dependency
    def get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    # this Base is the parent class of our Tables; they can be found in models.py. 
    Base = declarative_base()

    # # when we create all of the Table models from within models.py, each child class will have an equivalent metadata object (shared by the parent)
    models.Base.metadata.create_all(bind=engine)

    # hardcoded raw data source --> table model mapping.

    table_models = [models.CampaignDesc,
                    models.CampaignTable,
                    models.CausalData,
                    models.Coupon,
                    models.CouponRedempt,
                    models.HHDemographic,
                    models.Product,
                    models.TransactionData]

    tables = ['campaign_desc',
            'campaign_table',
            'causal_data',
            'coupon',
            'coupon_redempt',
            'hh_demographic',
            'product',
            'transaction_data']

    model_mapping = dict(zip(tables, table_models))

    ### determine which tables need to be updated...
    existing_rowcounts = dict()
    con = engine.raw_connection()
    cursor = con.cursor()
    for x in tables:
        # cursor.execute(f'select count(1) from {x}')
        cursor.execute(f'drop table {x}')
        # cursor.execute(f'select count(1) from {x}')

        print(x,'dropped')
        # res = cursor.fetchall()
        # existing_rowcounts[x] = res[0][0]
        
        
        
    for x in tables:
        db = next(get_db())

        print(f'trying table {x}')

        with open(f'../data/{x}.csv') as f:
            rowcount = len(f.readlines())
        try:
            with open(f'../data/{x}.csv') as f:
                reader = csv.reader(f)
                columns = [x.casefold() for x in next(reader)]
                print(rowcount)
                # objs = []
                for idx, row in enumerate(reader):
                    obj = model_mapping[x](**dict(zip(columns, row)))
                    # objs.append(obj)
                    db.add(obj)

                    if idx % 500000 == 0:
                        # db.add_all(objs)
                        # objs.clear()
                        print(f'row {idx}', end='\r')
            db.commit()
        except BaseException as e:
            print('failed', e)
            db.rollback()
        finally:
            db.close()


        # pprint.pprint(a.log)









