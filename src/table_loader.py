
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd
from fastapi import Depends
import models
import pprint
from database import SessionLocal, engine



# a wrapper to instantiate a session allows us to ensure the database connection is closed after our transaction; that a new Session is generated each time we interact with the db.
# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()




class TableLoader:
    """simulated cron job processing class.
        depends on models.py file being loaded with necessary table models present
    - accepts:
        - data folder filepath
                - todo: some sort of qualification of which tables to "update"
        
        - could incorporate logging tools of your choice?   
        
        """

    def __init__(self,
                 data_source_prefix:str="../data/",
                 skip:int = 0,
                 offset:int = 0,
                 db:Session = SessionLocal()):

        # self.skip = 0
        # self.offset = 0
        self.chunksize = 10**7
        self.db=db
        self.data_folder = data_source_prefix

        ### specific data sources/endpoints using your prefix; source URL (or disk data)
        self.table_names = ['campaign_desc',
                        'campaign_table',
                        'causal_data',
                        'coupon',
                        'coupon_redempt',
                        'hh_demographic',
                        'product',
                        'transaction_data']

        ### along with a map of your table models (the abstraction layer of SQLALchemy)
        self.table_models = [models.CampaignDesc,
                            models.CampaignTable,
                            models.CausalData,
                            models.Coupon,
                            models.CouponRedempt,
                            models.HHDemographic,
                            models.Product,
                            models.TransactionData]

        ### map the two together for reference
        self.name_model_map = dict(zip(self.table_names, self.table_models)) 
        # TODO: add map of 'total rows required' or similar?
        self.existing_rowcounts = dict()

        ### instantiate logging...
        self._log = ""

        ### ping db to find existing rowcount/index for known tables
        self.update_existing_rowcounts()
        
        pprint.pprint(self.existing_rowcounts)
        # run the auto-updating feature to ensure all rows are accounted for...
        # for x in self.table_names:
        #     if 
        #     self.insert_table
        #     self.run_update(x)



    def get_existing_rowcount(self, table_name):
        try:
            con = engine.raw_connection()
            cursor = con.cursor()
            cursor.execute(f'select count(1) from {table_name}')
            res = cursor.fetchall()
        except:
            return 0
        return res[0][0]
        #self.existing_rowcounts[x] = res[0][0]


    def update_existing_rowcounts(self):
        con = engine.raw_connection()
        cursor = con.cursor()
        for x in self.table_names:
            self.existing_rowcounts[x] = self.get_existing_rowcount(x)


    def delete_known_table(self, table_name):
        con = engine.raw_connection()
        cursor = con.cursor()
        cursor.execute(f'drop table {table_name}')
    

    def delete_known_tables(self):
        for x in self.table_names:
            self.delete_known_table(x)


    ### logger
    @property
    def log(self):
        return self._log
    
    @log.setter
    def log(self, new):
        print(new, flush=True)
        self._log += "\n " + new
        

    def print_log(self):
        pprint.pprint(self.log)


    #### chunker
    # def start_stop(self, offset=0, limit=10**7, chunk_size=10**7):
    #     start=offset
    #     stop=limit
    #     while start < limit:
    #         try: 
    #             yield start, stop
    #         except BaseException as e:
    #             self.log = f"Chunker failed with {e}"
    #         finally:
    #             start = stop
    #             stop = stop + chunk_size

    #### clunker
    # def update_table(self, table_name):
    #     '''cron job'''

    #     #### start index
    #     start_index = self.existing_rowcounts[table_name] ### the offset

    #     #### EXTRACT DATA 
    #     df = pd.read_csv(self.data_folder+table_name+'.csv').reset_index().set_index('index')
    #     if start_index == 0:
    #         df.to_sql(name =table_name, con=engine, chunksize=5000000, if_exists='append', method='multi')


    #     else:
            
    #         fields = [x.casefold() for x in df.columns]
    #         stop_index = df.shape[0] ### assume this value is the limit (length) of the data we need to pull from the .csv.
            

    #         #### DATA VERIFICATION ASSERTIONS...ADD REAL INDEX COMPARISON?
    #         assert start_index < stop_index, f'start {start_index} < {stop_index} stop'

    #         self.log = f"Beginning '{table_name}' Update..."
    #         self.log = f"Starting at 0-index {start_index}, going up to but not including {stop_index}..."
    #         self.log = f"Shape is {df.shape}. Fields are {fields}..."
            
    #         rows_to_rip = stop_index - start_index
    #         try:
    #             #### automatic check for filesize --> do we need chunking?
    #             if rows_to_rip < 10**7:
    #                 self.log = f"no chunking necessary..."
    #                 #### regular row-level upload?
    #                 for x in df.values[start_index:stop_index]:
    #                     hh_object=self.name_model_map[table_name](**dict(zip(fields, x)))
    #                     self.db.add(hh_object)
    #                     self.db.commit()
    #                     self.db.refresh(hh_object)
    #             else:
    #                 #### enter chunking logic
    #                 self.log = f'entering chunking logic;'
    #                 start_stopper = self.start_stop(offset=start_index
    #                                                 , limit=stop_index
    #                                                 , chunk_size=10**7)

    #                 while start_index <= stop_index:
    #                     start_index, stop_index = next(start_stopper)
    #                     self.log = f'rows {start_index} through {stop_index} of {len(df.values)}'
    #                     chunk = list(df.values[start_index:stop_index])

    #                     self.db.add_all([self.name_model_map[table_name](**dict(zip(fields, x))) for x in chunk])
    #                     self.db.commit()

    #         except BaseException as e:
    #             #print(f'process failed on table {file}: {e}')
    #             self.log += f'process failed on table {table_name}: {e}'

    
    def insert_table(self, table_name):
        """for our use case, we just need the whole data to go in.
         only use this if the table models have been created, but the table is empty """
        
        # try:
        #     assert self.existing_rowcounts[table_name] == 0, f'{table_name} already has data populated. Use update_table'
        #     self.log = f" inserting {table_name}"

        #     with open(f'../data/{table_name}.csv') as f:
        #         benchmark = len(f.readlines())
            

        #     with open(f'../data/{table_name}.csv') as f:
        #         reader = csv.reader(f)
        #         columns = [x.casefold() for x in next(reader)]
        #         print(rowcount)
        #         objs = []
        #         for idx, row in enumerate(reader):
        #             obj = self.name_model_map[table_name](**dict(zip(columns, row)))
        #             objs.append(obj)
        #             # db.add(obj)

        #             if idx % 500000 == 0:
        #                 self.db.add_all(objs)
        #                 try:
        #                     self.db.commit()
        #                     objs.clear()
        #                 except:
        #                     self.db.rollback()
        #                     break
        #                 print(f'row {idx}', end='\r')
        #         try:
        #             results = self.db.commit()
        #             self.log = results
        #         except:
                    
        # except:
        #     self.db.rollback()
        # finally:
        #     self.db.close()


        # except AssertionError as e:
        #     self.log = f"{e}"
        #     self.db.rollback()

        # finally:
        #     self.db.close()

        df = pd.read_csv(self.data_folder+table_name+'.csv')
        df.columns = [x.casefold() for x in df.columns]
        df.to_sql(name=table_name, con=engine, if_exists='append', method='multi')
        
        
        # except AssertionError as e:
        #     self.log = str(e)


    def insert_all(self, all_names:list=None):
        if all_names == None:
            all_names = self.table_names
        for x in all_names:
            self.log = f'reading table {x}'
            self.insert_table(x)

                


if __name__ == '__main__':
    from my_url import _SQLALCHEMY_DATABASE_URL
    import csv 

    ### creating a database
    SQLALCHEMY_DATABASE_URL = _SQLALCHEMY_DATABASE_URL

    # this engine has a special 'check_same_thread' argument for sqlite3
    engine = create_engine(
        SQLALCHEMY_DATABASE_URL)

    # the sessionmaker function creates a new Session for our database interactions, and is the preferred method of transacting with the db when using SQLAlchemy
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


    # this Base is the parent class of our Tables; they can be found in models.py. 
    Base = declarative_base()

    # # when we create all of the Table models from within models.py, each child class will have an equivalent metadata object (shared by the parent)
    models.Base.metadata.create_all(bind=engine)

    a = TableLoader(db=SessionLocal())

    #a.delete_known_tables()

    a.insert_all()

    # pprint.pprint(a.log)







