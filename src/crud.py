from sqlalchemy.orm import Session

import models, schemas



# def insert_hh(db: Session, fields:list, item_data:dict):
#     payload = dict(zip(fields, row))
#     db_hh = models.HHDemographic(**hh.dict())
#     db.add(db_hh)
#     db.commit()
#     db.refresh(db_hh)
#     return None

# def get_hh(db: Session, hh_id: int):
#     return db.query(models.HHDemographic).all()#filter(models.HHDemographic.household_key == hh_id).first()


# def get_user_by_email(db: Session, email: str):
#     return db.query(models.HHDemographic).filter(models.HHDemographic.email == email).first()


def get_transactions(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.TransactionData).offset(skip).limit(limit).all()


# def create_hh(db: Session, hh: schemas.HHDemographicCreate):
#     hh = models.HHDemographic(**hh.dict())
#     db.add(hh)
#     db.commit()
#     db.refresh(hh)
#     return hh

def get_hh(hh_id:int
            ,db: Session):
    return db.query(models.HHDemographic).filter(models.HHDemographic.household_key == hh_id).first()


# def get_items(db: Session, skip: int = 0, limit: int = 100):
#     return db.query(models.Item).offset(skip).limit(limit).all()


# def create_user_item(db: Session, item: schemas.ItemCreate, user_id: int):
#     db_item = models.Item(**item.dict(), owner_id=user_id)
#     db.add(db_item)
#     db.commit()
#     db.refresh(db_item)
#     return db_item