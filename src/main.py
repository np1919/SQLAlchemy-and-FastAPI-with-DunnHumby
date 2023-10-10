from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

import crud, models, schemas
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/transactions/", response_model=list[schemas.TransactionData])
def read_transactions(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    users = crud.get_transactions(db, skip=skip, limit=limit)
    return users


@app.get("/hh/{hh_id}", response_model=schemas.HHDemographic)
def read_hh(hh_id: int, db: Session = Depends(get_db)):
    hh = crud.get_hh(hh_id=hh_id, db=db)
    if hh is None:
        raise HTTPException(status_code=404, detail="Household not found")
    return hh


# @app.post("/hh/", response_model=schemas.HHDemographic)
# def create_hh(hh: schemas.HHDemographicCreate, db: Session = Depends(get_db)):
#     db_item = models.HHDemographic(**hh.dict())
#     db.add(db_item)
#     db.commit()
#     db.refresh(db_item)
#     return db_item
    # #hh = crud.create_hh(db, hh=hh)
    # if hh:
    #     pass
    #     raise HTTPException(status_code=400, detail="hh already created")
    # return crud.create_hh(db=db, hh=hh)


# @app.get("/users/", response_model=list[schemas.User])
# def read_users(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
#     users = crud.get_users(db, skip=skip, limit=limit)
#     return users


# @app.get("/users/{user_id}", response_model=schemas.User)
# def read_user(user_id: int, db: Session = Depends(get_db)):
#     db_user = crud.get_user(db, user_id=user_id)
#     if db_user is None:
#         raise HTTPException(status_code=404, detail="User not found")
#     return db_user


# @app.post("/users/{user_id}/items/", response_model=schemas.Item)
# def create_item_for_user(
#     user_id: int, item: schemas.ItemCreate, db: Session = Depends(get_db)
# ):
#     return crud.create_user_item(db=db, item=item, user_id=user_id)


# @app.get("/items/", response_model=list[schemas.Item])
# def read_items(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
#     items = crud.get_items(db, skip=skip, limit=limit)
#     return items