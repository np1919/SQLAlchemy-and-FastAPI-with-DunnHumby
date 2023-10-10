from pydantic import BaseModel, Field
# from fastapi_utils.guid_type import GUID

### we only need certain data to be available through the API?
### do we want to post to 
class CampaignDescBase(BaseModel):
    index : int
    campaign : int
    description : str
    end_day : int
    start_day : int

class CampaignDescCreate(CampaignDescBase):
    pass

class CampaignDesc(CampaignDescBase):

    class Config:
        orm_mode = True



class CampaignTableBase(BaseModel):
    index : int
    campaign : int
    description : str
    household_key : int

class CampaignTableCreate(CampaignTableBase):
    pass

class CampaignTable(CampaignTableBase):
    
    class Config:
        orm_mode = True


class CausalDataBase(BaseModel):
    index : int 
    product_id : int
    store_id : int
    week_no : int
    display : str
    mailer : str

class CausalDataCreate(CausalDataBase):
    pass

class CausalData(CausalDataBase):
    
    class Config:
        orm_mode = True


class CouponBase(BaseModel):
    index : int
    campaign : int
    coupon_upc : int
    product_id : int

class CouponCreate(CouponBase):
    pass

class Coupon(CouponBase):
    
    class Config:
        orm_mode = True


class CouponRedemptBase(BaseModel):
    index : int 
    campaign : int
    coupon_upc : int
    day : int
    household_key : int

class CouponRedemptCreate(CouponRedemptBase):
    pass

class CouponRedempt(CouponRedemptBase):
    
    class Config:
        orm_mode = True


class HHDemographicBase(BaseModel):
    index : int
    age_desc : str
    hh_comp_desc : str
    homeowner_desc : str
    household_key : int
    household_size_desc : str
    income_desc : str
    kid_category_desc : str
    marital_status_code : str

class HHDemographicCreate(HHDemographicBase):
    pass

class HHDemographic(HHDemographicBase):
    
    class Config:
        orm_mode = True


class ProductBase(BaseModel):
    index : int
    product_id : int
    manufacturer : str
    department : str
    brand : str
    commodity_desc : str
    sub_commodity_desc : str
    curr_size_of_product : str

class ProductCreate(ProductBase):
    pass

class Product(ProductBase):
    
    class Config:
        orm_mode = True


class TransactionDataBase(BaseModel):
    index : int
    household_key : int
    basket_id : int
    day : int
    product_id :int
    quantity : int
    sales_value : float
    store_id : int
    retail_disc : float
    trans_time : int
    week_no : int
    coupon_disc : float
    coupon_match_disc : float

class TransactionDataCreate(TransactionDataBase):
    pass

class TransactionData(TransactionDataBase):
    
    class Config:
        orm_mode = True

