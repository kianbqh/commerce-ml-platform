"""Small synthetic baskets for CI contracts; never used for published quality."""
import csv
from pathlib import Path


def write_fixture(directory,change_test=False):
    root=Path(directory); root.mkdir(parents=True,exist_ok=True)
    def write(name,header,rows):
        with (root/f'{name}.csv').open('w',newline='') as stream:
            csv.writer(stream).writerows([header,*rows])
    write('aisles',['aisle_id','aisle'],[[10+i,f'aisle {i}'] for i in range(4)])
    write('departments',['department_id','department'],[[3,'department 0'],[7,'department 1']])
    write('products',['product_id','product_name','aisle_id','department_id'],
          [[3001+i,f'fixture grocery {i}',10+i//6,3 if i<12 else 7] for i in range(24)])
    orders=[]; prior=[]; train=[]
    for user in range(40):
        seen=set()
        for number in range(1,8):
            order=10000+user*10+number
            orders.append([order,1001+user,'test' if number==7 else 'train' if number==6 else 'prior',number,number%7,10,0 if number==1 else 7])
            if number==7: continue
            products=sorted({(user%4)*6,(user%4)*6+1,(user+number)%24,(user+2*number)%24})
            if change_test and number==6: products=sorted({(p+3)%24 for p in products})
            target=train if number==6 else prior
            for position,product in enumerate(products,1): target.append([order,3001+product,position,int(product in seen)])
            seen.update(products)
    # This user's two labeled baskets are excluded explicitly, rather than inventing a train history.
    orders.extend([[99991,9999,'prior',1,0,10,0],[99992,9999,'train',2,1,10,7]])
    prior.append([99991,3001,1,0]); train.append([99992,3001,1,1])
    write('orders',['order_id','user_id','eval_set','order_number','order_dow','order_hour_of_day','days_since_prior_order'],orders)
    header=['order_id','product_id','add_to_cart_order','reordered']
    write('order_products__prior',header,prior); write('order_products__train',header,train)
