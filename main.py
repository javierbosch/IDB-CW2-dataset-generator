import pandas as pd
import names
import random
import string
import datetime

TABLE_SIZES = 30 # Magical main constant

# Jungle of constants
Customers_size = int(TABLE_SIZES*2)
Countries = ['AFG','ALA','ALB','DZA','ASM','AND','AGO','AIA','ATA','ATG','ARG','ARM','ABW','AUS','AUT','AZE','BHS','BHR','BGD','BRB','BLR']
Products_size = int(TABLE_SIZES*0.5)
price_scale = 50
pname_len = 12
ptype_options = ['BOOK','MOVIE','MUSIC']
start_date = datetime.datetime(2023, 1, 1)
timedelta = datetime.timedelta(days=50)
OrdersSize = int(TABLE_SIZES*1.5)
start_odate = datetime.date(2022, 1, 1)
end_odate = datetime.date(2022, 2, 1)
Details_ordid_size = int(TABLE_SIZES*1.5)
Details_pcode_range = (1,Products_size)
qty_max = 50
Invoices_size = int(TABLE_SIZES*1.1)
amount_scale = 150
Payments_size = int(TABLE_SIZES*1.3)
output_file = 'insert_queries.sql'

def main():
    # Generate Orders
	cname =  [names.get_full_name() for i in range(Customers_size)]
	country = random.choices(Countries, k=Customers_size)
	Customers = pd.DataFrame(list(zip(cname, country)), columns =['cname', 'country'])
	Customers.index.name = 'custid'
	Customers.reset_index(inplace=True)

	# Generate Products
	price = [random.random()*price_scale for i in range(Products_size)]
	pname = [''.join((random.choice(string.ascii_lowercase)) for x in range(pname_len)) for i in range(Products_size)]
	ptype = [random.choice(ptype_options) for i in range(Products_size)]
	Products = pd.DataFrame(list(zip(pname, ptype, price)), columns =['pname', 'ptype', 'price'])
	Products.index.name = 'pcode'
	Products.reset_index(inplace=True)

	# Generate Orders
	odate = [(start_date + random.random() * timedelta).strftime('%Y-%m-%d')for i in range(OrdersSize)]
	ocust = random.choices(Customers.index,k=OrdersSize)
	Orders = pd.DataFrame(list(zip(odate, ocust)), columns =['odate', 'ocust'])
	Orders.index.name = 'ordid'
	Orders.reset_index(inplace=True)

	# Generate Details
	Details_ordid = []
	ordid, pcode = [], []
	for x in random.sample(list(Orders.index),Details_ordid_size):
	    for y in random.sample(list(Products.index),random.randint(Details_pcode_range[0],Details_pcode_range[1])):
	        ordid.append(x)
	        pcode.append(y)
	qty = [random.randint(1, qty_max) for i in range(len(ordid))]
	Details = pd.DataFrame(list(zip(ordid, pcode, qty)), columns =['ordid', 'pcode', 'qty'])

	# Generate Invoices
	ordid = random.sample(list(Orders.index),Invoices_size)
	amount = [random.random()*amount_scale for i in range(Invoices_size)]
	issued, due = [], []
	for i in range(Invoices_size):
	    issued_i = start_odate + datetime.timedelta(days=random.randrange((end_odate - start_odate).days))
	    issued.append(issued_i.strftime('%Y-%m-%d'))
	    due.append((issued_i + datetime.timedelta(days=random.randrange((end_odate - start_odate).days))).strftime('%Y-%m-%d'))
	Invoices = pd.DataFrame(list(zip(ordid, amount, issued,due)), columns =['ordid', 'amount', 'issued','due'])
	Invoices.index.name = 'invid'
	Invoices.reset_index(inplace=True)

	# Generate Payments
	tstamp = [(start_date + random.random() * timedelta).strftime('%Y-%m-%d %H:%M:%S') for i in range(Payments_size)]
	amount = [random.random()*amount_scale for i in range(Payments_size)]
	invid = random.choices(Invoices.index,k=Payments_size)
	Payments = pd.DataFrame(list(zip(tstamp, amount, invid)), columns =['tstamp', 'amount', 'invid'])
	Payments.index.name = 'payid'
	Payments.reset_index(inplace=True)

	tables = {'Customers':Customers,'Products':Products,'Orders':Orders,'Details':Details,'Invoices':Invoices,'Payments':Payments}
	text = ";\n".join([";\n".join(sql_insert_from_df(tables[x],x)) for x in tables])
	
	with open(output_file, 'w') as f:
		f.write(text)

def sql_insert_from_df(source,target):
    sql_texts = []
    for index, row in source.iterrows():       
        sql_texts.append('INSERT INTO '+target+' ('+ str(', '.join(source.columns))+ ') VALUES '+ str(tuple(row.values)))      
    return sql_texts

if __name__=="__main__":
    main()