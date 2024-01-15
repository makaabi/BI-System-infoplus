import mysql.connector
import mysql.connector

config = {
  'user': 'root',
  'password': '',
  'host': '127.0.0.1',
  'database': 'infoplus',
  'raise_on_warnings': True
}

cnx = mysql.connector.connect(**config)






def create_fact_table():
   mytable=select_oneTable()
   facts=[]
   for t in mytable:
      facts.append({'idt':t[6],'datet':t[8],'produit':t[1],'categorie':t[2],'method':t[9],'mois':t[8].month,'gain':round(t[3]-t[4], 2)})
      
   prods=select_dimprod()
   categs=select_dimcategory()
   methods=select_dimmethod()
   mois= select_dimmois()
   for ind,data in enumerate(facts):
       for t in prods:
        if (data['produit']==t[1]):
           data['produit']=t[0]
        for t in categs:
          if (data['categorie']==t[1]):
            data['categorie']=t[0]
        for t in methods:
          if (data['method']==t[1]):
            data['method']=t[0]
        for t in mois:
          if (data['mois']==t[1]):
            data['mois']=t[0]

   for t in facts:
      insert_fact_table(t)

      
      


   
    

def  create_dims_tables():
   mytable=select_oneTable()
   produits=[]
   categories=[]
   methods=[]
   mois=[]
   for t in mytable:
      produits.append(t[1])
      categories.append(t[2])
      methods.append(t[9])
      mois.append(t[8].month)
   produits = set(produits)
   categories = set(categories)
   methods = set(methods)
   mois = set(mois)
   
   for t in produits:
      insert_dimprod(t)
   for t in categories:
      insert_dimcategory(t)
   for t in methods:
      insert_dimmethod(t)
   for t in mois:
      insert_dimmois(t)
  
    

    

      
   
   


def select_oneTable():
  if cnx and cnx.is_connected():
    with cnx.cursor() as cursor:
          result = cursor.execute("SELECT * FROM  produits p inner join transactions t WHERE p.id=t.idprod")
          rows = cursor.fetchall()
          return rows
    cnx.close()
  else:
    print("Could not connect")


def select_dimprod():
  if cnx and cnx.is_connected():
    with cnx.cursor() as cursor:
          result = cursor.execute("SELECT * FROM dimprod")
          rows = cursor.fetchall()
          return rows
    cnx.close()
  else:
    print("Could not connect")

def select_dimmois():
  if cnx and cnx.is_connected():
    with cnx.cursor() as cursor:
          result = cursor.execute("SELECT * FROM dimmois")
          rows = cursor.fetchall()
          return rows
    cnx.close()
  else:
    print("Could not connect")

def select_dimmethod():
  if cnx and cnx.is_connected():
    with cnx.cursor() as cursor:
          result = cursor.execute("SELECT * FROM dimmethod")
          rows = cursor.fetchall()
          return rows
    cnx.close()
  else:
    print("Could not connect")

def select_dimcategory():
  if cnx and cnx.is_connected():
    with cnx.cursor() as cursor:
          result = cursor.execute("SELECT * FROM dimcategory")
          rows = cursor.fetchall()
          return rows
    cnx.close()
  else:
    print("Could not connect")

    


def select_transactions():
  if cnx and cnx.is_connected():
    with cnx.cursor() as cursor:
          result = cursor.execute("SELECT * FROM transactions")
          rows = cursor.fetchall()
          return rows
    cnx.close()
  else:
    print("Could not connect")
  

def select_produits():
  if cnx and cnx.is_connected():
    with cnx.cursor() as cursor:
          result = cursor.execute("SELECT * FROM produits")
          rows = cursor.fetchall()
          return rows
    cnx.close()
  else:
    print("Could not connect")

def delete_all_star():
    cursor = cnx.cursor()
    request1 ="delete  from dimprod "
    request2 ="delete  from dimcategory "
    request3 ="delete  from dimmethod "
    request4 ="delete  from dimmois "
    request5 ="delete  from facttable "
    cursor.execute(request1)
    cursor.execute(request2)
    cursor.execute(request3)
    cursor.execute(request4)
    cursor.execute(request5)
    cnx.commit()


def delete_all():
    cursor = cnx.cursor()
    request1 ="delete  from produits "
    request2 ="delete  from transactions "
    cursor.execute(request1)
    cursor.execute(request2)
    cnx.commit()

def insert_fact_table(obj):
    cursor = cnx.cursor()
    request = ("INSERT INTO facttable ""(idt,datet,idp, idc,idm,idmois,gain) ""VALUES (%s, %s,%s, %s, %s, %s, %s)")
    data = (obj['idt'], obj['datet'],obj['produit'], obj['categorie'],obj['method'],obj['mois'], obj['gain'])
    cursor.execute(request, data)
    cnx.commit()

def insert_dimprod(val):
    cursor = cnx.cursor()
    request = ("INSERT INTO dimprod ""(nomprod) ""VALUES (%s)")
    data = (val,)
    cursor.execute(request, data)
    cnx.commit()

def insert_dimcategory(val):
    cursor = cnx.cursor()
    request = ("INSERT INTO dimcategory ""(nomcategory) ""VALUES (%s)")
    data = (val,)
    cursor.execute(request, data)
    cnx.commit()

def insert_dimmethod(val):
    cursor = cnx.cursor()
    request = ("INSERT INTO dimmethod ""(nommethod) ""VALUES (%s)")
    data = (val,)
    cursor.execute(request, data)
    cnx.commit()

def insert_dimmois(val):
    cursor = cnx.cursor()
    request = ("INSERT INTO dimmois ""(mois) ""VALUES (%s)")
    data = (val,)
    cursor.execute(request, data)
    cnx.commit()

def insert_transaction(transaction):
    cursor = cnx.cursor()
    request = ("INSERT INTO transactions ""(idprod, dateheure, method) ""VALUES (%s, %s, %s)")
    data_transaction = (transaction['idprod'], transaction['dateheure'], transaction['method'])
    cursor.execute(request, data_transaction)
    cnx.commit()

def insert_produit(produit):
    cursor = cnx.cursor()
    request = ("INSERT INTO produits ""(id, name,category,sellPrice,buyPrice,stock) ""VALUES (%s, %s, %s,%s, %s, %s)")
    data_produit = (produit['id'], produit['name'], produit['category'],produit['sellPrice'],produit['buyPrice'],produit['stock'])
    cursor.execute(request, data_produit)
    cnx.commit()





