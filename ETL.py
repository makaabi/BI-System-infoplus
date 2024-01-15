import pandas as pd
import numpy as np
import os
import databaseobj


def ETLProduitTransaction():
  databaseobj.delete_all()
  produits_paths=['data\souris.csv','data\claviers.csv']
  produits = pd.concat([pd.read_csv(file) for file in produits_paths ], ignore_index=True)
  print(produits)

  produits = produits.drop(['description', 'hasDiscount','price_before_discount'], axis = 1) 
  print(produits)
  if(produits.duplicated().sum()!=0):
    print("there is duplicates in the dataset")

  
  transactions_paths=['data\sept2023.csv','data\oct2023.csv']
  transactions = pd.concat([pd.read_csv(file) for file in transactions_paths ], ignore_index=True)

  print(transactions)
  transactions = transactions.drop(['idtrans'], axis = 1) 
  print(transactions)
  if(transactions.duplicated().sum()!=0):
    print("there is duplicates in the dataset")
  
  for index, row in produits.iterrows():
    databaseobj.insert_produit(row)

  for index, row in transactions.iterrows():
    databaseobj.insert_transaction(row)




