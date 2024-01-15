import databaseobj


def OLAPSales():
  #produits=databaseobj.select_produits()
  #transactions=databaseobj.select_transactions()
  create_star_schema()

def create_star_schema():
  #databaseobj.delete_all_star()
  
  #databaseobj.create_dims_tables()
  databaseobj.create_fact_table()


