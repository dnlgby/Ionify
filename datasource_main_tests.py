from connections.my_sql_connection import MySQLConnection
from datasources.my_sql_datasource import MySQLDataSource

if __name__ == "__main__":
    # connection = MySQLConnection('test', '127.0.0.1', 3306, 'my_database', 'root', 'password')
    # ds = MySQLDataSource(connection)
    # ds.connect()
    # ds.insert('cameras', {'model': 'Panasonic3', 'resolution': '1080p'})
    #
    # all_instances = ds.find_all('cameras')
    # for camera in all_instances:
    #     print(f"ID: {camera.id}, Model: {camera.model}, Resolution: {camera.resolution}")



    # connection = MySQLConnection('test', '127.0.0.1', 3306, 'my_database', 'root', 'password')
    #
    # with MySQLDataSource(connection) as ds:
    #     ds.insert('cameras', {'model': 'Panasonic5', 'resolution': '1080p'})
    #     all_instances = ds.find_all('cameras')
    #     for camera in all_instances:
    #         print(f"ID: {camera.id}, Model: {camera.model}, Resolution: {camera.resolution}")


    # connection = MySQLConnection('test', '127.0.0.1', 3306, 'my_database', 'root', 'password')
    #
    # with MySQLDataSource(connection) as ds:
    #     res = ds.query('select * from cameras')
    #     for i in res:
    #         print(i)


    # connection = MySQLConnection('test', '127.0.0.1', 3306, 'my_database', 'root', 'password')
    #
    # with MySQLDataSource(connection) as ds:
    #     print(ds.automap_base_model.classes.cameras)
    #     for i in ds.automap_base_model.classes:
    #         print(i)
    #     #res = ds.find_by_id(data_entity_key='cameras', data_entity_id=2)
    #     #print(f"ID: {res.id}, Model: {res.model}, Resolution: {res.resolution}")

    connection = MySQLConnection('test', '127.0.0.1', 3306, 'my_database', 'root', 'password')

    with MySQLDataSource(connection) as ds:

        # ### Class definition
        from sqlalchemy import Column, Integer, String
        class Phone(connection.declarative_base_model):
            __tablename__ = "phones"

            id = Column(Integer, primary_key=True)
            model = Column(String(255))
            version = Column(Integer)
        ds.register_model(Phone)
        ds.create_all_user_defined_models()

        new_phone = Phone(model="Pixel", version=7)
        session = ds.get_new_session()
        session.add(new_phone)
        session.commit()






        # join_res = ds.left_join('customers', 'transactions', 'customer_id', condition='amount>30')
        # for customer, transaction in join_res:
        #     print(f"Id: {customer.customer_id}, First name: {customer.first_name}, Last name {customer.last_name}")
        #     if transaction:
        #         print(f"Transaction id: {transaction.transaction_id}, Amount: {transaction.amount}, Customer id: {transaction.customer_id}")
        #     else:
        #         print(f'No transaction for customer_id {customer.customer_id}')
        #



        # join_res = ds.inner_join('transactions', 'customers', 'customer_id', condition='amount=32')
        # for transaction, customer in join_res:
        #     print(f"Transaction id: {transaction.transaction_id}, Amount: {transaction.amount}, Customer id: {transaction.customer_id}")
        #     print(f"Id: {customer.customer_id}, First name: {customer.first_name}, Last name {customer.last_name}")

        #print(ds.exists('transactions', 52))

        # print(ds.count('transactions', 'amount>40'))
        # print(ds.count('transactions', 'amount>50'))

        # for i in ds.find_all('transactions', 'amount>40'):
        #     print(f"Transaction id: {i.transaction_id}, Amount: {i.amount}, Customer id: {i.customer_id}")


        #print(ds.check_health())



        #customer = ds.find_by_id('customers', 3)
        #print(f"Id: {customer.customer_id}, First name: {customer.first_name}, Last name {customer.last_name}")

        # for i in ds.query("select * from customers"):
        #     print(i)

        #ds.remove('cameras', 1)

        # ds.update('cameras', 2, {'resolution':2000})
        #
        # condition = "resolution=2000"
        # cameras = ds.find_all('cameras', condition=condition)
        #
        # for camera in cameras:
        #     print(f"Camera ID: {camera.id}, Model: {camera.model}, Resolution: {camera.resolution}")
