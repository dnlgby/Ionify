
from connections import parser
from connections.my_sql_connection import MySQLConnection

if __name__ == "__main__":

    # connections = parser.parse_connections_config('connections.yaml')
    # print(connections)

    with MySQLConnection('test', '127.0.0.1', 3306, 'my_database', 'root', 'password') as connection:
        #print(connection.check_health())
        #print(connection.create_connection_string())
        #print(connection.automap_base_model.classes)

        # # ### Class definition
        # from sqlalchemy import Column, Integer, String
        # class Camera(connection.declarative_base_model):
        #     __tablename__ = "cameras"
        #
        #     id = Column(Integer, primary_key=True)
        #     model = Column(String(255))
        #     resolution = Column(String(255))
        # #
        # # connection.register_model(Camera)
        # # connection.create_all_user_defined_models()
        # # ### EO class definition
        #
        # session = connection.get_new_session()
        #
        # new_camera = Camera(model="Canon", resolution="1080p")
        # session.add(new_camera)
        # session.commit()

        # model = connection.automap_base_model.classes.get("cameras")
        # session = connection.get_new_session()
        # all_instances = session.query(model).all()
        #
        # print(f"ID: {all_instances[0].id}, Model: {all_instances[0].model}, Resolution: {all_instances[0].resolution}")

        for entity_name, entity_class in connection.automap_base_model.classes.items():
           print(f"Entity name: {entity_name}, Entity class: {entity_class}")