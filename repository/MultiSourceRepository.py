import pandas as pd



class MultiSourceRepository:
    def __init__(self, mysql_ds, postgres_ds):
        self.mysql_ds = mysql_ds
        self.postgres_ds = postgres_ds

    def get_combined_data(self):
        # Specify your tables
        mysql_table = "my_table"
        postgres_table = "my_other_table"

        # Use the data sources to get all records from the tables
        mysql_result = self.mysql_ds.find_all(mysql_table)
        postgres_result = self.postgres_ds.find_all(postgres_table)

        # Convert the results to dataframes
        mysql_df = pd.DataFrame([obj.__dict__ for obj in mysql_result])
        postgres_df = pd.DataFrame([obj.__dict__ for obj in postgres_result])

        # Concatenate the dataframes into one
        combined_df = pd.concat([mysql_df, postgres_df], axis=0)

        return combined_df


