import pymysql
from itemadapter import ItemAdapter
from bestbuy.items import BestbuyDetailsItem, BestCatLinksItem, SubcatLinksItem, Subcat_Of_cat_LinksItem


class BestbuyPipeline:
    def __init__(self):
        # Initialize the pipeline and connect to MySQL database
        self.conn = pymysql.connect(
            host='localhost',         # Database host
            user='root',              # Database user
            password='actowiz',       # Database password
            database='bestbuy_db'     # Database name
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        # Define table names for different item types. These variables will be used to specify
        # the target tables for inserting data based on the type of item processed.
        details_data_table = 'details_data'  # Table for storing detailed item data
        cat_link_table = 'cat_link'  # Table for storing category links
        subcat_link_table = 'subcat_link'  # Table for storing subcategory links
        subcat_Of_cat_link_table = 'subcat_of_cat_link'  # Table for storing subcategory-of-category links

        # Check if the item is an instance of BestbuyDetailsItem
        if isinstance(item, BestbuyDetailsItem):
            try:
                # Get item fields and prepare columns
                item_fields = list(item.keys())
                columns_definitions = ', '.join([f"`{field.replace(' ', '_')}` LONGTEXT" for field in item_fields])

                # Create the table with columns matching item fields
                query = f"""
                                   CREATE TABLE IF NOT EXISTS {details_data_table} (
                                       `Store No.` INT AUTO_INCREMENT PRIMARY KEY,

                                       {columns_definitions}
                                   )
                               """
                self.cursor.execute(query)

                # Fetch existing columns in the table
                self.cursor.execute(f"SHOW COLUMNS FROM {details_data_table}")
                existing_columns = [column[0] for column in self.cursor.fetchall()]

                # Add new columns if they don't exist
                for field in item_fields:
                    column_name = field.replace(' ', '_')
                    if column_name not in existing_columns:
                        try:
                            # Add new column to the table
                            self.cursor.execute(f"ALTER TABLE {details_data_table} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(f"Error adding column {column_name}: {e}")

            except Exception as e:
                print(f"Error in table creation or column addition: {e}")

            try:
                # Prepare and execute the SQL query for inserting data
                fields = ', '.join([f"`{field.replace(' ', '_')}`" for field in item_fields])
                values = ', '.join(['%s'] * len(item_fields))

                insert_query = f"INSERT  INTO {details_data_table} ({fields}) VALUES ({values})"
                self.cursor.execute(insert_query, tuple(item.values()))

                # Commit the transaction
                self.conn.commit()

            except Exception as e:
                print(f"Error inserting item into database: {e}")

        try:
            # Update `subcat_of_cat_link` status
            if 'URL' in item:
                update_query = "UPDATE subcat_of_cat_link SET status = 'Done' WHERE link = %s"
                self.cursor.execute(update_query, (item['URL'],))
                self.conn.commit()
            else:
                print("URL not found in item.")
        except Exception as e:
            print(f"Error updating master_table: {e}")

        if isinstance(item, BestCatLinksItem):
            try:
                # Get item fields and prepare columns
                item_fields = list(item.keys())
                columns_definitions = ', '.join([f"`{field.replace(' ', '_')}` LONGTEXT" for field in item_fields])

                # Create the table with columns matching item fields
                query = f"""
                                   CREATE TABLE IF NOT EXISTS {cat_link_table} (
                                       id INT AUTO_INCREMENT PRIMARY KEY,

                                       {columns_definitions}
                                   )
                               """
                self.cursor.execute(query)

                # Fetch existing columns in the table
                self.cursor.execute(f"SHOW COLUMNS FROM {cat_link_table}")
                existing_columns = [column[0] for column in self.cursor.fetchall()]

                # Add new columns if they don't exist
                for field in item_fields:
                    column_name = field.replace(' ', '_')
                    if column_name not in existing_columns:
                        try:
                            # Add new column to the table
                            self.cursor.execute(f"ALTER TABLE {cat_link_table} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(f"Error adding column {column_name}: {e}")

            except Exception as e:
                print(f"Error in table creation or column addition: {e}")

            try:
                # Prepare and execute the SQL query for inserting data
                fields = ', '.join([f"`{field.replace(' ', '_')}`" for field in item_fields])
                values = ', '.join(['%s'] * len(item_fields))

                insert_query = f"INSERT  INTO {cat_link_table} ({fields}) VALUES ({values})"
                self.cursor.execute(insert_query, tuple(item.values()))

                # Commit the transaction
                self.conn.commit()

            except Exception as e:
                print(f"Error inserting item into database: {e}")


        if isinstance(item, SubcatLinksItem):
            try:
                # Get item fields and prepare columns
                item_fields = list(item.keys())
                columns_definitions = ', '.join([f"`{field.replace(' ', '_')}` LONGTEXT" for field in item_fields])

                # Create the table with columns matching item fields
                query = f"""
                                   CREATE TABLE IF NOT EXISTS {subcat_link_table} (
                                       `id` INT AUTO_INCREMENT PRIMARY KEY,

                                       {columns_definitions}
                                   )
                               """
                self.cursor.execute(query)

                # Fetch existing columns in the table
                self.cursor.execute(f"SHOW COLUMNS FROM {subcat_link_table}")
                existing_columns = [column[0] for column in self.cursor.fetchall()]

                # Add new columns if they don't exist
                for field in item_fields:
                    column_name = field.replace(' ', '_')
                    if column_name not in existing_columns:
                        try:
                            # Add new column to the table
                            self.cursor.execute(f"ALTER TABLE {subcat_link_table} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(f"Error adding column {column_name}: {e}")

            except Exception as e:
                print(f"Error in table creation or column addition: {e}")

            try:
                # Prepare and execute the SQL query for inserting data
                fields = ', '.join([f"`{field.replace(' ', '_')}`" for field in item_fields])
                values = ', '.join(['%s'] * len(item_fields))

                insert_query = f"INSERT  INTO {subcat_link_table} ({fields}) VALUES ({values})"
                self.cursor.execute(insert_query, tuple(item.values()))

                # Commit the transaction
                self.conn.commit()

            except Exception as e:
                print(f"Error inserting item into database: {e}")

        if isinstance(item, Subcat_Of_cat_LinksItem):
            try:
                # Get item fields and prepare columns
                item_fields = list(item.keys())
                columns_definitions = ', '.join([f"`{field.replace(' ', '_')}` LONGTEXT" for field in item_fields])

                # Create the table with columns matching item fields
                query = f"""
                                   CREATE TABLE IF NOT EXISTS {subcat_Of_cat_link_table} (
                                       `id` INT AUTO_INCREMENT PRIMARY KEY,

                                       {columns_definitions}
                                   )
                               """
                self.cursor.execute(query)

                # Fetch existing columns in the table
                self.cursor.execute(f"SHOW COLUMNS FROM {subcat_Of_cat_link_table}")
                existing_columns = [column[0] for column in self.cursor.fetchall()]

                # Add new columns if they don't exist
                for field in item_fields:
                    column_name = field.replace(' ', '_')
                    if column_name not in existing_columns:
                        try:
                            # Add new column to the table
                            self.cursor.execute(f"ALTER TABLE {subcat_Of_cat_link_table} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(f"Error adding column {column_name}: {e}")

            except Exception as e:
                print(f"Error in table creation or column addition: {e}")

            try:
                # Prepare and execute the SQL query for inserting data
                fields = ', '.join([f"`{field.replace(' ', '_')}`" for field in item_fields])
                values = ', '.join(['%s'] * len(item_fields))

                insert_query = f"INSERT  INTO {subcat_Of_cat_link_table} ({fields}) VALUES ({values})"
                self.cursor.execute(insert_query, tuple(item.values()))

                # Commit the transaction
                self.conn.commit()

            except Exception as e:
                print(f"Error inserting item into database: {e}")


        return item
