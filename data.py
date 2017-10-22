from config import parameters as conf
import os
import pandas as pd


class Data:

    def debug(self, message):
        if conf['debug']:
            print(message)

    def read_original_csv(self, table):
        columns = {
            # Warehouse
            'warehouse': ["w_id", "w_name", "w_street_1", "w_street_2",
                          "w_city", "w_state", "w_zip", "w_tax", "w_ytd"],
            # District
            'district': ["w_id", "d_id", "d_name", "d_street_1", "d_street_2",
                         "d_city", "d_state", "d_zip", "d_tax", "d_ytd",
                         "d_next_o_id"],
            # Customer
            'customer': ["w_id", "d_id", "c_id", "c_first", "c_middle",
                         "c_last", "c_street_1", "c_street_2", "c_city",
                         "c_state", "c_zip", "c_phone", "c_since", "c_credit",
                         "c_credit_lim", "c_discount", "c_balance",
                         "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt",
                         "c_data"],
            # Orders
            'order': ["w_id", "d_id", "o_id", "c_id", "o_carrier_id",
                      "o_ol_cnt", "o_all_local", "o_entry_d"],
            # Item
            'item': ["i_id", "i_name", "i_price", "i_im_id", "i_data"],
            # Orderline
            'order-line': ["w_id", "d_id", "o_id", "ol_number", "ol_i_id",
                           "ol_delivery_d", "ol_amount", "ol_supply_w_id",
                           "ol_quantity", "ol_dist_info"],
            # Stock
            'stock': ["w_id", "i_id", "s_quantity", "s_ytd", "s_order_cnt",
                      "s_remote_cnt", "s_dist_01", "s_dist_02", "s_dist_03",
                      "s_dist_04", "s_dist_05", "s_dist_06", "s_dist_07",
                      "s_dist_08", "s_dist_09", "s_dist_10", "s_data"]

        }[table]
        filename = table + ".csv"
        filepath = os.path.join(os.path.sep, conf['data-path'], filename)
        dataframe = pd.read_csv(filepath, na_values='null',
                                header=None, dtype=str)
        dataframe.columns = columns
        self.debug("Original {}: {}\n".format(filename, dataframe.shape))
        return dataframe

    def helper_write_csv(self, dataframe, filename, null_value='null'):
        filepath = os.path.join(os.path.sep, conf['data-path'], filename)
        dataframe.to_csv(filepath, na_rep="null", header=True,
                         index=False)

    def get_full_filepath(self, filename):
        return os.path.join(os.path.sep, conf['data-path'], filename)

    def rename_as_nested(self, parent, child):
        result = {}
        for child_attribute in child:
            renamed_attribute_name = "{}.{}".format(parent, child_attribute)
            result[child_attribute] = renamed_attribute_name
        return result

    def create_warehouse(self):
        filepath = self.get_full_filepath("mongo_warehouse.csv")
        df_warehouse = self.read_original_csv("warehouse")
        processed_warehouse = df_warehouse[["w_id", "w_name", "w_street_1",
                                            "w_street_2", "w_city", "w_state",
                                            "w_zip", "w_ytd"]]
        self.debug("Processed {}: {}\n".format(filepath,
                                               processed_warehouse.shape))
        # Create key
        processed_warehouse.rename(columns={"_id": "w_id"}, inplace=True)
        # Rename column to allow for nesting
        new_columns = self.rename_as_nested("w_address", ["w_street_1",
                                                          "w_street_2",
                                                          "w_city",
                                                          "w_state",
                                                          "w_zip"])
        processed_warehouse.rename(columns=new_columns, inplace=True)
        self.helper_write_csv(processed_warehouse, filepath)
        # Return filepath if exist
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            return ("warehouse", filepath)

    def create_district(self):
        filepath = self.get_full_filepath("mongo_district.csv")
        df_warehouse = self.read_original_csv("warehouse")
        df_district = self.read_original_csv("district")
        processed_district = pd.merge(df_district, df_warehouse,
                                      on="w_id", how='left')
        processed_district = processed_district[["w_id", "d_id", "d_name",
                                                 "d_street_1", "d_street_2",
                                                 "d_city", "d_state", "d_zip",
                                                 "w_tax", "d_tax", "d_ytd",
                                                 "d_next_o_id"]]
        self.debug("Processed {}: {}\n".format(filepath,
                                               processed_district.shape))
        # Create key
        new_columns = self.rename_as_nested("_id", ["w_id", "d_id"])
        processed_district.rename(columns=new_columns, inplace=True)
        # Rename column to allow for nesting
        new_columns = self.rename_as_nested("d_address", ["d_street_1",
                                                          "d_street_2",
                                                          "d_city",
                                                          "d_state",
                                                          "d_zip"])
        self.helper_write_csv(processed_district, filepath)
        # Return filepath if exist
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            return ("district", filepath)

    def create_order(self):
        pass

    def create_customer(self):
        filepath = self.get_full_filepath("mongo_customer.csv")
        df_customer = self.read_original_csv("customer")
        self.debug("Processed {}: {}\n".format(filepath,
                                               df_customer.shape))
        # Create key
        new_columns = self.rename_as_nested("_id", ["w_id", "d_id", "c_id"])
        df_customer.rename(columns=new_columns, inplace=True)
        # Rename column to allow for nesting
        new_columns = self.rename_as_nested("c_name", ["c_first",
                                                       "c_middle",
                                                       "c_last"])
        df_customer.rename(columns=new_columns, inplace=True)
        new_columns = self.rename_as_nested("c_address", ["c_street_1",
                                                          "c_street_2",
                                                          "c_city",
                                                          "c_state",
                                                          "c_zip"])
        df_customer.rename(columns=new_columns, inplace=True)
        self.helper_write_csv(df_customer, filepath)
        # Return filepath if exist
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            return ("customer", filepath)

    def create_stock(self):
        filepath = self.get_full_filepath("mongo_stock.csv")
        df_stock = self.read_original_csv("stock")
        df_item = self.read_original_csv("item")
        processed_stock = pd.merge(df_stock, df_item, on="i_id", how='left')
        processed_stock = processed_stock[["w_id", "i_id", "s_quantity",
                                           "s_ytd", "s_order_cnt",
                                           "s_remote_cnt", "s_dist_01",
                                           "s_dist_02", "s_dist_03",
                                           "s_dist_04", "s_dist_05",
                                           "s_dist_06", "s_dist_07",
                                           "s_dist_08", "s_dist_09",
                                           "s_dist_10", "s_data", "i_name",
                                           "i_price", "i_im_id", "i_data"]]
        self.debug("Processed {}: {}\n".format(filepath,
                                               processed_stock.shape))
        # Create key
        new_columns = self.rename_as_nested("_id", ["w_id", "i_id"])
        processed_district.rename(columns=new_columns, inplace=True)
        # Rename column to allow for nesting
        new_columns = self.rename_as_nested("district_info", ["s_dist_01",
                                                              "s_dist_02",
                                                              "s_dist_03",
                                                              "s_dist_04",
                                                              "s_dist_05",
                                                              "s_dist_06",
                                                              "s_dist_07",
                                                              "s_dist_08",
                                                              "s_dist_09",
                                                              "s_dist_10"])
        processed_stock.rename(columns=new_columns, inplace=True)
        self.helper_write_csv(processed_stock, filepath)
        # Return filepath if exist
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            return ("stock", filepath)

    def preprocess(self):
        list_of_processed_files = []
        list_of_processed_files.append(self.create_warehouse())
        list_of_processed_files.append(self.create_district())
        # list_of_processed_files.append(self.create_order())
        list_of_processed_files.append(self.create_customer())
        list_of_processed_files.append(self.create_stock())
        return list_of_processed_files
