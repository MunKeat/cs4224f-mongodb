from config import parameters as conf
from pathos.multiprocessing import ProcessingPool as Pool
import os
import pandas as pd
import time


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

    def to_distinct_list(self, x):
        return str(list(set(x.values)))

    def to_list(self, x):
        return str(x.values.tolist())

    def get_preprocessed_orderline(self):
        " Return orderline dataframe with item name "
        df_orderline = self.read_original_csv("order-line")
        df_item = self.read_original_csv("item")
        df_item = df_item[["i_id", "i_name"]]
        processsed_orderline = pd.merge(df_orderline, df_item,
                                        left_on=["ol_i_id"], right_on=["i_id"],
                                        how="left")
        processsed_orderline = processsed_orderline[["w_id", "d_id", "o_id",
                                                     "ol_number", "ol_i_id",
                                                     "i_name",
                                                     "ol_delivery_d",
                                                     "ol_amount",
                                                     "ol_supply_w_id",
                                                     "ol_quantity",
                                                     "ol_dist_info"]]
        processsed_orderline.rename(columns={'i_name': 'ol_i_name'},
                                    inplace=True)
        return processsed_orderline

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
        filepath = self.get_full_filepath("mongo_orders.csv")
        df_orders = self.read_original_csv("order")
        df_orderlines = self.get_preprocessed_orderline()
        # Get popular items
        orderline_agg_id = ['w_id', 'd_id', 'o_id']
        # 1. Get orderline(s) with maximum quantity per order
        grp_pop_product = df_orderlines[df_orderlines.groupby(
            orderline_agg_id)['ol_quantity'].transform(max) ==
            df_orderlines['ol_quantity']]
        grp_pop_product = grp_pop_product[["w_id", "d_id", "o_id", "ol_i_id",
                                           "ol_i_name", "ol_quantity"]]
        grp_pop_product = grp_pop_product.groupby(orderline_agg_id)
        agg_2_start = time.time()
        # 2. Get popular item ID(s) as a list
        grp_pop_ids = grp_pop_product['ol_i_id'].agg([self.to_list])\
                                                .reset_index()
        grp_pop_ids.rename(columns={'to_list': 'popular_item_id'},
                           inplace=True)
        agg_2_end = time.time()
        agg_3_start = time.time()
        # 3. Get popular item name(s) as a list
        grp_pop_names = grp_pop_product['ol_i_name'].agg([self.to_list])\
                                                    .reset_index()
        grp_pop_names.rename(columns={'to_list': 'popular_item_name'},
                             inplace=True)
        agg_3_end = time.time()
        agg_4_start = time.time()
        # 4. Get popular item quantity
        grp_pop_qty = grp_pop_product['ol_quantity'].nth(0).reset_index()
        grp_pop_qty.rename(columns={'ol_quantity': 'popular_item_qty'},
                           inplace=True)
        agg_4_end = time.time()
        agg_5_start = time.time()
        # 5. Get list of all item IDs per order
        grp_order_ids = df_orderlines.groupby(orderline_agg_id)['ol_i_id'].\
                                      agg([self.to_distinct_list]).\
                                      reset_index()
        grp_order_ids.rename(columns={'to_distinct_list': 'ordered_items'},
                             inplace=True)
        agg_5_end = time.time()
        # 6. Get merged
        processed_orders = df_orders.merge(grp_pop_ids, on=orderline_agg_id).\
                                    merge(grp_pop_names, on=orderline_agg_id).\
                                    merge(grp_pop_qty, on=orderline_agg_id).\
                                    merge(grp_order_ids, on=orderline_agg_id)
        self.debug("Processed {}: {}\n".format(filepath,
                                               processed_orders.shape))
        self.helper_write_csv(processed_orders, filepath)
        # self.debug("Aggregate 2: {}s\n".format(agg_2_end - agg_2_start))
        # self.debug("Aggregate 3: {}s\n".format(agg_3_end - agg_3_start))
        # self.debug("Aggregate 4: {}s\n".format(agg_4_end - agg_4_start))
        # self.debug("Aggregate 5: {}s\n".format(agg_5_end - agg_5_start))
        # Return filepath if exist
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            return ("orders", filepath)


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
        processed_stock.rename(columns=new_columns, inplace=True)
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
        pool = Pool()
        start = time.time()
        # Run in parallel
        res_warehouse = pool.apipe(self.create_warehouse)
        res_district = pool.apipe(self.create_district)
        res_order = pool.apipe(self.create_order)
        res_customer = pool.apipe(self.create_customer)
        res_stock = pool.apipe(self.create_stock)
        # Consolidate result
        pool.close()
        pool.join()
        list_of_processed_files = [res_warehouse.get(), res_district.get(),
                                   res_order.get(), res_customer.get(),
                                   res_stock.get()]
        end = time.time()
        self.debug("Preprocessing of csv file took {}s".format(end - start))
        return list_of_processed_files
