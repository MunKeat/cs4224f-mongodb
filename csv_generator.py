import os
import pandas as pd


class Data:
    # TODO: Set as Configuration
    DEBUG = True
    DATA_PATH = None

    def debug(self, message):
        if self.DEBUG:
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
            'orders': ["w_id", "d_id", "o_id", "c_id", "o_carrier_id",
                       "o_ol_cnt", "o_all_local", "o_entry_d"],
            # Item
            'item': ["i_id", "i_name", "i_price", "i_im_id", "i_data"],
            # Orderline
            'orderline': ["w_id", "d_id", "o_id", "ol_number", "ol_i_id",
                          "ol_delivery_d", "ol_amount", "ol_supply_w_id",
                          "ol_quantity", "ol_dist_info"],
            # Stock
            'stock': ["w_id", "i_id", "s_quantity", "s_ytd", "s_order_cnt",
                      "s_remote_cnt", "s_dist_01", "s_dist_02", "s_dist_03",
                      "s_dist_04", "s_dist_05", "s_dist_06", "s_dist_07",
                      "s_dist_08", "s_dist_09", "s_dist_10", "s_data"]

        }[table]
        filename = table + ".csv"
        filepath = os.path.join(os.path.sep, self.DATA_PATH, filename)
        dataframe = pd.read_csv(filepath, na_values='null',
                                header=None, dtype=str)
        dataframe.columns = columns
        self.debug("Original {}: {}\n".format(filename, dataframe.shape))
        return dataframe

    def helper_write_csv(self, dataframe, filename, null_value='null'):
        filepath = os.path.join(os.path.sep, self.DATA_PATH, filename)
        dataframe.to_csv(filepath, na_rep="null", header=False,
                         index=False)

    # TODO: Files to be Written

    def preprocess(self):
        # TODO: Files to be Written
        pass
