from volatility_models import WingModel
from xlwings import Book as xw_Book
from pandas import DataFrame as pd_DataFrame
from numpy import hstack as np_hstack
from numpy import array as np_array


xlsx_path = "C:/Users/hoore/Documents/workspace/OHS/50ETF_IV_observe_tool.xlsm"
max_rows = 30


mdl_dct = {}


def recalibrate(sht_str: str):
    global c_mdl, p_mdl

    if sht_str in mdl_dct:
        c_mdl, p_mdl = mdl_dct[sht_str]
    else:
        c_mdl = WingModel(2.9, 2.9, vol_ref=0.1)
        p_mdl = WingModel(2.9, 2.9, vol_ref=0.2)

    sht = xw_Book(xlsx_path).sheets[sht_str]

    rd_df = pd_DataFrame(data=sht[f"C2:I{max_rows}"].value,
                           columns=["ca", "cf", "cb", "K", "pa", "pf", "pb"]).set_index("K")

    rd_df.loc[:, ["cf", "pf"]] = 0.0

    data_df = rd_df.dropna()
    n_cols = len(data_df)

    c_mdl.fit(np_hstack((data_df.ca.values, data_df.cb.values)),
              K_l := np_hstack((data_df.index.values, data_df.index.values))
              )

    p_mdl.fit(np_hstack((data_df.pa.values, data_df.pb.values)),
              K_l
              )

    sht[f"D2:D{n_cols+1}"].options(np_array, expand="down").value = c_mdl.predict(K_s := data_df.index.values).reshape(-1, 1)
    sht[f"H2:H{n_cols+1}"].options(np_array, expand="down").value = p_mdl.predict(K_s).reshape(-1, 1)
