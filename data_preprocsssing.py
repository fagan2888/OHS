from pandas import read_excel as pd_read_excel
from pandas import DatetimeIndex as pd_DatetimeIndex
from pandas import DataFrame as pd_DataFrame
from pandas import read_hdf as pd_read_hdf
from pandas import to_datetime as pd_to_datetime
from pandas import concat as pd_concat
from os.path import exists as op_exists
from WindPy import w as w_obj
from typing import Dict, Tuple

from consts import OPTION_LIST_FILE, D_DF_COLS, START_DT, END_DT, OBS_DT, MKT_DATA_FILE, DAYS_IN_YEAR


def load_derivatives_df_n_cast() -> Tuple[pd_DataFrame, Dict]:
    d_df = pd_read_excel(OPTION_LIST_FILE)
    d_df.columns = D_DF_COLS
    d_df["lst_dt"] = pd_DatetimeIndex(d_df["lst_dt"])
    d_df["type"] = d_df["name"].apply(lambda x: "c" if "购" in x or "C" in x else "p")
    return d_df, d_df.set_index("d_code").T.to_dict()


def get_mkt_data_days(d_df: pd_DataFrame) -> Tuple:
    if op_exists(MKT_DATA_FILE):
        d_op_df = pd_read_hdf(MKT_DATA_FILE, key="do")
        u_op_df = pd_read_hdf(MKT_DATA_FILE, key="uo")
        d_hp_df = pd_read_hdf(MKT_DATA_FILE, key="dh")
        u_hp_df = pd_read_hdf(MKT_DATA_FILE, key="uh")
        d_lp_df = pd_read_hdf(MKT_DATA_FILE, key="dl")
        u_lp_df = pd_read_hdf(MKT_DATA_FILE, key="ul")
        d_cp_df = pd_read_hdf(MKT_DATA_FILE, key="dc")
        u_cp_df = pd_read_hdf(MKT_DATA_FILE, key="uc")
    else:
        w_obj.start()
        data = w_obj.wsd(",".join(d_df["d_code"].to_list()), "open", START_DT, END_DT, "")
        d_op_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)

        data = w_obj.wsd(",".join(d_df["u_code"].unique().tolist()), "open", START_DT, END_DT, "")
        u_op_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)

        data = w_obj.wsd(",".join(d_df["d_code"].to_list()), "high", START_DT, END_DT, "")
        d_hp_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)

        data = w_obj.wsd(",".join(d_df["u_code"].unique().tolist()), "high", START_DT, END_DT, "")
        u_hp_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)

        data = w_obj.wsd(",".join(d_df["d_code"].to_list()), "low", START_DT, END_DT, "")
        d_lp_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)

        data = w_obj.wsd(",".join(d_df["u_code"].unique().tolist()), "low", START_DT, END_DT, "")
        u_lp_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)

        data = w_obj.wsd(",".join(d_df["d_code"].to_list()), "close", START_DT, END_DT, "")
        d_cp_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)

        data = w_obj.wsd(",".join(d_df["u_code"].unique().tolist()), "close", START_DT, END_DT, "")
        u_cp_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)
        w_obj.close()

        d_op_df.sort_index(inplace=True)
        d_op_df.to_hdf(MKT_DATA_FILE, key="do")
        u_op_df.sort_index(inplace=True)
        u_op_df.to_hdf(MKT_DATA_FILE, key="uo")

        d_hp_df.sort_index(inplace=True)
        d_hp_df.to_hdf(MKT_DATA_FILE, key="dh")
        u_hp_df.sort_index(inplace=True)
        u_hp_df.to_hdf(MKT_DATA_FILE, key="uh")

        d_lp_df.sort_index(inplace=True)
        d_lp_df.to_hdf(MKT_DATA_FILE, key="dl")
        u_lp_df.sort_index(inplace=True)
        u_lp_df.to_hdf(MKT_DATA_FILE, key="ul")

        d_cp_df.sort_index(inplace=True)
        d_cp_df.to_hdf(MKT_DATA_FILE, key="dc")
        u_cp_df.sort_index(inplace=True)
        u_cp_df.to_hdf(MKT_DATA_FILE, key="uc")

    return d_op_df, u_op_df, d_hp_df, u_hp_df, d_lp_df, u_lp_df, d_cp_df, u_cp_df


def get_mkt_data_minutes(d_df: pd_DataFrame, freq: int = 30) -> Tuple:
    if op_exists(MKT_DATA_FILE):
        d_op_df = pd_read_hdf(MKT_DATA_FILE, key="do")
        u_op_df = pd_read_hdf(MKT_DATA_FILE, key="uo")
        d_hp_df = pd_read_hdf(MKT_DATA_FILE, key="dh")
        u_hp_df = pd_read_hdf(MKT_DATA_FILE, key="uh")
        d_lp_df = pd_read_hdf(MKT_DATA_FILE, key="dl")
        u_lp_df = pd_read_hdf(MKT_DATA_FILE, key="ul")
        d_cp_df = pd_read_hdf(MKT_DATA_FILE, key="dc")
        u_cp_df = pd_read_hdf(MKT_DATA_FILE, key="uc")
    else:
        def _data2df(data):
            tdf = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=["code", "p"]).T
            rst_df = pd_concat([df["p"] for _, df in tdf.groupby("code")], axis=1)
            rst_df.columns = [tkr for tkr, _ in tdf.groupby("code")]
            return rst_df.astype(float)

        w_obj.start()
        data = w_obj.wsi(",".join(d_df["d_code"].to_list()), "open", START_DT, END_DT, f"BarSize={freq}")
        d_op_df = _data2df(data)

        data = w_obj.wsi(",".join(d_df["u_code"].unique().tolist()), "open", START_DT, END_DT, f"BarSize={freq}")
        u_op_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)

        data = w_obj.wsi(",".join(d_df["d_code"].to_list()), "high", START_DT, END_DT, f"BarSize={freq}")
        d_hp_df = _data2df(data)

        data = w_obj.wsi(",".join(d_df["u_code"].unique().tolist()), "high", START_DT, END_DT, f"BarSize={freq}")
        u_hp_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)

        data = w_obj.wsi(",".join(d_df["d_code"].to_list()), "low", START_DT, END_DT, f"BarSize={freq}")
        d_lp_df = _data2df(data)

        data = w_obj.wsi(",".join(d_df["u_code"].unique().tolist()), "low", START_DT, END_DT, f"BarSize={freq}")
        u_lp_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)

        data = w_obj.wsi(",".join(d_df["d_code"].to_list()), "close", START_DT, END_DT, f"BarSize={freq}")
        d_cp_df = _data2df(data)

        data = w_obj.wsi(",".join(d_df["u_code"].unique().tolist()), "close", START_DT, END_DT, f"BarSize={freq}")
        u_cp_df = pd_DataFrame(data=data.Data, columns=pd_DatetimeIndex(data.Times), index=data.Codes).T.astype(float)
        w_obj.close()

        d_op_df.sort_index(inplace=True)
        d_op_df.to_hdf(MKT_DATA_FILE, key="do")
        u_op_df.sort_index(inplace=True)
        u_op_df.to_hdf(MKT_DATA_FILE, key="uo")

        d_hp_df.sort_index(inplace=True)
        d_hp_df.to_hdf(MKT_DATA_FILE, key="dh")
        u_hp_df.sort_index(inplace=True)
        u_hp_df.to_hdf(MKT_DATA_FILE, key="uh")

        d_lp_df.sort_index(inplace=True)
        d_lp_df.to_hdf(MKT_DATA_FILE, key="dl")
        u_lp_df.sort_index(inplace=True)
        u_lp_df.to_hdf(MKT_DATA_FILE, key="ul")

        d_cp_df.sort_index(inplace=True)
        d_cp_df.to_hdf(MKT_DATA_FILE, key="dc")
        u_cp_df.sort_index(inplace=True)
        u_cp_df.to_hdf(MKT_DATA_FILE, key="uc")

    return d_op_df, u_op_df, d_hp_df, u_hp_df, d_lp_df, u_lp_df, d_cp_df, u_cp_df


def add_info2p_df(d_p_df: pd_DataFrame) -> pd_DataFrame:
    d_t_df = d_p_df.copy()
    d_t_df["tao"] = (pd_to_datetime(OBS_DT) - d_t_df.index).days / DAYS_IN_YEAR

    # TODO: add term structure, or real financing costs
    d_t_df["r"] = 0.03
    d_t_df["q"] = 0.15

    return d_t_df
