from functools import partial
from numpy import array as np_array
from numpy import ndarray as np_ndarray
from numpy import inf as np_inf
from math import log
from scipy.optimize import least_squares

from utils import _vec


class AbstractVolModel(object):

    def fit(self, iv_ar: np_ndarray, K: np_ndarray) -> object:
        raise NotImplementedError("fit")

    def predict(self, K: np_ndarray) -> np_ndarray:
        raise NotImplementedError("K")


class WingModel(AbstractVolModel):

    """
    Ref: http://sourceforge.net/p/quantlib/mailman/attachment/SNT147-DS14C62FD328A11C206965D6AED90%40phx.gbl/1/
    """

    def __init__(self, atm_f: float = None, p_ref: float = None, ssr: float = 100, vol_ref: float = 0.2,
                 vcr: float = 0.0, slp_ref: float = 0.8, scr: float = 0.0, pt_crv: float = 0.5, cl_crv: float = 0.5,
                 dn_ctof: float = -0.5, up_ctof: float = 0.5, dn_smrg: float = 0.5, up_smrg: float = 0.5):

        assert atm_f is not None
        assert p_ref > 0.0
        assert dn_ctof < 0.0
        assert up_ctof > 0.0
        assert 0.0 <= ssr <= 100.0
        assert dn_smrg > 0.0
        assert up_smrg > 0.0

        self.atm_f = atm_f
        self.p_ref = p_ref
        self.ssr = ssr
        self.vol_ref = vol_ref
        self.vcr = vcr
        self.slp_ref = slp_ref
        self.scr = scr
        self.pt_crv = pt_crv
        self.cl_crv = cl_crv
        self.dn_ctof = dn_ctof
        self.up_ctof = up_ctof
        self.dn_smrg = dn_smrg
        self.up_smrg = up_smrg

    def fit(self, iv_ar: np_ndarray, K: np_ndarray) -> AbstractVolModel:
        pfunc = partial(WingModel._predict, K=K)

        w_0 = np_array([self.atm_f, self.ssr, self.vol_ref, self.vcr, self.slp_ref, self.scr, self.pt_crv, self.cl_crv,
                        self.dn_ctof, self.up_ctof, self.dn_smrg, self.up_smrg, self.p_ref])

        def tgt_func(w: np_ndarray) -> np_ndarray:
            return pfunc(*w) - iv_ar

        self.atm_f, self.ssr, self.vol_ref, self.vcr, self.slp_ref, self.scr, self.pt_crv, self.cl_crv, self.dn_ctof, \
        self.up_ctof, self.dn_smrg, self.up_smrg, self.p_ref = least_squares(tgt_func, w_0,
                                                                             bounds=[(0.0, 0.0, 0.0, -np_inf,
                                                                                     -np_inf, -np_inf, -np_inf,
                                                                                     -np_inf, -np_inf, 0.0, 0.0,
                                                                                     0.0, 0.0),
                                                                                     (np_inf, 100.0, np_inf, np_inf,
                                                                                     np_inf, np_inf, np_inf,
                                                                                     np_inf, 0.0, np_inf, np_inf,
                                                                                     np_inf, np_inf)
                                                                                     ]
                                                                             ).x

        return self

    @staticmethod
    @_vec
    def _predict(atm_f, ssr, vol_ref, vcr, slp_ref, scr, pt_crv, cl_crv, dn_ctof, up_ctof, dn_smrg, up_smrg, p_ref, K) \
            -> np_ndarray:

        f = atm_f**(ssr / 100) * p_ref**(1 - ssr / 100)

        vc = vol_ref - vcr * ssr * (atm_f - p_ref) / p_ref
        sc = slp_ref - scr * ssr * (atm_f - p_ref) / p_ref

        x = log(K / f)

        if x <= dn_ctof * (1 + dn_smrg):
            return vc + dn_ctof * (2 + dn_smrg) * sc / 2 + (1 + dn_smrg) * pt_crv * dn_ctof**2
        elif x <= dn_ctof:
            return vc - (1 + 1 / dn_smrg) * pt_crv * dn_ctof**2 - sc * dn_ctof / 2 / dn_smrg + (1 + 1 / dn_smrg) \
            * (2 * pt_crv * dn_ctof + sc) * x - (pt_crv / dn_smrg + sc / 2 / dn_ctof / dn_smrg) * x**2
        elif x <= 0:
            return vc + sc * x + pt_crv * x**2
        elif x <= up_ctof:
            return vc + sc * x + cl_crv * x**2
        elif x <= up_ctof * (1 + up_smrg):
            return vc - (1 + 1 / up_smrg) * cl_crv * up_ctof**2 - sc * up_ctof / 2 / up_smrg + (1 + 1 / up_smrg) \
            * (2 * cl_crv * up_ctof + sc) * x - (cl_crv / up_smrg + sc / 2 / up_ctof / up_smrg) * x**2
        else:
            return vc + up_ctof * (2 + dn_smrg) * sc / 2 + (1 + up_smrg) * cl_crv * up_ctof**2

    def predict(self, K: np_ndarray) -> np_ndarray:
        return self._predict(self.atm_f, self.ssr, self.vol_ref, self.vcr, self.slp_ref, self.scr, self.pt_crv,
                             self.cl_crv, self.dn_ctof, self.up_ctof, self.dn_smrg, self.up_smrg, self.p_ref, K)
