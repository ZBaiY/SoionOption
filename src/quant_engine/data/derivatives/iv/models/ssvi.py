# quant_engine/iv/ssvi.py

from dataclasses import dataclass
from quant_engine.data.derivatives.option_chain.option_chain import OptionChain
from .surface import IVSurface


@dataclass
class SSVIParams:
    eta: float | None = None
    rho: float | None = None
    theta: float | None = None


class SSVIModel(IVSurface):
    """
    Skeleton SSVI Surface Model.
    This class does NOT implement real math yet.
    It only exposes a clean interface for future fitting.
    """

    def __init__(self, **kwargs):
        # allow user to override initial parameters
        self.params = SSVIParams(
            eta=kwargs.get("eta"),
            rho=kwargs.get("rho"),
            theta=kwargs.get("theta"),
        )

    def fit(self, chain: OptionChain):
        """
        Fit SSVI parameters from option chain.
        (Skeleton: no optimization implemented)
        """
        # TODO: real SSVI calibration
        # For now, use placeholders
        self.params.eta = self.params.eta or 0.2
        self.params.rho = self.params.rho or -0.3
        self.params.theta = self.params.theta or 0.5

        return self

    # ---- Surface Statistics ----

    def atm_iv(self) -> float:
        # TODO: real ATM extraction
        return 0.5 * (self.params.theta or 0.5)

    def smile_slope(self) -> float:
        # TODO: slope of SSVI smile
        return self.params.rho or -0.3

    def smile_curvature(self) -> float:
        # TODO: curvature measure from SSVI formula
        return self.params.eta or 0.2