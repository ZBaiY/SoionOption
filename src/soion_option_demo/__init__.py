# DEMO-ONLY MODULE
# Intended final location: TBD (package will be dissolved; components migrate to respective SoionLab layers)
"""
SoionOption Demo Scaffolding Package

This package contains placeholder modules for the demo pipeline.
QC/MC/Regime logic is NOT implemented - scaffolding only.
"""

__version__ = "0.0.1"

from .pipeline import DemoPipeline
from .io import load_sample_data, write_artifact
from .qc import QCPlaceholder
from .mc import MCPlaceholder
from .regime import RegimePlaceholder
from .sde import SDEPlaceholder
from .surface import SurfacePlaceholder
