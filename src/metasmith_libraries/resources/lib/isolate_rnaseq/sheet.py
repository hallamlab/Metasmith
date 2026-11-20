import numpy as np
import pandas as pd


def read_sheet(path) -> pd.DataFrame:
    sheet = pd.read_csv(path, sep="\t", dtype={"library": str}).set_index("library")
    sheet["ppm"] = sheet.copper_ppm.astype(float)
    sheet = sheet.sort_values("ppm")
    sheet["label"] = [f"{p:g}ppm" for p in sheet.ppm]
    sheet["log_dose"] = np.log10(sheet.ppm + 1)
    return sheet
