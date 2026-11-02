"""Shared simulation helpers for ASPIRE power analyses."""

from __future__ import annotations

import numpy as np


def bootstrap_patients(count_matrix, patient_ids, case_status, n_cancer, n_control, seed=42):
    """Resample patients within case groups while preserving all patient samples."""
    counts = np.asarray(count_matrix)
    patients = np.asarray(patient_ids).astype(str)
    status = np.asarray(case_status).astype(str)
    if not (len(counts) == len(patients) == len(status)):
        raise ValueError("count_matrix, patient_ids, and case_status must have matching rows")

    cancer_patients = np.unique(patients[status == "Cancer"])
    control_patients = np.unique(patients[np.isin(status, ["Control", "Non-Cancer"])])
    if cancer_patients.size == 0 or control_patients.size == 0:
        raise ValueError(
            "Patient bootstrap requires both case groups; "
            f"found cancer={cancer_patients.size}, control={control_patients.size}"
        )

    rng = np.random.default_rng(seed)
    draws = [
        (rng.choice(cancer_patients, size=int(n_cancer), replace=True), "Cancer"),
        (rng.choice(control_patients, size=int(n_control), replace=True), "Control"),
    ]
    indices = []
    boot_patients = []
    boot_status = []
    draw_counts = {}
    for selected, label in draws:
        for patient in selected:
            draw_index = draw_counts.get(patient, 0)
            draw_counts[patient] = draw_index + 1
            boot_id = f"{patient}__b{draw_index}"
            patient_indices = np.flatnonzero(patients == patient)
            indices.extend(patient_indices.tolist())
            boot_patients.extend([boot_id] * len(patient_indices))
            boot_status.extend([label] * len(patient_indices))

    return counts[indices].copy(), np.asarray(boot_patients), np.asarray(boot_status)


def spike_in_fold_change(count_matrix, group_labels, feature_indices, fold_change, target_group="Cancer"):
    """Apply a compositional spike while preserving each sample's library size."""
    counts = np.asarray(count_matrix)
    labels = np.asarray(group_labels).astype(str)
    out = counts.copy()
    indices = np.asarray(feature_indices, dtype=int)
    if indices.size == 0 or float(fold_change) == 1.0:
        return out
    if np.any(indices < 0) or np.any(indices >= counts.shape[1]):
        raise IndexError("feature_indices contains an out-of-range column")
    if float(fold_change) <= 0:
        raise ValueError("fold_change must be positive")

    for row_index in np.flatnonzero(labels == str(target_group)):
        row = counts[row_index].astype(float)
        library_size = int(round(row.sum()))
        if library_size <= 0:
            continue
        row[indices] *= float(fold_change)
        scaled = row / row.sum() * library_size
        integer = np.floor(scaled).astype(int)
        remainder = library_size - int(integer.sum())
        if remainder:
            order = np.argsort(-(scaled - integer))
            integer[order[:remainder]] += 1
        out[row_index] = integer
    return out


def filter_by_sample_type(count_matrix, patient_ids, case_status, sample_types, target_type):
    """Filter aligned sample-level arrays to one sample type."""
    counts = np.asarray(count_matrix)
    patients = np.asarray(patient_ids)
    status = np.asarray(case_status)
    types = np.asarray(sample_types).astype(str)
    if not (len(counts) == len(patients) == len(status) == len(types)):
        raise ValueError("All sample-level inputs must have matching rows")
    keep = types == str(target_type)
    return counts[keep].copy(), patients[keep].copy(), status[keep].copy()


def aggregate_to_patient_level(count_matrix, patient_ids):
    """Sum sample counts for each patient and return patient x feature counts."""
    counts = np.asarray(count_matrix)
    patients = np.asarray(patient_ids)
    if len(counts) != len(patients):
        raise ValueError("count_matrix and patient_ids must have matching rows")
    unique_patients = np.unique(patients)
    patient_counts = np.zeros((len(unique_patients), counts.shape[1]), dtype=counts.dtype)
    for index, patient in enumerate(unique_patients):
        patient_counts[index] = counts[patients == patient].sum(axis=0)
    return patient_counts, unique_patients
