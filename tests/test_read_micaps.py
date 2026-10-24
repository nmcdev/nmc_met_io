"""Regression tests for MICAPS surface pressure decoding."""

from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

from nmc_met_io.read_micaps import read_micaps_1


def write_micaps_1(tmp_path, pressures, columns=24):
    """Write a complete MICAPS 1 file with one station per pressure."""
    rows = []
    for i, pressure in enumerate(pressures):
        row = [str(value) for value in (
            50000 + i, 110 + i, 30 + i, 100, 1, 5, 180, 3,
            pressure, 2, 0, 0, 1, 2, 3, 800, 10, 20, 0, 15,
            1, 2, 1 if columns == 26 else 0, 2 if columns == 26 else 0,
        )]
        if columns == 26:
            row.extend(['3', '-2'])
        rows.append(' '.join(row))
    path = tmp_path / 'surface.000'
    path.write_text(
        'diamond 1 surface\n2026 9 30 6 {}\n{}\n'.format(
            len(rows), '\n'.join(rows)), encoding='utf-8')
    return str(path)


@pytest.mark.parametrize('columns', [24, 26])
@pytest.mark.parametrize('encoded, expected', [
    (0, 1000.0), (1, 1000.1), (132, 1013.2), (599, 1059.9),
    (600, 1060.0), (601, 960.1), (999, 999.9),
])
def test_pressure_is_decoded_once(tmp_path, columns, encoded, expected):
    data = read_micaps_1(write_micaps_1(tmp_path, [encoded], columns))
    assert data.loc[0, 'MSLP'] == pytest.approx(expected)
    assert data.loc[0, 'time'] == datetime(2026, 9, 30, 6)
    assert data.loc[0, 'temperature'] == 15
    assert data.loc[0, 'pressure_3h_trend'] == 2
    if columns == 26:
        assert data.loc[0, 'temperature_24h_trend'] == 3
        assert data.loc[0, 'pressure_24h_trend'] == -2
    else:
        assert 'pressure_24h_trend' not in data.columns


@pytest.mark.parametrize('columns', [24, 26])
@pytest.mark.parametrize('missing', [9999, 'NaN', 'invalid'])
def test_missing_pressure_remains_nan(tmp_path, columns, missing):
    data = read_micaps_1(write_micaps_1(tmp_path, [missing], columns))
    assert np.isnan(data.loc[0, 'MSLP'])


@pytest.mark.parametrize('columns', [24, 26])
def test_mixed_pressures_and_missing_values(tmp_path, columns):
    data = read_micaps_1(write_micaps_1(
        tmp_path, [132, 9999, 601, 600, 'invalid', 0, 999], columns))
    np.testing.assert_allclose(
        data['MSLP'], [1013.2, np.nan, 960.1, 1060.0, np.nan, 1000.0, 999.9])
    assert data['ID'].tolist() == [str(50000 + i) for i in range(7)]


@pytest.mark.parametrize('columns', [24, 26])
def test_region_filter_preserves_pressure_alignment(tmp_path, columns):
    path = write_micaps_1(tmp_path, [999, 132, 9999, 601, 0], columns)
    data = read_micaps_1(path, limit=[31, 111, 33, 113])
    assert data.index.tolist() == [1, 2, 3]
    np.testing.assert_allclose(data['MSLP'], [1013.2, np.nan, 960.1])
    assert data['ID'].tolist() == ['50001', '50002', '50003']


def test_empty_region_still_returns_none(tmp_path):
    path = write_micaps_1(tmp_path, [132])
    assert read_micaps_1(path, limit=[0, 0, 1, 1]) is None


def test_missing_file_still_returns_none(tmp_path):
    assert read_micaps_1(str(tmp_path / 'missing.000')) is None


@pytest.mark.parametrize('filename', ['20020402.000', '20032714.000'])
def test_repository_surface_samples(filename):
    path = (Path(__file__).resolve().parents[1] / 'examples' / 'samples' /
            'micaps' / 'surface_plot' / filename)
    tokens = path.read_text(encoding='gb18030').replace(',', '').split()
    station_count = int(tokens[7])
    rows = np.array(tokens[8:]).reshape(station_count, -1)
    expected = []
    for row in rows:
        encoded = float(row[8])
        if encoded == 9999:
            expected.append(np.nan)
        elif encoded <= 600:
            expected.append(1000.0 + encoded / 10.0)
        else:
            expected.append(900.0 + encoded / 10.0)
    data = read_micaps_1(str(path))
    assert len(data) == station_count
    np.testing.assert_allclose(data['MSLP'], expected)