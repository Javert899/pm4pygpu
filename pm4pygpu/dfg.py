from pm4pygpu.constants import Constants
from numba import cuda
import numpy as np

def paths_udf(custom_column_temp_1, custom_column_temp_2):
	for i in range(cuda.threadIdx.x, len(custom_column_temp_2), cuda.blockDim.x):
		if i > 0:
			custom_column_temp_1[i] = custom_column_temp_2[i-1]

def get_frequency_dfg(df, att=None):
	if att is None:
		col1 = Constants.TARGET_ACTIVITY
		col3 = Constants.TARGET_PRE_ACTIVITY
		col2 = Constants.TARGET_ACTIVITY_CODE
	else:
		df = df.copy()
		col1 = att
		col2 = Constants.TEMP_COLUMN_2
		col3 = Constants.TEMP_COLUMN_1
		df[col1] = df[col1].astype("category")
		df[col2] = df[col1].cat.codes
		df = df.groupby(Constants.TARGET_CASE_IDX).apply_grouped(paths_udf, incols=[col2], outcols={col3: np.int32})
	av = df[col1].cat.categories.to_arrow().to_pylist()
	av = {i: av[i] for i in range(len(av))}
	dfg = df.query(Constants.TARGET_CASE_IDX+" == "+Constants.TARGET_PRE_CASE).groupby([col3, col2]).count()[Constants.TARGET_VARIANT_NUMBER].to_pandas().to_dict()
	dfg = {(str(av[x[0]]), str(av[x[1]])): int(y) for x, y in dfg.items()}
	return dfg

def get_performance_dfg(df, att=None):
	if att is None:
		col1 = Constants.TARGET_ACTIVITY
		col3 = Constants.TARGET_PRE_ACTIVITY
		col2 = Constants.TARGET_ACTIVITY_CODE
	else:
		df = df.copy()
		col1 = att
		col2 = Constants.TEMP_COLUMN_2
		col3 = Constants.TEMP_COLUMN_1
		df[col1] = df[col1].astype("category")
		df[col2] = df[col1].cat.codes
		df = df.groupby(Constants.TARGET_CASE_IDX).apply_grouped(paths_udf, incols=[col2], outcols={col3: np.int32})
	av = df[col1].cat.categories.to_arrow().to_pylist()
	av = {i: av[i] for i in range(len(av))}
	dfg_perf = df.query(Constants.TARGET_CASE_IDX+" == "+Constants.TARGET_PRE_CASE).groupby([col3, col2]).agg({Constants.TIMESTAMP_DIFF: "mean"})[Constants.TIMESTAMP_DIFF].to_pandas().to_dict()
	dfg_perf = {(str(av[x[0]]), str(av[x[1]])): float(y) for x, y in dfg_perf.items()}
	return dfg_perf

def filter_paths(df0, allowed_paths, att=None, retain=True):
	if att is None:
		col1 = Constants.TARGET_ACTIVITY
		col3 = Constants.TARGET_PRE_ACTIVITY
		col2 = Constants.TARGET_ACTIVITY_CODE
		df = df0
	else:
		df = df0.copy()
		col1 = att
		col2 = Constants.TEMP_COLUMN_2
		col3 = Constants.TEMP_COLUMN_1
		df[col1] = df[col1].astype("category")
		df[col2] = df[col1].cat.codes
		df = df.groupby(Constants.TARGET_CASE_IDX).apply_grouped(paths_udf, incols=[col2], outcols={col3: np.int32})
	av = df[col1].cat.categories.to_arrow().to_pylist()
	av = {av[i]: i for i in range(len(av))}
	allowed_paths = [(av[x[0]], av[x[1]]) for x in allowed_paths]
	filters = []
	for ap in allowed_paths:
		filters.append("(" + col3 + " == " + str(ap[0]) + " and " + col2 + " == " + str(ap[1]) + ")")
	query = " or ".join(filters)
	cdf = df.query(Constants.TARGET_CASE_IDX+" == "+Constants.TARGET_PRE_CASE).query(query)[Constants.TARGET_CASE_IDX]
	if retain:
		return df0[df0[Constants.TARGET_CASE_IDX].isin(cdf)]
	else:
		return df0[~df0[Constants.TARGET_CASE_IDX].isin(cdf)]
