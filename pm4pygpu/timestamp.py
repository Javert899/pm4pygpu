from pm4pygpu.cases_df import build_cases_df
from pm4pygpu.constants import Constants
import sys


def filter_events(df, min_timest=0, max_timest=100000000000, att=None):
	if att is None:
		col = Constants.TARGET_TIMESTAMP
	else:
		col = Constants.TEMP_COLUMN_1
		df = df.copy()
		df[col] = df[att].astype(int) // 10**6
	df = df.query(col+" >= "+str(min_timest)+" and "+col + " <= "+str(max_timest))
	return df

def filter_cases_contained(df, min_timest=0, max_timest=100000000000, att=None):
	if att is None:
		col1 = Constants.TARGET_TIMESTAMP
		col2 = Constants.TARGET_TIMESTAMP + "_2"
	else:
		col1 = Constants.TEMP_COLUMN_1
		col2 = Constants.TEMP_COLUMN_2
	cdf = build_cases_df(df, att=att)
	cdf = cdf.query(col1+" >= "+str(min_timest)+" and "+col2+" <= "+str(max_timest))[Constants.TARGET_CASE_IDX]
	return df[df[Constants.TARGET_CASE_IDX].isin(cdf)]

def filter_cases_intersecting(df, min_timest=0, max_timest=100000000000, att=None):
	if att is None:
		col1 = Constants.TARGET_TIMESTAMP
		col2 = Constants.TARGET_TIMESTAMP + "_2"
	else:
		col1 = Constants.TEMP_COLUMN_1
		col2 = Constants.TEMP_COLUMN_2
	cdf = build_cases_df(df, att=att)
	query_parts = []
	query_parts.append("(" + col1 + " >= " + str(min_timest)+" and " + col1 + " <= " + str(max_timest) + ")")
	query_parts.append("(" + col2 + " >= " + str(min_timest)+" and " + col2 + " <= " + str(max_timest) + ")")
	query_parts.append("(" + col1 + " <= " + str(min_timest)+" and " + col2 + " >= " + str(max_timest) + ")")
	query = " or ".join(query_parts)
	cdf = cdf.query(query)[Constants.TARGET_CASE_IDX]
	return df[df[Constants.TARGET_CASE_IDX].isin(cdf)]

def timestamp_attribute_values(df, attribute=None, n_values=sys.maxsize):
	if attribute is not None:
		df = df.copy()
		df[Constants.TEMP_COLUMN_1] = df[attribute].astype("int") // 10**6
		attribute = Constants.TEMP_COLUMN_1
	else:
		attribute = Constants.TARGET_TIMESTAMP
	serie = df[attribute].dropna().sort_values()
	lenn = len(serie)
	if n_values < lenn:
		serie = serie.sample(n_values)
	ret = serie.to_arrow().to_pylist()
	for i in range(len(ret)):
		ret[i] = float(ret[i])
	return ret

