from pm4pygpu.constants import Constants
import sys

def get_first_df(df):
	group_df_min = df.groupby(Constants.TARGET_CASE_IDX).agg({Constants.TARGET_EV_CASE_MULT_ID: "min"})
	idxs = group_df_min[Constants.TARGET_EV_CASE_MULT_ID].unique().to_arrow()
	first_df = df[df[Constants.TARGET_EV_CASE_MULT_ID].isin(idxs)]
	return first_df

def get_last_df(df):
	group_df_max = df.groupby(Constants.TARGET_CASE_IDX).agg({Constants.TARGET_EV_CASE_MULT_ID: "max"})
	idxs = group_df_max[Constants.TARGET_EV_CASE_MULT_ID].unique().to_arrow()
	last_df = df[df[Constants.TARGET_EV_CASE_MULT_ID].isin(idxs)]
	return last_df

def build_cases_df(df, att=None):
	if att is None:
		col1 = Constants.TARGET_TIMESTAMP
		col2 = Constants.TARGET_TIMESTAMP + "_2"
	else:
		col1 = Constants.TEMP_COLUMN_1
		col2 = Constants.TEMP_COLUMN_2
		df = df.copy()
		df[col1] = df[att].astype(int) // 10**6
		df[col2] = df[Constants.TEMP_COLUMN_1]
	cases_df = df.groupby(Constants.TARGET_CASE_IDX).agg({col1: "min", col2: "max", Constants.TARGET_EV_IDX: "count"}).reset_index()
	cases_df[Constants.CASE_DURATION] = cases_df[col2] - cases_df[col1]
	cases_df = cases_df.sort_values(col1)
	return cases_df

def filter_on_case_size(df, min_size=1, max_size=1000000000):
	cdf = build_cases_df(df)
	cdf = cdf.query(Constants.TARGET_EV_IDX + " >= "+str(min_size)+" and "+Constants.TARGET_EV_IDX + " <= "+str(max_size))[Constants.TARGET_CASE_IDX]
	return df[df[Constants.TARGET_CASE_IDX].isin(cdf)]

def filter_on_case_perf(df, min_perf=0, max_perf=1000000000):
	cdf = build_cases_df(df)
	cdf = cdf.query(Constants.CASE_DURATION + " >= "+str(min_perf)+" and "+Constants.CASE_DURATION + " <= "+str(max_perf))[Constants.TARGET_CASE_IDX]
	return df[df[Constants.TARGET_CASE_IDX].isin(cdf)]

def get_case_durations(df, n_values=sys.maxsize):
	cdf = build_cases_df(df)
	serie = cdf[Constants.CASE_DURATION].sort_values()
	lenn = len(serie)
	if n_values < lenn:
		serie = serie.sample(n_values)
	ret = serie.to_arrow().to_pylist()
	for i in range(len(ret)):
		ret[i] = float(ret[i])
	return ret

def get_intervals(df, n_values=500):
	cdf = build_cases_df(df)
	lenn = len(cdf)
	if n_values < lenn:
		cdf = cdf.sample(n_values)
	inte = cdf.to_arrow().to_pydict()
	ret = []
	for i in range(len(inte[Constants.TARGET_TIMESTAMP])):
		ret.append((float(inte[Constants.TARGET_TIMESTAMP][i]), float(inte[Constants.TARGET_TIMESTAMP+"_2"][i])))
	return ret

def get_case_size(df):
	ret = build_cases_df(df).groupby(Constants.TARGET_EV_IDX).count().to_pandas().to_dict()[Constants.TARGET_CASE_IDX]
	ret = {int(x): int(y) for x, y in ret.items()}
	return ret

def get_cases_description(df, start_idx=0, end_idx=200, sort_column=Constants.TARGET_TIMESTAMP, ascending=True):
	cdf = build_cases_df(df)
	cdf = cdf.sort_values(sort_column, ascending=ascending)
	desc = cdf.to_arrow().to_pydict()
	ret = []
	for i in range(start_idx, min(end_idx, len(desc[Constants.TARGET_TIMESTAMP]))):
		el = {}
		el["caseDuration"] = float(desc[Constants.CASE_DURATION][i])
		el["startTime"] = float(desc[Constants.TARGET_TIMESTAMP][i])
		el["endTime"] = float(desc[Constants.TARGET_TIMESTAMP + "_2"][i])
		el["caseId"] = str(desc[Constants.TARGET_CASE_IDX][i])
		ret.append(el)
	return ret
