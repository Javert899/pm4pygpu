from pm4pygpu.constants import Constants

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

def build_cases_df(df):
	cases_df = df.groupby(Constants.TARGET_CASE_IDX).agg({Constants.TARGET_TIMESTAMP: "min", Constants.TARGET_TIMESTAMP + "_2": "max", Constants.TARGET_EV_IDX: "count"}).reset_index()
	cases_df[Constants.CASE_DURATION] = cases_df[Constants.TARGET_TIMESTAMP + "_2"] - cases_df[Constants.TARGET_TIMESTAMP]
	cases_df = cases_df.sort_values(Constants.TARGET_TIMESTAMP)
	return cases_df

def filter_on_case_size(df, min_size=1, max_size=1000000000):
	cdf = build_cases_df(df)
	cdf = cdf.query(Constants.TARGET_EV_IDX + " >= "+str(min_size)+" and "+Constants.TARGET_EV_IDX + " <= "+str(max_size))[Constants.TARGET_CASE_IDX]
	return df[df[Constants.TARGET_CASE_IDX].isin(cdf)]

def filter_on_case_perf(df, min_perf=0, max_perf=1000000000):
	cdf = build_cases_df(df)
	cdf = cdf.query(Constants.CASE_DURATION + " >= "+str(min_perf)+" and "+Constants.CASE_DURATION + " <= "+str(max_perf))[Constants.TARGET_CASE_IDX]
	return df[df[Constants.TARGET_CASE_IDX].isin(cdf)]
