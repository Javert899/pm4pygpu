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
	cases_df = df.groupby(Constants.TARGET_CASE_IDX).agg({Constants.TARGET_TIMESTAMP: "min", Constants.TARGET_TIMESTAMP + "_2": "max"})
	cases_df[Constants.CASE_DURATION] = cases_df[Constants.TARGET_TIMESTAMP + "_2"] - cases_df[Constants.TARGET_TIMESTAMP]
	cases_df = cases_df.sort_values(Constants.TARGET_TIMESTAMP)
	return cases_df
