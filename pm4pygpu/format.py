from pm4pygpu.constants import Constants
from numba import cuda
import numpy as np

def post_grouping_function(custom_column_activity_code, custom_column_timestamp, custom_column_case_idx, custom_column_pre_activity_code, custom_column_pre_timestamp, custom_column_pre_case, custom_column_variant_number):
	for i in range(cuda.threadIdx.x, len(custom_column_activity_code), cuda.blockDim.x):
		custom_column_variant_number[i] = (len(custom_column_activity_code) + i + 1) * (custom_column_activity_code[i] + 1)
		if i > 0:
			custom_column_pre_activity_code[i] = custom_column_activity_code[i-1]
			custom_column_pre_timestamp[i] = custom_column_timestamp[i-1]
			custom_column_pre_case[i] = custom_column_case_idx[i-1]
		else:
			custom_column_pre_case[i] = -1

def post_filtering(df):
	cdf = df.groupby(Constants.TARGET_CASE_IDX)
	df = cdf.apply_grouped(post_grouping_function, incols=[Constants.TARGET_ACTIVITY_CODE, Constants.TARGET_TIMESTAMP, Constants.TARGET_CASE_IDX], outcols={Constants.TARGET_PRE_ACTIVITY: np.int32, Constants.TARGET_PRE_TIMESTAMP: np.int32, Constants.TARGET_PRE_CASE: np.int32, Constants.TARGET_VARIANT_NUMBER: np.int32})
	df[Constants.TIMESTAMP_DIFF] = df[Constants.TARGET_TIMESTAMP] - df[Constants.TARGET_PRE_TIMESTAMP]
	return df

def prefix_columns(df):
	columns = list(df.columns)
	columns = [x.replace("AAA", ":") for x in columns]
	df.columns = columns
	return df


def apply(df, case_id="case:concept:name", activity_key="concept:name", timestamp_key="time:timestamp"):
	df = prefix_columns(df)
	df[Constants.TARGET_ACTIVITY] = df[activity_key].astype("category")
	df[Constants.TARGET_ACTIVITY_CODE] = df[Constants.TARGET_ACTIVITY].cat.codes
	df[Constants.TARGET_TIMESTAMP] = df[timestamp_key].astype("int") // 10**6
	df[Constants.TARGET_TIMESTAMP + "_2"] = df[Constants.TARGET_TIMESTAMP]
	df[Constants.TARGET_EV_IDX] = df.index.astype("int")
	df = df.sort_values([Constants.TARGET_TIMESTAMP, Constants.TARGET_EV_IDX]).reset_index()
	df[Constants.TARGET_CASE_IDX] = df[case_id].astype("category").cat.codes
	#df = df.sort_values([Constants.TARGET_CASE_IDX, Constants.TARGET_TIMESTAMP, Constants.TARGET_EV_IDX]).reset_index()
	df[Constants.TARGET_EV_IDX] = df.index.astype("int")
	df[Constants.TARGET_EV_IDX] = df[Constants.TARGET_EV_IDX] + 1
	mult_fact = df[Constants.TARGET_EV_IDX].max() + 2
	df[Constants.TARGET_EV_CASE_MULT_ID] = df[Constants.TARGET_CASE_IDX].astype(np.int32) + 1
	df[Constants.TARGET_EV_CASE_MULT_ID] = mult_fact * df[Constants.TARGET_EV_CASE_MULT_ID]
	df[Constants.TARGET_EV_CASE_MULT_ID] = df[Constants.TARGET_EV_CASE_MULT_ID] + df[Constants.TARGET_EV_IDX]
	return post_filtering(df)
