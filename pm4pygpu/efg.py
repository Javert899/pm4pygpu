from pm4pygpu.constants import Constants

def calculate_efg(df):
	df = df[[Constants.TARGET_CASE_IDX, Constants.TARGET_ACTIVITY, Constants.TARGET_EV_IDX, Constants.TARGET_TIMESTAMP]]
	merged_df = df.merge(df, on=[Constants.TARGET_CASE_IDX], how="left", suffixes=('', '_y'))
	merged_df = merged_df.query(Constants.TARGET_EV_IDX + " < " + Constants.TARGET_EV_IDX + "_y")
	merged_df[Constants.TIMESTAMP_JOIN_DIFF] = merged_df[Constants.TARGET_TIMESTAMP + "_y"] - merged_df[Constants.TARGET_TIMESTAMP]
	merged_df[Constants.TIMESTAMP_JOIN_DIFF + "_2"] = merged_df[Constants.TIMESTAMP_JOIN_DIFF]
	return merged_df

def calculate_temporal_profile(df):
	merged_df = calculate_efg(df)
	tf = merged_df.groupby([Constants.TARGET_ACTIVITY, Constants.TARGET_ACTIVITY + "_y"]).agg({Constants.TARGET_EV_IDX: "count", Constants.TIMESTAMP_JOIN_DIFF: "mean", Constants.TIMESTAMP_JOIN_DIFF + "_2": "mean"}).reset_index()
	return merged_df, tf

def conformance_temporal_profile(df, sigma=6):
	merged_df, tf = calculate_temporal_profile(df)
	tf[Constants.TIMESTAMP_JOIN_DIFF + "_2"] = sigma * tf[Constants.TIMESTAMP_JOIN_DIFF + "_2"]
	tf[Constants.TIMESTAMP_MIN_ALLOWED_COUPLE] = tf[Constants.TIMESTAMP_JOIN_DIFF] - tf[Constants.TIMESTAMP_JOIN_DIFF + "_2"]
	tf[Constants.TIMESTAMP_MAX_ALLOWED_COUPLE] = tf[Constants.TIMESTAMP_JOIN_DIFF] + tf[Constants.TIMESTAMP_JOIN_DIFF + "_2"]
	tf = tf[[Constants.TARGET_ACTIVITY, Constants.TARGET_ACTIVITY + "_y", Constants.TIMESTAMP_MIN_ALLOWED_COUPLE, Constants.TIMESTAMP_MAX_ALLOWED_COUPLE]]
	merged_df = merged_df.merge(tf, on=[Constants.TARGET_ACTIVITY, Constants.TARGET_ACTIVITY + "_y"], how="left", suffixes=('', '_z'))
	return merged_df
