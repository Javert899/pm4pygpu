from pm4pygpu.constants import Constants

def get_frequency_dfg(df):
	activities = df[Constants.TARGET_ACTIVITY].cat.categories.to_arrow().to_pylist()
	activities = {i: activities[i] for i in range(len(activities))}
	dfg = df.query(Constants.TARGET_CASE_IDX+" == "+Constants.TARGET_PRE_CASE).groupby([Constants.TARGET_PRE_ACTIVITY, Constants.TARGET_ACTIVITY_CODE]).count()[Constants.TARGET_VARIANT_NUMBER].to_pandas().to_dict()
	dfg = {(activities[x[0]], activities[x[1]]): int(y) for x, y in dfg.items()}
	return dfg

def get_performance_dfg(df):
	activities = df[Constants.TARGET_ACTIVITY].cat.categories.to_arrow().to_pylist()
	activities = {i: activities[i] for i in range(len(activities))}
	dfg_perf = df.query(Constants.TARGET_CASE_IDX+" == "+Constants.TARGET_PRE_CASE).groupby([Constants.TARGET_PRE_ACTIVITY, Constants.TARGET_ACTIVITY_CODE]).agg({Constants.TIMESTAMP_DIFF: "mean"})[Constants.TIMESTAMP_DIFF].to_pandas().to_dict()
	dfg_perf = {(activities[x[0]], activities[x[1]]): float(y) for x, y in dfg_perf.items()}
	return dfg_perf
