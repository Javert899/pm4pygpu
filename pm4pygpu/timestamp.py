from pm4pygpu.cases_df import build_cases_df
from pm4pygpu.constants import Constants

def filter_events(df, min_timest=0, max_timest=100000000000):
	df = df.query(Constants.TARGET_TIMESTAMP+" >= "+str(min_timest)+" and "+Constants.TARGET_TIMESTAMP + " <= "+str(max_timest))
	return df

def filter_cases_contained(df, min_timest=0, max_timest=100000000000):
	cdf = build_cases_df(df)
	cdf = cdf.query(Constants.TARGET_TIMESTAMP+" >= "+str(min_timest)+" and "+Constants.TARGET_TIMESTAMP + "_2 <= "+str(max_timest))[Constants.TARGET_CASE_IDX]
	return df[df[Constants.TARGET_CASE_IDX].isin(cdf)]

def filter_cases_intersecting(df, min_timest=0, max_timest=100000000000):
	cdf = build_cases_df(df)
	query_parts = []
	query_parts.append("(" + Constants.TARGET_TIMESTAMP + " >= " + str(min_timest)+" and " + Constants.TARGET_TIMESTAMP + " <= " + str(max_timest) + ")")
	query_parts.append("(" + Constants.TARGET_TIMESTAMP + "_2 >= " + str(min_timest)+" and " + Constants.TARGET_TIMESTAMP + "_2 <= " + str(max_timest) + ")")
	query_parts.append("(" + Constants.TARGET_TIMESTAMP + " <= " + str(min_timest)+" and " + Constants.TARGET_TIMESTAMP + "_2 >= " + str(max_timest) + ")")
	query = " or ".join(query_parts)
	cdf = cdf.query(query)[Constants.TARGET_CASE_IDX]
	return df[df[Constants.TARGET_CASE_IDX].isin(cdf)]
