from pm4pygpu.constants import Constants
from pm4pygpu.cases_df import get_first_df, get_last_df


def get_start_activities(df):
	first_df = get_first_df(df)[Constants.TARGET_ACTIVITY]
	sa_dict = first_df.value_counts().to_pandas().to_dict()
	for el in sa_dict:
		sa_dict[el] = int(sa_dict[el])
	return sa_dict

def get_end_activities(df):
	last_df = get_last_df(df)[Constants.TARGET_ACTIVITY]
	ea_dict = last_df.value_counts().to_pandas().to_dict()
	for el in ea_dict:
		ea_dict[el] = int(ea_dict[el])
	return ea_dict

def filter_start_activities(df, list_act, retain=True):
	first_df = get_first_df(df)
	first_df = first_df[first_df[Constants.TARGET_ACTIVITY].isin(list_act)]
	cases = first_df[Constants.TARGET_CASE_IDX].unique()
	if retain:
		return df[df[Constants.TARGET_CASE_IDX].isin(cases)]
	else:
		return df[~df[Constants.TARGET_CASE_IDX].isin(cases)]

def filter_end_activities(df, list_act, retain=True):
	last_df = get_last_df(df)
	last_df = last_df[last_df[Constants.TARGET_ACTIVITY].isin(list_act)]
	cases = last_df[Constants.TARGET_CASE_IDX].unique()
	if retain:
		return df[df[Constants.TARGET_CASE_IDX].isin(cases)]
	else:
		return df[~df[Constants.TARGET_CASE_IDX].isin(cases)]
