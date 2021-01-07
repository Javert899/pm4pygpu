from pm4pygpu.constants import Constants

def get_attribute_values(df, attribute=Constants.TARGET_ACTIVITY):
	att_dict = df[attribute].value_counts().to_pandas().to_dict()
	for att in att_dict:
		att_dict[att] = int(att_dict[att])
	return att_dict

def attribute_filter_cases(df, list_act, attribute=Constants.TARGET_ACTIVITY):
	filt_df = df[df[attribute].isin(list_act)]
	cases = filt_df[Constants.TARGET_CASE_IDX].unique()
	return df[df[Constants.TARGET_CASE_IDX].isin(cases)]

def attribute_filter_events(df, list_act, attribute=Constants.TARGET_ACTIVITY):
	return df[df[attribute].isin(list_act)]
