from pm4pygpu.constants import Constants
import sys

def get_attributes_list(df):
	attributes = list(df.columns)
	attributes = [x for x in attributes if not x.startswith(Constants.CUSTOM_PREFIX) and not x.startswith("@@")]
	return attributes

def get_attribute_values(df, attribute=Constants.TARGET_ACTIVITY):
	att_dict = df[attribute].value_counts().to_pandas().to_dict()
	for att in att_dict:
		att_dict[att] = int(att_dict[att])
	return att_dict

def attribute_filter_cases(df, list_act, attribute=Constants.TARGET_ACTIVITY, retain=True):
	filt_df = df[df[attribute].isin(list_act)]
	cases = filt_df[Constants.TARGET_CASE_IDX].unique()
	if retain:
		return df[df[Constants.TARGET_CASE_IDX].isin(cases)]
	else:
		return df[~df[Constants.TARGET_CASE_IDX].isin(cases)]

def attribute_filter_events(df, list_act, attribute=Constants.TARGET_ACTIVITY, retain=True):
	if retain:
		return df[df[attribute].isin(list_act)]
	else:
		return df[~df[attribute].isin(list_act)]

def numeric_attribute_filter_cases(df, attribute, val_inf, val_sup):
	filt_df = df.query(str(val_inf) + " <= " + attribute + " <= " + str(val_sup))[Constants.TARGET_CASE_IDX]
	return df[df[Constants.TARGET_CASE_IDX].isin(filt_df)]

def numeric_attribute_filter_events(df, attribute, val_inf, val_sup):
	filt_df = df.query(str(val_inf) + " <= " + attribute + " <= " + str(val_sup))
	return filt_df

def numeric_attribute_values(df, attribute, n_values=sys.maxsize):
	serie = df[attribute].dropna().sort_values()
	lenn = len(serie)
	if n_values < lenn:
		serie = serie.sample(n_values)
	ret = serie.to_arrow().to_pylist()
	for i in range(len(ret)):
		ret[i] = float(ret[i])
	return ret
