from pm4pygpu.constants import Constants
from pm4pygpu.variants import get_variants_df

def num_events(df):
	return int(len(df))

def num_cases(df):
	return int(df[Constants.TARGET_CASE_IDX].nunique())

def num_variants(df):
	var_df = get_variants_df(df)
	var_df = var_df.groupby([Constants.TARGET_ACTIVITY_CODE, Constants.TARGET_VARIANT_NUMBER]).count()
	return int(len(var_df))

def get_events_of_case(df, case_id):
	df2 = df.query(Constants.TARGET_CASE_IDX + " == " + str(case_id))
	ret = df2.to_pandas().to_dict("r")
	return ret
