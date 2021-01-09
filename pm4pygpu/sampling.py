from pm4pygpu.constants import Constants
from pm4pygpu.cases_df import get_first_df

def sample_events(df, n):
	lenn = len(df)
	ss = min(lenn, n)
	return df.sample(ss)

def sample_cases(df, n):
	cdf = get_first_df(df)
	lenn = len(cdf)
	ss = min(lenn, n)
	sample = cdf.sample(ss)[Constants.TARGET_CASE_IDX].to_arrow()
	return df[df[Constants.TARGET_CASE_IDX].isin(sample)]
