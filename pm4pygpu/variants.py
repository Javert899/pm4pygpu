from pm4pygpu.constants import Constants
from pm4pygpu.start_end_activities import get_end_activities
import sys

def get_variants_df(df):
	return df.groupby(Constants.TARGET_CASE_IDX).agg({Constants.TARGET_ACTIVITY_CODE: "sum", Constants.TARGET_VARIANT_NUMBER: "sum", Constants.TARGET_EV_IDX: "count"}).reset_index()

def filter_on_variants(df, allowed_variants):
	activities = df[Constants.TARGET_ACTIVITY].cat.categories.to_arrow().to_pylist()
	activities = {activities[i]: i for i in range(len(activities))}
	list_tup_vars = []
	for varstri in allowed_variants:
		var = varstri.split(Constants.VARIANTS_SEP)
		v1 = 0
		v2 = 0
		for i in range(len(var)):
			v1 += activities[var[i]]
			v2 += (len(var) + i + 1) * (activities[var[i]] + 1)
		list_tup_vars.append("(" + Constants.TARGET_ACTIVITY_CODE + " == "+str(v1)+" and " +Constants.TARGET_VARIANT_NUMBER+" == "+str(v2) + ")")
	this_query = " or ".join(list_tup_vars)
	cdf = get_variants_df(df)
	cdf = cdf.query(this_query)[Constants.TARGET_CASE_IDX]
	return df[df[Constants.TARGET_CASE_IDX].isin(cdf)]

def get_variants(df, max_des_vars_num=sys.maxsize, return_list=False):
	activities = df[Constants.TARGET_ACTIVITY].cat.categories.to_arrow().to_pylist()
	activities = {activities[i]: i for i in range(len(activities))}
	cdf = get_variants_df(df)
	vars_count = cdf.groupby([Constants.TARGET_ACTIVITY_CODE, Constants.TARGET_VARIANT_NUMBER]).count()[Constants.TARGET_CASE_IDX].to_pandas().to_dict()
	cdf = cdf.groupby([Constants.TARGET_ACTIVITY_CODE, Constants.TARGET_VARIANT_NUMBER]).agg({Constants.TARGET_CASE_IDX: "min", Constants.TARGET_EV_IDX: "count"})
	if max_des_vars_num < sys.maxsize:
		nlarg = min(cdf[Constants.TARGET_EV_IDX].nlargest(max_des_vars_num).to_pandas().to_dict().values())
		cdf = cdf.query(Constants.TARGET_EV_IDX + " >= " + str(nlarg))
	vars0 = df[df[Constants.TARGET_CASE_IDX].isin(cdf[Constants.TARGET_CASE_IDX])][[Constants.TARGET_CASE_IDX, Constants.TARGET_ACTIVITY]].to_arrow().to_pydict()
	cases = {}
	for i in range(len(vars0[Constants.TARGET_ACTIVITY])):
		case = vars0[Constants.TARGET_CASE_IDX][i]
		act = vars0[Constants.TARGET_ACTIVITY][i]
		if not case in cases:
			cases[case] = []
		cases[case].append(act)
	ret = {}
	for case in cases:
		v1 = 0
		v2 = 0
		cases_act = cases[case]
		for i in range(len(cases_act)):
			v1 += activities[cases_act[i]]
			v2 += (len(cases_act) + i + 1) * (activities[cases_act[i]] + 1)
		ret[",".join(cases_act)] = int(vars_count[(v1, v2)])
	if return_list:
		ret = [{"variant": x, "count": y} for x, y in ret.items()]
	return ret
