from pm4pygpu.constants import Constants
from pm4pygpu.cases_df import get_first_df, get_last_df

def select_number_column(df, fea_df, col):
	df = get_last_df(df.dropna(subset=[col]))[[Constants.TARGET_CASE_IDX, col]]
	fea_df = fea_df.merge(df, on=[Constants.TARGET_CASE_IDX], how="left", suffixes=('','_y'))
	return fea_df

def select_string_column(df, fea_df, col):
	vals = df[col].unique().to_arrow().to_pylist()
	for val in vals:
		if val is not None:
			filt_df_cases = df[df[col].isin([val])][Constants.TARGET_CASE_IDX].unique()
			new_col = col + "_" + val.encode('ascii',errors='ignore').decode('ascii').replace(" ","")
			fea_df[new_col] = fea_df[Constants.TARGET_CASE_IDX].isin(filt_df_cases)
			fea_df[new_col] = fea_df[new_col].astype("int")
	return fea_df

def get_features_df(df, list_columns):
	fea_df = df[Constants.TARGET_CASE_IDX].unique().to_frame()
	for col in list_columns:
		if "object" in str(df[col].dtype):
			fea_df = select_string_column(df, fea_df, col)
		elif "float" in str(df[col].dtype) or "int" in str(df[col].dtype):
			fea_df = select_number_column(df, fea_df, col)
	fea_df = fea_df.sort_values(Constants.TARGET_CASE_IDX)
	return fea_df

def select_features(df, low_b_str=5, up_b_str=50):
	list_columns = []
	df_cases = df[Constants.TARGET_CASE_IDX].nunique()
	for col in df.columns:
		if not col.startswith("custom_") and not col.startswith("index"):
			if "object" in str(df[col].dtype):
				nuniq = df[col].nunique()
				if low_b_str <= nuniq <= up_b_str:
					list_columns.append(col)
			elif "float" in str(df[col].dtype) or "int" in str(df[col].dtype):
				filt_df_cases = df.dropna(subset=[col])[Constants.TARGET_CASE_IDX].nunique()
				if df_cases == filt_df_cases:
					list_columns.append(col)
	return list_columns

def get_automatic_features_df(df, low_b_str=5, up_b_str=50):
	list_columns = select_features(df, low_b_str=low_b_str, up_b_str=up_b_str)
	return get_features_df(df, list_columns)
