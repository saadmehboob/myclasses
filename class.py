import pandas as pd
class DL_GL_processor:
    def __init__(self, df, inditex_only=False, *months):
        pd.set_option('display.float_format', '{:,.2f}'.format)
        self.df = df
        self.inditex_only = inditex_only
        self.months_filter = list(months) if months else None
        self.process_df()

    def process_df(self):
        self.df = self.df.fillna(0)
        self.df['net'] = self.df['Converted_Debit_Amount'] - self.df['Converted_Credit_Amount']

        self.df['Movement_Reference_by_type'] = self.df.apply(
            lambda row: row['type'] if row['Movement_Reference'] == "not_present-not_present-not_present"
            else row['Movement_Reference'],
            axis=1
        )

        if self.inditex_only:
            self.df = self.df[~self.df["BrandId"].isin(["MNG", "DCT", "Mango",'Decathlon'])]

        self.df['Effective_Date_Of_Transaction'] = pd.to_datetime(
            self.df['Effective_Date_Of_Transaction'], errors='coerce'
        )
        self.df['month'] = self.df['Effective_Date_Of_Transaction'].dt.strftime('%b-%y')

        if self.months_filter:
            self.df = self.df[self.df['month'].isin(self.months_filter)]

    def GL_summary(self):
        return self.df[['Segment_5', 'net']].groupby(by='Segment_5').sum().reset_index()

    def Mov_summary(self):
        summary = self.df[
            self.df['Segment_5'].astype(str).str.startswith("23")
        ][['Movement_Reference_by_type', 'net']].groupby(by='Movement_Reference_by_type').sum().reset_index()

        summary = summary[summary['net'] != 0]
        summary['Movement_Reference_by_type'] = summary['Movement_Reference_by_type'].astype(str).replace(
            "not_present", "0", regex=True
        )
        return summary.groupby(by='Movement_Reference_by_type')['net'].sum().reset_index()

    def Brand_summary(self):
        summary = self.df[
            self.df['Segment_5'].astype(str).str.startswith("23")
        ][['BrandId', 'net']].groupby(by='BrandId').sum().reset_index()
        return summary

    def Mov_summary_by_month(self):
        df_filtered = self.df[self.df['Segment_5'].astype(str).str.startswith("23")]
        summary = df_filtered.groupby(['month', 'Movement_Reference_by_type'])['net'].sum().reset_index()
        summary = summary[summary['net'] != 0]
        summary['Movement_Reference_by_type'] = summary['Movement_Reference_by_type'].astype(str).replace(
            "not_present", "0", regex=True
        )
        return summary

    def Brand_summary_by_month(self):
        df_filtered = self.df[self.df['Segment_5'].astype(str).str.startswith("23")]
        summary = df_filtered.groupby(['month', 'BrandId'])['net'].sum().reset_index()
        summary = summary[summary['net'] != 0]
        return summary

    def Mov_summary_by_month_by_brand(self):
        df_filtered = self.df[self.df['Segment_5'].astype(str).str.startswith("23")]
        summary = df_filtered.groupby(['month', 'BrandId', 'Movement_Reference_by_type'])['net'].sum().reset_index()
        summary = summary[summary['net'] != 0]
        summary['Movement_Reference_by_type'] = summary['Movement_Reference_by_type'].astype(str).replace(
            "not_present", "0", regex=True
        )
        return summary
    def GL_summary_by_month(self):
        df_filtered = self.df
        summary = df_filtered.groupby(['month', 'Segment_5'])['net'].sum().reset_index()
        summary = summary[summary['net'] != 0]
        return summary

import pandas as pd

class SL_processor:
    def __init__(self, df, inditex_only=False, *months):
        pd.set_option('display.float_format', '{:,.2f}'.format)
        self.df = df
        self.inditex_only = inditex_only
        self.months_filter = list(months) if months else None
        
        self.brand_map = {
            'Bershka': 'BRK',
            'Decathlon': 'DCT',
            'Lefties': 'LEF',
            'Mango': 'MNG',
            'Massimo Dutti': 'MSD',
            'Oysho': 'OSH',
            'Pull And Bear': 'PNB',
            'Stradivarius': 'STR',
            'Zara': 'ZAR',
            'Zara Home': 'ZAH'
        }
    def process_df(self):
        self.df = self.df.iloc[:-1,:]
        self.df.rename(columns={"Brand Name":"Brand ID"},inplace=True,errors='ignore')
        self.df['Brand ID'] = self.df['Brand ID'].replace(self.brand_map)
        self.df.drop(['Opening Cost', 'Total Movement Cost', 'Closing Cost'], axis=1, inplace=True, errors='ignore')
        self.df['Transaction Date'] = pd.to_datetime(self.df['Transaction Date'], errors='coerce')
        self.df['month'] = self.df['Transaction Date'].dt.strftime('%b-%y')
        self.df['Store ID'] = self.df['Store ID'].astype(str)

        if self.inditex_only:
            self.df = self.df[~self.df["Brand ID"].isin(['MNG', 'DCT',"Mango",'Decathlon'])]

        if self.months_filter:
            self.df = self.df[self.df['month'].isin(self.months_filter)]

    def Mov_summary(self):
        numeric_cols = self.df.select_dtypes(include='number').columns.difference(['Inventory'])
        melted = self.df.melt(value_vars=numeric_cols, var_name='Movement_Reference_by_type', value_name='cost')
        melted = melted[melted['cost'] != 0]
        melted['Movement_Reference_by_type'] = melted['Movement_Reference_by_type'].str.replace(
            "not_applicable|MOV_", "0", regex=True
        ).str.replace("_", "-", regex=True)
        summary = melted.groupby('Movement_Reference_by_type')['cost'].sum().reset_index()
        return summary

    def Brand_summary(self):
        numeric_cols = self.df.select_dtypes(include='number').columns.difference(['Inventory'])
        grouped = self.df.groupby('Brand ID')[numeric_cols].sum()
        grouped['Total Movement'] = grouped.sum(axis=1)
        return grouped[['Total Movement']].reset_index()

    def Mov_summary_by_month(self):
        numeric_cols = self.df.select_dtypes(include='number').columns.difference(['Inventory'])
        melted = self.df.melt(id_vars='month', value_vars=numeric_cols, var_name='Movement_Reference_by_type', value_name='cost')
        melted = melted[melted['cost'] != 0]
        melted['Movement_Reference_by_type'] = melted['Movement_Reference_by_type'].str.replace(
            "not_applicable|MOV_", "0", regex=True
        ).str.replace("_", "-", regex=True)
        summary = melted.groupby(['month', 'Movement_Reference_by_type'])['cost'].sum().reset_index()
        return summary

    def Brand_summary_by_month(self):
        numeric_cols = self.df.select_dtypes(include='number').columns.difference(['Inventory'])
        melted = self.df.melt(id_vars=['month', 'Brand ID'], value_vars=numeric_cols, var_name='Movement_Reference', value_name='cost')
        summary = melted.groupby(['month', 'Brand ID'])['cost'].sum().reset_index()
        summary = summary[summary['cost'] != 0]
        return summary

    def Mov_summary_by_month_by_brand(self):
        numeric_cols = self.df.select_dtypes(include='number').columns.difference(['Inventory'])
        melted = self.df.melt(id_vars=['month', 'Brand ID'], value_vars=numeric_cols, var_name='Movement_Reference_by_type', value_name='cost')
        melted = melted[melted['cost'] != 0]
        melted['Movement_Reference_by_type'] = melted['Movement_Reference_by_type'].str.replace(
            "not_applicable|MOV_", "0", regex=True
        ).str.replace("_", "-", regex=True)
        summary = melted.groupby(['month', 'Brand ID', 'Movement_Reference_by_type'])['cost'].sum().reset_index()
        return summary

class System_GL_processor:
    def __init__(self, df, inditex_only=False, *months):
        pd.set_option("display.float_format", "{:,.2f}".format)
        self.df = df.copy()
        self.inditex_only = inditex_only
        self.months_filter = list(months) if months else None
        self.process_df()

    def extract_segment(self, account, index):
        try:
            return account.split("-")[index]
        except Exception:
            return None

    def generate_columns(self, df):
        # Extract Trancode, Storecode, and Movement
        df[['Trancode', 'Storecode', 'Movement']] = df['JRNL_LINE_DESC'].str.extract(r'(\d+)_([\d]+)_(.*)')
        df['Movement'] = df['Movement'].str.replace('not_present', '0', regex=False)
        #df['Movement'] = df['Movement'].str.replace('-', '0-', regex=False)
        df['Movement_Reference_by_type'] = df['Movement']
        return df

    def process_df(self):
        self.df = self.df.fillna(0)

        # Compute net value
        self.df["net"] = pd.to_numeric(self.df["ACCOUNTED_DR"], errors="coerce") - pd.to_numeric(self.df["ACCOUNTED_CR"], errors="coerce")

        # Extract segments from ACCOUNT
        self.df["BrandId"] = self.df["ACCOUNT"].apply(lambda x: self.extract_segment(x, 2))
        self.df["Segment_5"] = self.df["ACCOUNT"].apply(lambda x: self.extract_segment(x, 4))

        # Parse JRNL_LINE_DESC
        self.df = self.generate_columns(self.df)

        # Extract month from date
        self.df["ACCOUNTING_DATE"] = pd.to_datetime(self.df["ACCOUNTING_DATE"], errors="coerce")
        self.df["month"] = self.df["ACCOUNTING_DATE"].dt.strftime("%b-%y")

        # Filter by brand if requested
        if self.inditex_only:
            self.df = self.df[~self.df["BrandId"].isin(["MNG", "DCT","Mango",'Decathlon'])]

        # Filter by specified months if provided
        if self.months_filter:
            self.df = self.df[self.df["month"].isin(self.months_filter)]

    # --- Summaries ---
    def GL_summary(self):
        return self.df[["Segment_5", "net"]].groupby("Segment_5").sum().reset_index()

    def Mov_summary(self):
        df_filtered = self.df[self.df["Segment_5"].astype(str).str.startswith("23")]
        summary = df_filtered.groupby("Movement_Reference_by_type")["net"].sum().reset_index()
        return summary[summary["net"] != 0]

    def Brand_summary(self):
        df_filtered = self.df[self.df["Segment_5"].astype(str).str.startswith("23")]
        return df_filtered.groupby("BrandId")["net"].sum().reset_index()

    def Mov_summary_by_month(self):
        df_filtered = self.df[self.df["Segment_5"].astype(str).str.startswith("23")]
        summary = df_filtered.groupby(["month", "Movement_Reference_by_type"])["net"].sum().reset_index()
        return summary[summary["net"] != 0]

    def Brand_summary_by_month(self):
        df_filtered = self.df[self.df["Segment_5"].astype(str).str.startswith("23")]
        summary = df_filtered.groupby(["month", "BrandId"])["net"].sum().reset_index()
        return summary[summary["net"] != 0]

    def Mov_summary_by_month_by_brand(self):
        df_filtered = self.df[self.df["Segment_5"].astype(str).str.startswith("23")]
        summary = df_filtered.groupby(["month", "BrandId", "Movement_Reference_by_type"])["net"].sum().reset_index()
        return summary[summary["net"] != 0]
    def GL_summary_by_month(self):
        #df_filtered = self.df
        df_filtered = self.df
        summary = df_filtered.groupby(["month", "Segment_5"])["net"].sum().reset_index()
        return summary[summary["net"] != 0]


