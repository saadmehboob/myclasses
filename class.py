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
            self.df = self.df[~self.df["BrandId"].isin(["MNG", "DCT"])]

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

import pandas as pd

class SL_processor:
    def __init__(self, df, inditex_only=False, *months):
        pd.set_option('display.float_format', '{:,.2f}'.format)
        self.df = df
        self.inditex_only = inditex_only
        self.months_filter = list(months) if months else None
        self.process_df()

    def process_df(self):
        self.df.drop(['Opening Cost', 'Total Movement Cost', 'Closing Cost'], axis=1, inplace=True, errors='ignore')
        self.df['Transaction Date'] = pd.to_datetime(self.df['Transaction Date'], errors='coerce')
        self.df['month'] = self.df['Transaction Date'].dt.strftime('%b-%y')
        self.df['Store ID'] = self.df['Store ID'].astype(str)

        if self.inditex_only:
            self.df = self.df[~self.df["Brand ID"].isin(['MNG', 'DCT'])]

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

