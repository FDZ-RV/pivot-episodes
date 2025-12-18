#######################################################################################################################
# Imports
#######################################################################################################################
import pandas as pd
import polars as pl
import time
from pathlib import Path

from frg import add_frg_pl_pure # function to create frg variables
# load state-conditions and order of states
from state_rules import state_order_new as state_order, conditions_new as conditions

#######################################################################################################################
# Global settings
#######################################################################################################################


#######################################################################################################################
# Loading and preprocessing:
#######################################################################################################################

# load factors for ZREG calculations
anlage10_df = pd.read_stata(r"D:\projects\soep_rv\VSKT\help\Anlage_10.dta")
anlage10_df = pl.from_pandas(anlage10_df)
anlage10_df = anlage10_df.select(
    pl.col("Jahr").cast(pl.Int64).alias("JAHR"),
    pl.col("Monat").cast(pl.Int64).alias("MONAT"),
    pl.col("ANLAGE_10")
)

def load_and_preprocess(working_folder: str, dataset: str, berichtsjahr: int) -> pl.DataFrame:
    ''' Adds variables "Ses_frg" and "Beruf_frg", converts "VZNR" and "BSZR" to datetime
    :param working_folder: folder to save temporary preprocessed df as parquet
    :param dataset: VVL or VSKT
    :param berichtsjahr: year
    :return: preprocessed df
    '''
    print("Loading data...")
    data_path = Path(fr"D:\rohdaten_fdz\{dataset}\OSV.{dataset}.{berichtsjahr}.VAR.dta")
    working_folder = Path(working_folder)
    file_path = working_folder / fr"OSV.{dataset}.{berichtsjahr}.VAR.preprocessed.parquet"
    
    if file_path.exists():
        print("File already preprocessed, loading preprocessed version ...")
        df = pd.read_parquet(file_path)
        df = pl.from_pandas(df)
    else:
        df = pd.read_stata(data_path, convert_categoricals=False)
        df = pl.from_pandas(df)

        # add Ses_frg and Beruf_frg variables (for status "FRG")
        print("Preprocessing data...")
        df = add_frg_pl_pure(df)

        # transform time variables into datetime objects
        df = df.with_columns([
            pl.col("VNZR").cast(pl.Utf8).str.strptime(pl.Date,"%Y%m%d").alias("VNZR"),
            pl.col("BSZR").cast(pl.Utf8).str.strptime(pl.Date,"%Y%m%d").alias("BSZR"),
        ])
		
        # optional: drop "PSY" col
        # df = df.drop("PSY")
        
		# save df in case further processing fails
        df.write_parquet(file_path)
        print("Done!")

    return df

#######################################################################################################################
# main functions:
#######################################################################################################################

def make_state_expr(conditions: dict[str, pl.Expr]) -> pl.Expr:
    ''' Creates one big polar expression from conditions, used to filter the df by states.
    :param conditions: dict of (state, expression)
    :return: polars expression
    '''
    expr = None
    for state, cond in conditions.items():
        if expr is None:
            expr = pl.when(cond).then(pl.lit(state))
        else:
            expr = expr.when(cond).then(pl.lit(state))
			
    return expr.otherwise(pl.lit("OTHER"))


def pivot_episodes(df: pl.DataFrame) -> pl.DataFrame:
    ''' Turns each row (=episode) into a monthly timeline running from VNZR to BSZR.
    :param df: preprocessed pl.DataFrame in episode format with 'Ses_frg' column.
    :return: pl.DataFrame in timeline format with cols for state/days/egpt/zreg per month
    '''

    # add state column
    df = df.with_columns(
        state=make_state_expr(conditions)
    ).drop([
        'LFNR', 'VSGR', 'BYAT', 'BYATSO', 'KI', 'GM', 'RTVS', 'FIZTVAR', 'KZSO', 'RCEG', 'BHBR',
        'QLGR', 'RESV2', 'ZRMO', 'EGPTAN', 'INJA', 'RESV', 'SES', 'Ses_frg', 'Beruf_frg'
    ])

    # create a timeline of steps (year, month) for each episode, count days per month
    df = df.with_columns(
        start_month = pl.col("VNZR").dt.truncate("1mo"),
        end_month = pl.col("BSZR").dt.truncate("1mo")
    ).with_columns(
        month = pl.date_ranges(
            start=pl.col("start_month"),
            end=pl.col("end_month"),
            interval="1mo",
            closed="both"
        )
    )
    df_monthly = df.explode("month")
    df_monthly = df_monthly.with_columns(
        month_start = pl.col("month"),
        month_end = pl.col("month").dt.offset_by("1mo") - pl.duration(days=1)
    )
    df_monthly = df_monthly.with_columns(
    overlap_start = pl.max_horizontal("VNZR","month_start"),
        overlap_end = pl.min_horizontal("BSZR","month_end")
   ).with_columns(tage = (pl.col("overlap_end")-pl.col("overlap_start")).dt.total_days()+1)


    # calculate ZREG und EGPT per day, then per month as columns
    df_monthly = df_monthly.with_columns(
            egpt = (pl.col("tage") * pl.col("EGPT") / (
                    (pl.col("BSZR")-pl.col("VNZR")).dt.total_days()+1)).cast(pl.Float32),
            zreg = (pl.col("tage") * pl.col("ZREG") / (
                    (pl.col("BSZR") - pl.col("VNZR")).dt.total_days() + 1)).cast(pl.Float32)
    )

    # extract JAHR and MONAT and add as columns
    df_monthly = df_monthly.with_columns(
        JAHR = pl.col("month").dt.year(),
        MONAT = pl.col("month").dt.month(),
        TAGE = ((pl.col("month_end")-pl.col("month_start")).dt.total_days()+1).cast(pl.Int8)
    ).drop([
        "month", 'VNZR', 'BSZR', 'ZREG', 'EGPT', 'start_month', 'end_month', 'month_start',
        'month_end', 'overlap_start', 'overlap_end'
    ])

    # deal with duplicates of (FDZ_ID, JAHR, MONAT)
    df_monthly = df_monthly.group_by(["FDZ_ID", "JAHR", "MONAT", "TAGE", "state"]).agg(
        [
            pl.sum("tage"),
            pl.sum("egpt"),
            pl.sum("zreg")
        ]
    )
    # reset nr of days if sum of tage is too large
    df_monthly = df_monthly.with_columns(pl.min_horizontal("tage", "TAGE").cast(pl.Int8).alias("tage"))

    # (re-)calculate daily EGPT and ZREG values (now per month, not per episode)
    df_monthly = df_monthly.with_columns(
            (pl.col("egpt") / pl.col("tage")).alias("egpt_daily"),
            (pl.col("zreg") / pl.col("tage")).alias("zreg_daily")
    )

    # adjust ZREG values using values from anlage10_df
    df_monthly = df_monthly.join(anlage10_df.cast({"JAHR": pl.Int32, "MONAT": pl.Int8}), on=["JAHR", "MONAT"])
    df_monthly = df_monthly.with_columns(
        pl.when(pl.col("state").is_in({"OSB", "OKN", "FRG OSB","OSS", "ATZ OSB", "ATZ OKN"}))
                .then(pl.col("zreg") / pl.col("ANLAGE_10")).otherwise("zreg").alias("zreg"),
                pl.when(pl.col("state").is_in({"OSB", "OKN", "FRG OSB", "OSS", "ATZ OSB", "ATZ OKN"}))
                        .then(pl.col("zreg_daily") / pl.col("ANLAGE_10")).otherwise("zreg_daily").alias("zreg_daily")
    ).drop("ANLAGE_10")

    return df_monthly

#######################################################################################################################

def generate_status(df: pl.DataFrame, names_and_vars: list[str]) -> pl.DataFrame:
    ''' Generates timeline format for a single state/status.
    :param df: pl.DataFrame in timeline format, output of pivot_episodes().
    :param state: which state to consider.
    :param name: how the status will be named, ie STATUS_name.
    :vars: list of the form variables to consider, subset of {TAGE, EGPT, ZREG}.
    :return: pl.DataFrame with cols FDZ_ID, JAHR, MONAT, TAGE, name_TAGE, name_EGPT etc.
    '''
	
    standard_cols = ["FDZ_ID", "JAHR", "MONAT", "TAGE"]
    additional_cols = [f"{variable.lower()}" for variable in vars]
    cols_to_keep = standard_cols + additional_cols

    df_state = df.filter(pl.col("state") == state)
    df_state = df_state[cols_to_keep].rename(
        {col: f"{name}_{col.upper()}" for col in additional_cols}
    )

    return df_state


def generate_status_1_and_NJB(df: pl.DataFrame) -> tuple[pl.DataFrame]:
    ''' Generates timeline format for STATUS_1, renames certain states as "NJB state" in df.
    :param df: pl.DataFrame in timeline format, output of pivot_episodes().
    :return: pl.DataFrame for STATUS_1, input df with certain states changed to "NJB state" (needed for STATUS 2 and 3). 
    '''

    # filter for first 12 states (relevant for STATUS_1)
    df_filtered = df.filter(pl.col("state").is_in(list(conditions.keys())[:12]))

    keys = ["FDZ_ID", "JAHR", "MONAT", "TAGE"]
    top3 = (
        df_filtered.group_by(keys).agg([
            pl.col("state").sort_by("zreg_daily", descending=True).head(6).alias("top3_states"),
            pl.col("tage").sort_by("zreg_daily", descending=True).head(6).alias("top3_tage"),
            pl.col("egpt").sort_by("zreg_daily", descending=True).head(6).alias("top3_egpt"),
            pl.col("zreg").sort_by("zreg_daily", descending=True).head(6).alias("top3_zreg"),
            pl.col("egpt_daily").sort_by("zreg_daily", descending=True).head(6).alias("top3_egpt_daily"),
            pl.col("zreg_daily").sort_by("zreg_daily", descending=True).head(6).alias("top3_zreg_daily")
        ])
    )

    # create df for STATUS_1
    df_status1 = top3.with_columns([
        pl.col("top3_states").list.get(0).alias("STATUS_1"),
        pl.col("top3_tage").list.get(0).alias("STATUS_1_TAGE"),
        pl.col("top3_egpt").list.get(0).alias("STATUS_1_EGPT"),
        pl.col("top3_zreg").list.get(0).alias("STATUS_1_ZREG")
    ]).drop(['top3_states', 'top3_tage', 'top3_egpt', 'top3_zreg', "top3_egpt_daily", "top3_zreg_daily"])

    # create rows where state = "NJB {state}" and update their og version in df
    nj_data = []
    for i in range(1, 3):
        df_nj = top3.with_columns(
            (pl.lit("NJB ") + pl.col("top3_states").list.get(i)).alias("state"),
            pl.col("top3_tage").list.get(i).alias("tage"),
            pl.col("top3_egpt").list.get(i).alias("egpt"),
            pl.col("top3_zreg").list.get(i).alias("zreg"),
            pl.col("top3_egpt_daily").list.get(i).alias("egpt_daily"),
            pl.col("top3_zreg_daily").list.get(i).alias("zreg_daily")
        ).drop(['top3_states', 'top3_tage', 'top3_egpt', 'top3_zreg', "top3_egpt_daily", "top3_zreg_daily"
        ]).filter(pl.col("zreg_daily").is_not_null())
        nj_data.append(df_nj)
    df_update = pl.concat(nj_data,how= "vertical")
    df = df.join(df_update,
                 on= ['FDZ_ID', 'JAHR', 'MONAT', 'TAGE', 'tage', 'egpt', 'zreg', 'egpt_daily', 'zreg_daily'],
                 how= "left",
                 suffix="_new"
    ).with_columns(state = pl.coalesce(["state_new","state"])).drop("state_new")

    return df_status1, df


def generate_multiple_ordered_status(df: pl.DataFrame, amount_of_status: int, state_order: list) -> pl.DataFrame:
    '''  Generates timeline format for STATUS_2,3 etc., their number specified by amount_of_status.
    :param df: pl.DataFrame in timeline format with NJB states, output of generate_status_1_and_NJB().
    :param amount_of_status: nr of status to generate.
    :param state_order: list that defines how states are prioritised in case their egpt_daily values are equal.
    :return: pl.DataFrame for STATUS_2, STATUS_3 etc.
    '''

    # turn state_order into a mapping
    order_map = {state: i for i, state in enumerate(state_order)}

    # filter df for relevant states, create a rank of states using state_order
    df_filtered = df.filter(pl.col("state").is_in(state_order)).with_columns(
        pl.col("state").replace(order_map).cast(pl.Int8).alias("rank")
    )

    # rank by egpt_daily, then by state_order
    keys = ["FDZ_ID", "JAHR", "MONAT", "TAGE"]
    top_egpt_daily = df_filtered.group_by(keys).agg([
            pl.col("state").sort_by(
                "egpt_daily", "rank", descending=[True,False]).head(amount_of_status).alias("top_states"),
            pl.col("tage").sort_by(
                "egpt_daily", "rank", descending=[True,False]).head(amount_of_status).alias("top_tage"),
            pl.col("egpt").sort_by(
                "egpt_daily", "rank", descending=[True,False]).head(amount_of_status).alias("top_egpt"),
            pl.col("zreg").sort_by(
                "egpt_daily", "rank", descending=[True,False]).head(amount_of_status).alias("top_zreg"),
            pl.col("egpt_daily").sort_by(
                "egpt_daily", "rank", descending=[True,False]).head(amount_of_status).alias("top_egpt_daily"),
            pl.col("zreg_daily").sort_by(
                "egpt_daily", "rank", descending=[True,False]).head(amount_of_status).alias("top_zreg_daily")
        ])

    # create df with STATUS_2, STATUS_3 etc
    status_df_list = []
    for i in range(2, amount_of_status + 2):
        status_df = top_egpt_daily.with_columns([
            pl.col("top_states").list.get(i - 2).alias(f"STATUS_{i}"),
            pl.col("top_tage").list.get(i - 2).alias(f"STATUS_{i}_TAGE"),
            pl.col("top_egpt").list.get(i - 2).alias(f"STATUS_{i}_EGPT"),
            pl.col("top_zreg").list.get(i - 2).alias(f"STATUS_{i}_ZREG")
        ]).drop(['top_states', 'top_tage', 'top_egpt', 'top_zreg', "top_egpt_daily", "top_zreg_daily"])
        status_df_list.append(status_df)

    df_status_ordered = status_df_list[0]
    for df_status in status_df_list[1:]:
        df_status_ordered = df_status_ordered.join(df_status, on= keys)

    return df_status_ordered

#######################################################################################################################

def merge_into_full_timeline(berichtsjahr: int, df_list: list[pl.DataFrame]) -> pl.DataFrame:
    ''' Merge everything into one big df.
    :param berichtsjahr: maximum year of each timeline.
    :param df_list: list of pl.DataFrames, containing all timelines with STATUS variables. Ie outputs of 
    generate_status(), generate_status_1_and_NJB() and generate_multiple_ordered_status().
    :return: pl.DataFrame with all timelines and all STATUS variables
    '''

    # find min dates per FDZ_ID in each df_status
    min_dates_per_df = []
    for df_status in df_list:
        df_min = (
            df_status.select(
                "FDZ_ID",
                pl.date("JAHR", "MONAT", 1).alias("date"),
            )
            .group_by("FDZ_ID")
            .agg(
                min_date=pl.col("date").min(),
            )
        )
        min_dates_per_df.append(df_min)
    all_mins = pl.concat(min_dates_per_df, how="vertical")

    # find "global" min date for each FDZ_ID
    date_bounds = (
        all_mins
        .group_by("FDZ_ID")
        .agg(
            min_date=pl.col("min_date").min()
        )
    ).with_columns(max_date=pl.date(berichtsjahr, 12, 31))

    # create timeline per FDZ_ID
    date_bounds = date_bounds.with_columns(
        months=pl.date_ranges(
            start=pl.col("min_date").dt.truncate("1mo"),
            end=pl.col("max_date").dt.truncate("1mo"),
            interval="1mo",
            closed="both",
        )
    )
    timeline_per_id = date_bounds.explode("months").with_columns(
            JAHR=pl.col("months").dt.year(),
            MONAT=pl.col("months").dt.month(),
            TAGE=(pl.col("months").dt.offset_by("1mo") - pl.col("months")).dt.total_days().cast(pl.Int8)
        ).select(["FDZ_ID", "JAHR", "MONAT", "TAGE"])

    # join timeline with all of df_status'
    df_final = timeline_per_id
    for df_status in df_list:
        df_final = df_final.join(df_status, on=["FDZ_ID", "JAHR", "MONAT", "TAGE"], how="left")
    df_final.sort(["FDZ_ID", "JAHR", "MONAT"])

    return df_final

#######################################################################################################################
# run (example)
#######################################################################################################################

working_folder = ...

start = time.time()

print("Preprocessing...")
df = load_and_preprocess(working_folder, "VVL", 2023)

print("Pivoting episodes into timeline....")
df = pivot_episodes(df)

print("Extracting Status 1 to 5...")
df_list = []
df_status1,df = generate_status_1_and_NJB(df)
df_list.append(df_status1)
df_list.append(generate_multiple_ordered_status(df, 2, state_order))
df_list.append(generate_status(df, "GF0", "STATUS_4", ["TAGE"]))
df_list.append(generate_status(df, "GF1", "STATUS_5", ["TAGE", "EGPT"]))

print("Merging everything into timeline...")
res = merge_into_full_timeline(2023, df_list)

end = time.time()
print(f"Run took {int((end - start)//60)} minutes and {round((end - start)%60, 3)} seconds.")







