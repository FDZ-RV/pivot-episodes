import pandas as pd
import time
from joblib import Parallel, delayed

#######################################################################################################################


# global definitions and functions

#pd.set_option("display.max_columns", None)
#pd.set_option("display.max_rows", None)

def day_nr(year: int, month: int) -> int:
    '''
    returns nr of days in month "month" of year "year"
    '''

    if month == 2:
        # Schaltjahr / leap year
        is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
        return 29 if is_leap else 28
    month_days = {
        1: 31, 3: 31, 4: 30, 5: 31, 6: 30,
        7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
    }
    return month_days.get(month, 0)


#######################################################################################################################


# Loading and preprocessing:

# load factors for ZREG calculations (see status 1)
anlage10_df = pd.read_stata("D:/projects/soep_rv/VSKT/help/Anlage_10.dta")
anlage10_df = anlage10_df.rename(columns={"Jahr": "JAHR", "Monat": "MONAT"})

# ordered list of zustände, required for status 2 and 3
zustand_order = ["BRF", "BMP", "VRS", "ALG", "AUF", "ARM", "PFL", "FWB", "SCH", "SON", "PMU", "USV",
                 "RTB", "HRT", "FRG", "FZR", "NJB", "AZ0", "AZ1", "ALH"]

# load data
def load_and_preprocess(berichtsjahr):
    load_start = time.time()

    data_path = #TODO
    file_name = #TODO
    
    print(f"Loading data for {berichtsjahr} from {data_path} ...")

    df = pd.read_stata(data_path + file_name, convert_categoricals=False)

    load_end = time.time()
    print(f"Dataset loaded in {round(load_end - load_start, 3)} seconds.")
    print(f" Number of episodes: {len(df)}.")

    # encode RVTS and VSGR variables:
    rtvs_dict = {"ohne Zuordnung von Entgeltpunkten: keine Rentenbezugszeit aus eigener Versicheru": 0,
                 "ohne Zuordnung von Entgeltpunkten: Zeit während Rentenbezug aus eigener Versich": 1,
                 "mit Zuordnung von Entgeltpunkten: keine Rentenbezugszeit aus eigener Versicherun": 5,
                 "mit Zuordnung von Entgeltpunkten: Zeit während Rentenbezug aus eigener Versiche": 6}
    
    vsgr_dict = {"AR, ab 2005 Allgemeine Rentenversicherung": 1,
                 "AV (bis 2004)": 2,
                 "Handwerker AR, ab 2005 Handwerker": 3,
                 "AV (bis 2004)": 4,
                 "KN (Arbeiter), ab 2005 Knappschaftliche Rentenversicherung": 5,
                 "KN (Angestellter) (bis 2004)": 6}
    
    df["RTVS"] = df["RTVS"].map(rtvs_dict)
    df["VSGR"] = df["VSGR"].map(vsgr_dict)

    # transform time variables into datetime objects
    df["VNZR"] = pd.to_datetime(df["VNZR"], format="%Y%m%d")
    df["BSZR"] = pd.to_datetime(df["BSZR"], format="%Y%m%d")

    # group by FDZ_ID 
    grouped_df = df.groupby("FDZ_ID")
    id_groups = [(id,group) for id, group in grouped_df]
    print(f" Number of unique FDZ_IDs: {len(id_groups)}.")

    return id_groups



'''
Idea: We loop over all ID's. For a fixed ID we take each Status/Zustand and transform the corresponding episodes 
into a new df with columns JAHR, MONAT, TAGE, EGPT, ZREG etc. (following each status' specific rules / requirements).
The resulting 5 df's (one per status) are then merged together (ordered by pairs of JAHR, MONAT). 
The result is one big df per ID and these are then concatenated into the final result. 
'''

# wrap all operations in a function, so that we can parallelise at the end:

def pivot_episodes(id, data, berichtsjahr):
    '''
    :param id: FDZ_ID
    :param data: element of df.groupby("FDZ_ID")
    :return: df in (JAHR,MONAT) format, built from all episodes of FDZ_ID
    '''

    # define timerange
    min_date = data["VNZR"].min()
    max_date = pd.to_datetime(f"{berichtsjahr}1231", format="%Y%m%d")  # End of Berichtsjahr

    # generate new df in required format - .replace ensures that first month is accounted for
    data_transformed = pd.DataFrame(
        {"ID": id,
         "JAHR": pd.date_range(min_date.replace(day=1), max_date, freq="MS").year,
         "MONAT": pd.date_range(min_date.replace(day=1), max_date, freq="MS").month
         }
    )
    data_transformed["TAGE"] = data_transformed.apply(lambda row: day_nr(row["JAHR"], row["MONAT"]), axis=1)

    # now fill data_transformed with all the data for status 1 to 5, then append to id_df_list

    ###################################################################################################################

    # STATUS 1:

    # dictionary for Zustände
    conditions = {
		"WSB": (data['BYAT'] == 10) & (data['BYATSO'].isin(["0", "3", "4", "5", "8", "9"])) & (data['Ses_frg'].isna()) & (data[
			'VSGR'].isin([1, 2, 3, 4])) & (data['RTVS'].isin([0, 1])),
		"OSB": (data['BYAT'] == 10) & (data['BYATSO'].isin(["0", "3", "4", "5", "8", "9"])) & (data['Ses_frg'].isna()) & (data[
			'VSGR'].isin([1, 2, 3, 4])) & (data['RTVS'].isin([5, 6])),
		"WKN": (data['BYAT'] == 10) & (data['BYATSO'].isin(["0", "3", "4", "5", "8", "9"])) & (data['Ses_frg'].isna()) & (data[
			'VSGR'].isin([5, 6])) & (data['RTVS'].isin([0, 1])),
		"OKN": (data['BYAT'] == 10) & (data['BYATSO'].isin(["0", "3", "4", "5", "8", "9"])) & (data['Ses_frg'].isna()) & (data[
			'VSGR'].isin([5, 6])) & (data['RTVS'].isin([5, 6])),
		"ATZ_WSB": (data['BYAT'] == 9) & (data['Ses_frg'].isna()) & (data['VSGR'].isin([1, 2, 3, 4])) & (data['RTVS'].isin(
			[0, 1])),
		"ATZ_OSB": (data['BYAT'] == 9) & (data['Ses_frg'].isna()) & (data['VSGR'].isin([1, 2, 3, 4])) & (data['RTVS'].isin(
			[5, 6])),
		"ATZ_WKN": (data['BYAT'] == 9) & data['Ses_frg'].isna() & data['VSGR'].isin([5, 6]) & data['RTVS'].isin([0, 1]),
		"ATZ_OKN": (data['BYAT'] == 9) & data['Ses_frg'].isna() & data['VSGR'].isin([5, 6]) & data['RTVS'].isin([5, 6]),
		"WSS": (data['BYAT'] == 17) & data['VSGR'].isin([1, 2, 3, 4]) & data['RTVS'].isin([0, 1]) & data[
			'Ses_frg'].isna(),
		"OSS": (data['BYAT'] == 17) & data['VSGR'].isin([1, 2, 3, 4]) & data['RTVS'].isin([5, 6]) & data[
			'Ses_frg'].isna()}

    # list to save all the df's, one per zustand
    zustand_df_liste = []

    # for each zustand read the episodes, calculate the required variables and format to merge with data_transformed:

    for zustand, bedingung in conditions.items():

        # isolate zustand
        zustand_df = data[bedingung]

        if len(zustand_df) != 0:

            # list to save temporary results
            episode_df_list = []

            # loop over episodes for the current zustand
            for i in range(len(zustand_df)):
                row = zustand_df.iloc[i]
                start_date = row.VNZR
                end_date = row.BSZR
                nr_of_days = len(pd.date_range(start_date, end_date, freq="D"))

                # 3 lists: 1 and 2 give (JAHR, MONAT) in the timespan, 3 gives STATUS_1_TAGE for each combo JAHR,MONAT:

                month_list = pd.date_range(start_date.replace(day=1), end_date, freq="MS").month.tolist()
                year_list = pd.date_range(start_date.replace(day=1), end_date, freq="MS").year.tolist()

                if len(month_list) == 1:
                    status_tage = [(end_date.date() - start_date.date()).days + 1]
                else:
                    next_month = (start_date + pd.offsets.MonthEnd(0)).date()
                    status_tage = [(next_month - start_date.date()).days + 1]  # first entry = days in first month
                    if len(month_list) > 2:  # days in the intermediate months
                        for j in range(1, len(month_list) - 1):
                            status_tage.append(day_nr(year_list[j], month_list[j]))
                    status_tage.append(end_date.day)  # days in last month

                # save as new df
                episode_df = pd.DataFrame({
                    "JAHR": year_list,
                    "MONAT": month_list,
                    "STATUS_1": zustand,
                    "STATUS_1_TAGE": status_tage
                })

                # calculate ZREG und EGPT per day (zustände are ordered by zreg_daily)
                zreg_daily = row.ZREG / nr_of_days
                egpt_daily = row.EGPT / nr_of_days

                # calculate monthly values
                episode_df["STATUS_1_ZREG"] = episode_df["STATUS_1_TAGE"] * zreg_daily
                episode_df["STATUS_1_EGPT"] = episode_df["STATUS_1_TAGE"] * egpt_daily
                episode_df["ZREG_tag"] = zreg_daily

                # save in list
                episode_df_list.append(episode_df)

            # concat all results
            output_df = pd.concat(episode_df_list, ignore_index=True)

            # adjust ZREG values by anlage10_df
            if zustand in {"OSB", "OKN", "OSS", "ATZ_OSB", "ATZ_OKN"}:
                output_df = pd.merge(output_df, anlage10_df, on=["JAHR", "MONAT"], how="left")
                output_df[["STATUS_1_ZREG", "ZREG_tag"]] = output_df[["STATUS_1_ZREG", "ZREG_tag"]].divide(
                    output_df["ANLAGE_10"], axis=0)
                output_df = output_df.drop(columns=["ANLAGE_10"])

        else:
            output_df = pd.DataFrame(columns=[
                "JAHR", "MONAT", "STATUS_1", "STATUS_1_TAGE", "STATUS_1_ZREG", "STATUS_1_EGPT", "ZREG_tag"])
            output_df = output_df.astype({
                "JAHR": "int",
                "MONAT": "int",
                "STATUS_1": "object",
                "STATUS_1_TAGE": "float",
                "STATUS_1_ZREG": "float",
                "STATUS_1_EGPT": "float",
                "ZREG_tag": "float"})
        # save result for one zustand
        zustand_df_liste.append(output_df)

    # concat all output_df's = one df with data for all Zustände
    alle_zustaende_df = pd.concat(zustand_df_liste, ignore_index=True)

    '''
    !: can have duplicates of combos JAHR, MONAT. in this case we keep only the contribution with max ZREG_tag,
    other zustände enter the calculation for NJB (in status 2 or 3) ...
    '''

    # find duplicates, keep track using "index", take the one with max ZREG_tag, save rest
    alle_zustaende_df = alle_zustaende_df.reset_index(drop=False)
    max_zustaende_df = alle_zustaende_df.sort_values("ZREG_tag", ascending=False).drop_duplicates(
        subset=["JAHR", "MONAT"])
    max_ids = set(max_zustaende_df["index"])
    rest_df = alle_zustaende_df[~alle_zustaende_df["index"].isin(max_ids)].drop(columns=["index"])
    max_zustaende_df = max_zustaende_df.drop(columns=["index", "ZREG_tag"])

    # save rest for below (status 2 and 3), rename zustand/status
    nebenjob_df = rest_df.groupby(["JAHR", "MONAT"], as_index=False)[["STATUS_1_TAGE", "STATUS_1_EGPT"]].sum()
    nebenjob_df["STATUS"] = "NJB"
    nebenjob_df = nebenjob_df.rename(columns={"STATUS_1_TAGE": "STATUS_TAGE", "STATUS_1_EGPT": "STATUS_EGPT"})

    # max_zustaende_df will enter in status 1, hence we merge with data_transformed
    data_transformed = pd.merge(data_transformed, max_zustaende_df, on=["JAHR", "MONAT"], how="left")

    ###################################################################################################################

    # STATUS 2 UND 3:

    # encode zustände
    conditions = {"BRF": (data['BYAT'] == 10) & (data['BYATSO'].isin(["1", "2", "6", "7"])),
                  "BMP": (data['BYAT'] == 90),
                  "VRS": (data['BYAT'] == 18),
                  "ALG": (data['BYAT'] == 13),
                  "AUF": (data['BYAT'] == 12) | ((data['BYAT'].isin([40, 41, 48])) & (data['BYATSO'].isin(["1", 'A']))),
                  "ARM": (data['BYAT'] == 14),
                  "PFL": (data['BYAT'] == 7) | ((data['BYAT'] == 60) & (data['BYATSO'] == '2')) |
                         ((data['BYAT'].isin([20, 21])) & (data['BYATSO'] == '8')),
                  "FWB": (data['BYAT'].isin([20, 21])) & (data['BYATSO'].isin(["0", "2", "5", "6", "7"])),
                  "SCH": ((data['BYAT'].isin([20, 21])) & (data['BYATSO'] == '3')) | (
                          (data['BYAT'].isin([40, 41, 42, 43, 48])) & (data['BYATSO'].isin(["4", "6", "7", "8", 'C']))),
                  "SON": (data['BYAT'].isin([8, 15, 16, 26, 30, 31, 49])) | (
                          (data['BYAT'].isin([40, 41, 48])) & (data['BYATSO'].isin(["9", "2"]))) | (
                                 (data['BYAT'] == 60) & (data['BYATSO'].isin(["6", "7"]))) | (
                                 (data['BYAT'].isin([20, 21])) & (data['BYATSO'] == '1')),
                  "PMU": (data['BYAT'] == 11),
                  "USV": (data['BYAT'].isin([2, 3])),
                  "RTB": (data['BYAT'].isin([70, 71, 72])),
                  "HRT": (data['BYAT'].isin([20, 21])) & (data['BYATSO'] == '4'),
                  "FRG": (data['Ses_frg'].notna()),
                  "FZR": (data['BYAT'] == 25),
                  "AZ0": (data['BYAT'].isin([40, 41, 48])) & (data['BYATSO'] == '5') & (data['RTVS'] == 0),
                  "AZ1": (data['BYAT'].isin([40, 41, 48])) & (data['BYATSO'] == '5') & (data['RTVS'] == 1),
                  "ALH": (data['BYAT'] == 4) | (
                          (data['BYAT'].isin([40, 41, 48])) & (data['BYATSO'].isin(["3", 'B', 'D'])))}

    # list to save the df's generated from each zustand
    zustand_df_liste = []

    for zustand, bedingung in conditions.items():

        # isolate zustand
        zustand_df = data[bedingung]

        if len(zustand_df) != 0:

            # list to save the result for each zustand
            episode_df_list = []

            # loop over episodes, calculate JAHR,MONAT,STATUS_x_TAGE etc...:

            for i in range(len(zustand_df)):
                row = zustand_df.iloc[i]
                start_date = row.VNZR
                end_date = row.BSZR
                nr_of_days = len(pd.date_range(start_date, end_date, freq="D"))

                # 3 lists: 1 and 2 for JAHR, MONAT, 3 for STATUS_x_TAGE
                month_list = pd.date_range(start_date.replace(day=1), end_date, freq="MS").month.tolist()
                year_list = pd.date_range(start_date.replace(day=1), end_date, freq="MS").year.tolist()

                if len(month_list) == 1:
                    status_tage = [(end_date.date() - start_date.date()).days + 1]
                else:
                    next_month = (start_date + pd.offsets.MonthEnd(0)).date()
                    status_tage = [(next_month - start_date.date()).days + 1]  # first entry = days in first month
                    if len(month_list) > 2:  # days in the intermediate months
                        for j in range(1, len(month_list) - 1):
                            status_tage.append(day_nr(year_list[j], month_list[j]))
                    status_tage.append(end_date.day)  # days in last month

                # save as new df
                episode_df = pd.DataFrame({
                    "JAHR": year_list,
                    "MONAT": month_list,
                    "STATUS": zustand,
                    "STATUS_TAGE": status_tage
                })

            # calculate EGPT per day
            egpt_daily = row.EGPT / nr_of_days
            episode_df["STATUS_EGPT"] = episode_df["STATUS_TAGE"] * egpt_daily

            # save in list
            episode_df_list.append(episode_df)

            # concat all episode_df's
            output_df = pd.concat(episode_df_list, ignore_index=True)

        else:
            output_df = pd.DataFrame(columns=["JAHR", "MONAT", "STATUS", "STATUS_TAGE", "STATUS_EGPT"])
            output_df = output_df.astype({
                "JAHR": "int",
                "MONAT": "int",
                "STATUS": "object",
                "STATUS_TAGE": "float",
                "STATUS_EGPT": "float"})

        zustand_df_liste.append(output_df)

    # add nebenjob_df (from status 1... )
    zustand_df_liste.append(nebenjob_df)

    # concat all output_df's (and nebenjob_df) = one df with data for every zustand
    alle_zustaende_df = pd.concat(zustand_df_liste, ignore_index=True)

    # sort according to zustand_order, keep top 2 entries
    alle_zustaende_df["STATUS"] = pd.Categorical(alle_zustaende_df["STATUS"], categories=zustand_order, ordered=True)
    alle_zustaende_geordnet_df = alle_zustaende_df.sort_values(by=["JAHR", "MONAT", "STATUS"])
    top2_zustaende_df = alle_zustaende_geordnet_df.groupby(["JAHR", "MONAT"]).head(2).reset_index(drop=True)

    # pivot to format for status 2 und 3:

    # RANG counts nr of multiple entries per JAHR,MONAT
    top2_zustaende_df["RANG"] = top2_zustaende_df.groupby(["JAHR", "MONAT"]).cumcount() + 1

    # pivot (if not empty)
    if top2_zustaende_df.empty:
        zustaende_pivot_df = pd.DataFrame(columns=[
            "JAHR", "MONAT", "STATUS_2", "STATUS_2_TAGE",
            "STATUS_2_EGPT", "STATUS_3", "STATUS_3_TAGE",
            "STATUS_3_EGPT"
        ])
        zustaende_pivot_df = zustaende_pivot_df.astype({
            "JAHR": "int",
            "MONAT": "int",
            "STATUS_2": "object",
            "STATUS_2_TAGE": "float",
            "STATUS_2_EGPT": "float",
            "STATUS_3": "object",
            "STATUS_3_TAGE": "float",
            "STATUS_3_EGPT": "float"})
    else:
        zustaende_pivot_df = top2_zustaende_df.pivot(index=["JAHR", "MONAT"], columns="RANG")[
            ["STATUS", "STATUS_TAGE", "STATUS_EGPT"]]

        # generate, order and rename correct variable names:

        zustaende_pivot_df.columns = [f"{spalte}_{rang + 1}" for spalte, rang in zustaende_pivot_df]
        zustaende_pivot_df = zustaende_pivot_df.reset_index()
        status_variablen = ["STATUS_2", "STATUS_TAGE_2", "STATUS_EGPT_2", "STATUS_3", "STATUS_TAGE_3", "STATUS_EGPT_3"]
        for variable in status_variablen:
            if variable not in zustaende_pivot_df.columns:
                zustaende_pivot_df[variable] = pd.NA
        zustaende_pivot_df = zustaende_pivot_df.rename(columns={
            "STATUS_TAGE_2": "STATUS_2_TAGE",
            "STATUS_EGPT_2": "STATUS_2_EGPT",
            "STATUS_TAGE_3": "STATUS_3_TAGE",
            "STATUS_EGPT_3": "STATUS_3_EGPT"
        })

    # merge with data_transformed
    data_transformed = pd.merge(data_transformed, zustaende_pivot_df, on=["JAHR", "MONAT"], how="left")

    ###################################################################################################################

    # STATUS 4 and 5:

    status_df_list = []  # list to save all status_df's

    # filter for status 4 (BYAT = 5):
    status_data = data[data["BYAT"] == 5]

    if len(status_data) != 0:
        # sort by VNZR
        episodes_df = status_data.sort_values(by="VNZR", ignore_index=True)

        # untangle overlapping episodes:

        # loop over rows in status_data, update row nr i and length of loop N dynamically
        i = 1
        N = len(episodes_df)
        while i < N:

            # get rid of overlaps:

            if episodes_df.loc[i, 'VNZR'] <= episodes_df.loc[i - 1, 'BSZR']:
                # split, update df, start over
                if episodes_df.loc[i - 1, 'BSZR'] <= episodes_df.loc[i, 'BSZR']:
                    new_df = pd.DataFrame(
                        {"VNZR": [episodes_df.loc[i, 'VNZR']],
                         "BSZR": [episodes_df.loc[i - 1, 'BSZR']]
                         })
                    if episodes_df.loc[i - 1, 'VNZR'] < episodes_df.loc[i, 'VNZR']:
                        new_df.loc[len(new_df)] = [episodes_df.loc[i - 1, 'VNZR'],
                                                   episodes_df.loc[i, 'VNZR'] - pd.Timedelta("1 day")]
                    if episodes_df.loc[i, 'BSZR'] > episodes_df.loc[i - 1, 'BSZR']:
                        new_df.loc[len(new_df)] = [episodes_df.loc[i - 1, 'BSZR'] + pd.Timedelta("1 day"),
                                                   episodes_df.loc[i, 'BSZR']]
                    unaltered_rows_df = episodes_df[~episodes_df.index.isin([i - 1, i])]
                # don't raise i to scan same row again
                elif episodes_df.loc[i - 1, 'BSZR'] > episodes_df.loc[i, 'BSZR']:
                    new_df = pd.DataFrame(
                        {"VNZR": [episodes_df.loc[i, 'VNZR'], episodes_df.loc[i, 'BSZR'] + pd.Timedelta("1 day")],
                         "BSZR": [episodes_df.loc[i, 'BSZR'], episodes_df.loc[i - 1, 'BSZR']]
                         })
                    if episodes_df.loc[i - 1, 'VNZR'] < episodes_df.loc[i, 'VNZR']:
                        new_df.loc[len(new_df)] = [episodes_df.loc[i - 1, 'VNZR'],
                                                   episodes_df.loc[i, 'VNZR'] - pd.Timedelta("1 day")]
                    unaltered_rows_df = episodes_df[~episodes_df.index.isin([i - 1, i])]
                    # raise i to scan next row
                    i = i + 1

                # update episodes_df
                episodes_df = pd.concat([new_df, unaltered_rows_df], ignore_index=True).sort_values(
                    by="VNZR").reset_index(drop=True)

                # update N
                N = len(episodes_df)
            else:
                i = i + 1

        # create new df with JAHR,MONAT and count nr of days in each pair:

        intermediate_results = []

        for _, row in episodes_df.iterrows():
            # create date range
            dates = pd.date_range(start=row["VNZR"], end=row["BSZR"], freq="D")

            # build df
            temp_df = pd.DataFrame({
                "JAHR": dates.year,
                "MONAT": dates.month
            })

            # count days
            day_count = temp_df.groupby(["JAHR", "MONAT"]).size().reset_index(name="STATUS_4_TAGE")

            # save in list
            intermediate_results.append(day_count)

        # put all results together
        output_df = pd.concat(intermediate_results, ignore_index=True)
        output_df = output_df.groupby(["JAHR", "MONAT"], as_index=False)["STATUS_4_TAGE"].sum()


    else:
        output_df = pd.DataFrame(columns=["JAHR", "MONAT", "STATUS_4_TAGE"])
        output_df = output_df.astype({
            "JAHR": "int",
            "MONAT": "int",
            "STATUS_4_TAGE": "float"})

    # save in status_df_list
    status_df_list.append(output_df)

    ###################################################################################################################

    # filter for Status 5 (BYAT = 6) - same logic as Status 4, but keep also track of EGPT
    status_data = data[data["BYAT"] == 6]

    if len(status_data) != 0:
        # sort by VNZR
        episodes_df = status_data.sort_values(by="VNZR", ignore_index=True)

        # calculate daily EGPT's per episode
        episodes_df["days"] = (episodes_df["BSZR"] - episodes_df["VNZR"]).dt.days + 1
        episodes_df["EGPT_daily"] = episodes_df["EGPT"] / episodes_df["days"]

        # untangle episodes: loop over rows in status_data, update row nr i and length of loop N dynamically
        i = 1
        N = len(episodes_df)
        while i < N:

            # get rid of overlaps:

            if episodes_df.loc[i, 'VNZR'] <= episodes_df.loc[i - 1, 'BSZR']:
                # split, sum daily EGPT's, update, start over
                if episodes_df.loc[i - 1, 'BSZR'] <= episodes_df.loc[i, 'BSZR']:
                    new_df = pd.DataFrame(
                        {"VNZR": [episodes_df.loc[i, 'VNZR']],
                         "BSZR": [episodes_df.loc[i - 1, 'BSZR']],
                         "EGPT_daily": [episodes_df.loc[i - 1, 'EGPT_daily'] + episodes_df.loc[i, 'EGPT_daily']]
                         })
                    if episodes_df.loc[i - 1, 'VNZR'] < episodes_df.loc[i, 'VNZR']:
                        new_df.loc[len(new_df)] = [episodes_df.loc[i - 1, 'VNZR'],
                                                   episodes_df.loc[i, 'VNZR'] - pd.Timedelta("1 day"),
                                                   episodes_df.loc[i - 1, 'EGPT_daily']]

                    if episodes_df.loc[i, 'BSZR'] > episodes_df.loc[i - 1, 'BSZR']:
                        new_df.loc[len(new_df)] = [episodes_df.loc[i - 1, 'BSZR'] + pd.Timedelta("1 day"),
                                                   episodes_df.loc[i, 'BSZR'],
                                                   episodes_df.loc[i, 'EGPT_daily']]
                    unaltered_rows_df = episodes_df[~episodes_df.index.isin([i - 1, i])]
                # do not raise i
                elif episodes_df.loc[i - 1, 'BSZR'] > episodes_df.loc[i, 'BSZR']:
                    new_df = pd.DataFrame(
                        {"VNZR": [episodes_df.loc[i, 'VNZR'], episodes_df.loc[i, 'BSZR'] + pd.Timedelta("1 day")],
                         "BSZR": [episodes_df.loc[i, 'BSZR'], episodes_df.loc[i - 1, 'BSZR']],
                         "EGPT_daily": [episodes_df.loc[i - 1, 'EGPT_daily'] + episodes_df.loc[i, 'EGPT_daily'],
                                        episodes_df.loc[i - 1, "EGPT_daily"]]
                         })
                    if episodes_df.loc[i - 1, 'VNZR'] < episodes_df.loc[i, 'VNZR']:
                        new_df.loc[len(new_df)] = [episodes_df.loc[i - 1, 'VNZR'],
                                                   episodes_df.loc[i, 'VNZR'] - pd.Timedelta("1 day"),
                                                   episodes_df.loc[i - 1, 'EGPT_daily']]
                    unaltered_rows_df = episodes_df[~episodes_df.index.isin([i - 1, i])]
                    i = i + 1  # raise i
                episodes_df = pd.concat([new_df, unaltered_rows_df], ignore_index=True).sort_values(
                    by="VNZR").reset_index(drop=True)
                # update N
                N = len(episodes_df)
            else:
                i = i + 1

        # create new df with JAHR,MONAT and count nr of days and EGPT's in each pair:

        intermediate_results = []

        for _, row in episodes_df.iterrows():
            # create date range
            dates = pd.date_range(start=row["VNZR"], end=row["BSZR"], freq="D")

            # fill a df with all days
            temp_df = pd.DataFrame({
                "JAHR": dates.year,
                "MONAT": dates.month
            })

            # count days, calculate EGPT per month
            day_count = temp_df.groupby(["JAHR", "MONAT"]).size().reset_index(name="STATUS_5_TAGE")
            day_count["STATUS_5_EGPT"] = day_count["STATUS_5_TAGE"] * row["EGPT_daily"]

            # save in list
            intermediate_results.append(day_count)

        # put all results together
        output_df = pd.concat(intermediate_results, ignore_index=True)
        output_df = output_df.groupby(["JAHR", "MONAT"], as_index=False)[["STATUS_5_TAGE", "STATUS_5_EGPT"]].sum()

    else:
        output_df = pd.DataFrame(columns=["JAHR", "MONAT", "STATUS_5_TAGE"])
        output_df = output_df.astype({
            "JAHR": "int",
            "MONAT": "int",
            "STATUS_5_TAGE": "float"})

    # save in status_df_list
    status_df_list.append(output_df)

    # merge all elements of status_df_list with data_transformed
    for element in status_df_list:
        data_transformed = data_transformed.merge(element, on=["JAHR", "MONAT"], how="left")

    process_end = time.time()
    #print(f" Runtime: {round(process_end - process_start, 3)} seconds.")

    return data_transformed


#######################################################################################################################


# run the function for each ID in parallel:

def run_in_batches_and_save_result(id_groups, batch_size, destination_folder, berichtsjahr):
    '''
    :param id_groups: output of load_and_preprocess(berichtsjahr)
    :param batch_size: int (should be less than 100000)
    :param destination_folder: str
    :param berichtsjahr: int
    :return: returns final result and saves it as #TODO to destination_folder
    '''

    batches = [id_groups[i:i+batch_size] for i in range(0, len(id_groups), batch_size)]
    all_files = []

    for i, batch in enumerate(batches):
        batch_start = time.time()
        print(f"Processing batch {i+1}/{len(batches)} ...")
        results = Parallel(n_jobs=8)(
            delayed(pivot_episodes)(id, group, berichtsjahr) for id, group in batch
        )
        result_df = pd.concat(results, ignore_index=True)
        result_df = result_df.rename(columns={"ID": "FDZ_ID"})

        # save each batch to disk
        file_path = destination_folder + f"episodes_pivot_part_{i}.parquet"
        result_df.to_parquet(file_path)
        all_files.append(file_path)
        batch_end = time.time()
        print(f" Done in {round(batch_end - batch_start, 3)} seconds.")

    # recombine from disk
    final_df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)

    # final formatting:
    columns_to_num = ['JAHR', 'MONAT', 'STATUS_1_TAGE',
                      'STATUS_2_TAGE',
                      'STATUS_3_TAGE', "STATUS_3_EGPT"]
    columns_to_str = ["STATUS_1", "STATUS_2", "STATUS_3"]

    for col in columns_to_num:
        final_df[col] = pd.to_numeric(final_df[col], errors="coerce")
    for col in columns_to_str:
        final_df[col] = final_df[col].astype(str)

    # replace "nan" and "None" strings by nan
    final_df[['STATUS_1', 'STATUS_2', 'STATUS_3']] = final_df[['STATUS_1', 'STATUS_2', 'STATUS_3']].replace(
        ["nan","None"], "")
    
    # optional: downcast dtypes to save space
    final_df["JAHR"] = final_df["JAHR"].astype("int16")
    final_df["MONAT"] = final_df["MONAT"].astype("int8")
    final_df["STATUS_4_TAGE"] = final_df["STATUS_4_TAGE"].astype("float16")
    final_df["STATUS_5_TAGE"] = final_df["STATUS_5_TAGE"].astype("float16")
    
    # optional: fill numeric nan's by 0
    # final_df[final_df.select_dtypes(include="number").columns] = final_df.select_dtypes(include="number").fillna(0)

    # export result
    save_as = #TODO
    final_df.to_stata(destination_folder + save_as, write_index=False)

    print(f"\n Output saved to {destination_folder + save_as}.")

    return final_df


#######################################################################################################################



start_time = time.time()

year = #TODO
destination_folder = #TODO

run_in_batches_and_save_result(id_groups=load_and_preprocess(year), batch_size=50000,
                               destination_folder=destination_folder,berichtsjahr=year)

end_time = time.time()
print(f" Total runtime: {int((end_time - start_time)//60)} minutes and {round((end_time - start_time)%60, 3)} seconds.")


